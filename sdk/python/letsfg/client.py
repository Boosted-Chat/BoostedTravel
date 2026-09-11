"""
LetsFG Python SDK — agent-native flight search & booking.

Zero-config, zero-browser, zero price bias. Built for autonomous agents.
Search is free. Booking adds no booking fee and no transaction fee: our margin is
already included in the price on every offer, so the amount you were shown is the
amount charged.

    import os
    from letsfg import LetsFG

    bt = LetsFG(api_key=os.environ["LETSFG_API_KEY"])

    # One-time: connect a payment method. Nothing is charged to connect.
    # (setup_payment() was retired with Stripe on 2026-09-08 and now raises.)
    print(bt.connect_payment()["connect_url"])   # open this in a browser

    # Search
    flights = bt.search("LON", "BCN", "2026-04-01")
    print(flights.cheapest.summary())

    # Book. There is no unlock step — unlock() was retired on 2026-09-08 and raises.
    # The fare is HELD on the connected method and captured only once a real airline
    # PNR exists; a failed booking releases the hold and charges nothing.
    booking = bt.book_and_wait(
        offer_id=flights.cheapest.id,
        search_id=flights.search_id,
        passengers=[{
            "given_name": "John", "family_name": "Doe",
            "born_on": "1990-01-15", "gender": "m", "title": "mr",
            "email": "john@example.com"
        }],
        contact_email="john@example.com",
    )
    print(booking)
"""

from __future__ import annotations

import json
import logging
import os
import time
from pathlib import Path
from typing import Any, Callable, Optional
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError
from urllib.parse import quote

from letsfg.models import (
    AgentProfile,
    BookingResult,
    CheckoutProgress,
    FlightSearchResult,
    Passenger,
    UnlockResult,
)

DEFAULT_BASE_URL = "https://letsfg.co/developers"

_log = logging.getLogger(__name__)


# ── Config file persistence (~/.letsfg/config.json) ───────────────────────

def _config_dir() -> Path:
    """Return the LetsFG config directory, creating it if needed."""
    if os.name == "nt":
        base = Path(os.environ.get("APPDATA", Path.home()))
    else:
        base = Path(os.environ.get("XDG_CONFIG_HOME", Path.home() / ".config"))
    d = base / "letsfg"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _config_path() -> Path:
    return _config_dir() / "config.json"


def _load_config() -> dict:
    """Load saved config (api_key, agent_id, etc.)."""
    p = _config_path()
    if p.exists():
        try:
            return json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}


def _save_config(data: dict) -> None:
    """Persist config to disk (owner read/write only)."""
    p = _config_path()
    try:
        existing = _load_config()
        existing.update(data)
        p.write_text(json.dumps(existing, indent=2), encoding="utf-8")
        try:
            p.chmod(0o600)
        except Exception:
            pass
    except Exception as e:
        _log.debug("Could not save config to %s: %s", p, e)


def _saved_api_key() -> str:
    """Return the API key from config file, or empty string."""
    return _load_config().get("api_key", "")



# ── Bookable connector registry ────────────────────────────────────────────
# Maps source tags to their BookableConnector subclass, loaded lazily.
# For the remaining hand-tuned connectors we keep explicit entries.
# All other airlines are handled by the GenericCheckoutEngine via config.

_BOOKABLE_CONNECTORS: dict[str, tuple[str, str]] = {
    "ryanair_direct": ("letsfg.connectors.ryanair", "RyanairBookableConnector"),
    "easyjet_direct": ("letsfg.connectors.easyjet", "EasyjetBookableConnector"),
}


def _get_bookable_connector(source: str):
    """Dynamically load a bookable connector class by source tag.

    Falls back to the generic config-driven checkout engine if no
    hand-tuned connector exists but an airline config is registered.
    """
    # 1. Check for hand-tuned connector
    entry = _BOOKABLE_CONNECTORS.get(source)
    if entry:
        mod_name, cls_name = entry
        try:
            import importlib
            mod = importlib.import_module(mod_name)
            return getattr(mod, cls_name)
        except (ImportError, AttributeError):
            pass

    # 2. Fall back to generic checkout engine config
    try:
        from letsfg.connectors.checkout_engine import AIRLINE_CONFIGS
        if source in AIRLINE_CONFIGS:
            return _make_generic_connector(source)
    except ImportError:
        pass

    return None


def _make_generic_connector(source: str):
    """Return a BookableConnector subclass backed by the generic engine."""
    from letsfg.connectors.booking_base import BookableConnector, CheckoutProgress as _CP
    from letsfg.connectors.checkout_engine import AIRLINE_CONFIGS, GenericCheckoutEngine

    config = AIRLINE_CONFIGS[source]

    class _GenericBookable(BookableConnector):
        AIRLINE_NAME = config.airline_name
        SOURCE_TAG = config.source_tag

        async def _run_checkout(self, offer, passengers):
            # Token already verified by base class start_checkout()
            # but the engine also verifies — pass dummy to skip double-check
            engine = GenericCheckoutEngine()
            return await engine.run(
                config=config,
                offer=offer,
                passengers=passengers,
                checkout_token=self._last_token,
                api_key=self._last_api_key,
                base_url=self._last_base_url,
            )

    _GenericBookable.__name__ = f"{config.airline_name.replace(' ', '')}Bookable"
    return _GenericBookable


# ── Error codes ──────────────────────────────────────────────────────────
# Machine-readable error codes for agent decision-making.
# Each code has a category that tells the agent how to react:
#   transient  — retry after a short delay (network blip, rate limit, supplier timeout)
#   validation — fix the request and retry (bad input, unsupported route)
#   business   — requires human decision (payment declined, fare expired, policy violation)

class ErrorCode:
    """Machine-readable error codes returned in LetsFGError.error_code."""
    # ── Transient (safe to retry) ──
    SUPPLIER_TIMEOUT = "SUPPLIER_TIMEOUT"
    RATE_LIMITED = "RATE_LIMITED"
    SERVICE_UNAVAILABLE = "SERVICE_UNAVAILABLE"
    NETWORK_ERROR = "NETWORK_ERROR"

    # ── Validation (fix input, then retry) ──
    INVALID_IATA = "INVALID_IATA"
    INVALID_DATE = "INVALID_DATE"
    INVALID_PASSENGERS = "INVALID_PASSENGERS"
    UNSUPPORTED_ROUTE = "UNSUPPORTED_ROUTE"
    MISSING_PARAMETER = "MISSING_PARAMETER"
    INVALID_PARAMETER = "INVALID_PARAMETER"

    # ── Business (human decision needed) ──
    AUTH_INVALID = "AUTH_INVALID"
    PAYMENT_REQUIRED = "PAYMENT_REQUIRED"
    PAYMENT_DECLINED = "PAYMENT_DECLINED"
    OFFER_EXPIRED = "OFFER_EXPIRED"
    OFFER_NOT_UNLOCKED = "OFFER_NOT_UNLOCKED"
    FARE_CHANGED = "FARE_CHANGED"
    ALREADY_BOOKED = "ALREADY_BOOKED"
    BOOKING_FAILED = "BOOKING_FAILED"


class ErrorCategory:
    """Error categories — tells agent whether to retry, fix input, or escalate."""
    TRANSIENT = "transient"
    VALIDATION = "validation"
    BUSINESS = "business"


_CODE_TO_CATEGORY = {
    ErrorCode.SUPPLIER_TIMEOUT: ErrorCategory.TRANSIENT,
    ErrorCode.RATE_LIMITED: ErrorCategory.TRANSIENT,
    ErrorCode.SERVICE_UNAVAILABLE: ErrorCategory.TRANSIENT,
    ErrorCode.NETWORK_ERROR: ErrorCategory.TRANSIENT,
    ErrorCode.INVALID_IATA: ErrorCategory.VALIDATION,
    ErrorCode.INVALID_DATE: ErrorCategory.VALIDATION,
    ErrorCode.INVALID_PASSENGERS: ErrorCategory.VALIDATION,
    ErrorCode.UNSUPPORTED_ROUTE: ErrorCategory.VALIDATION,
    ErrorCode.MISSING_PARAMETER: ErrorCategory.VALIDATION,
    ErrorCode.INVALID_PARAMETER: ErrorCategory.VALIDATION,
    ErrorCode.AUTH_INVALID: ErrorCategory.BUSINESS,
    ErrorCode.PAYMENT_REQUIRED: ErrorCategory.BUSINESS,
    ErrorCode.PAYMENT_DECLINED: ErrorCategory.BUSINESS,
    ErrorCode.OFFER_EXPIRED: ErrorCategory.BUSINESS,
    ErrorCode.OFFER_NOT_UNLOCKED: ErrorCategory.BUSINESS,
    ErrorCode.FARE_CHANGED: ErrorCategory.BUSINESS,
    ErrorCode.ALREADY_BOOKED: ErrorCategory.BUSINESS,
    ErrorCode.BOOKING_FAILED: ErrorCategory.BUSINESS,
}


def _infer_error_code(status_code: int, detail: str) -> str:
    """Infer a machine-readable error code from HTTP status and detail text."""
    detail_lower = detail.lower()
    if status_code == 401:
        return ErrorCode.AUTH_INVALID
    if status_code == 402:
        if "declined" in detail_lower:
            return ErrorCode.PAYMENT_DECLINED
        return ErrorCode.PAYMENT_REQUIRED
    if status_code == 410:
        return ErrorCode.OFFER_EXPIRED
    if status_code == 422:
        if "iata" in detail_lower or "airport" in detail_lower:
            return ErrorCode.INVALID_IATA
        if "date" in detail_lower:
            return ErrorCode.INVALID_DATE
        if "passenger" in detail_lower:
            return ErrorCode.INVALID_PASSENGERS
        if "route" in detail_lower:
            return ErrorCode.UNSUPPORTED_ROUTE
        return ErrorCode.INVALID_PARAMETER
    if status_code == 429:
        return ErrorCode.RATE_LIMITED
    if status_code == 503:
        return ErrorCode.SERVICE_UNAVAILABLE
    if status_code == 504:
        return ErrorCode.SUPPLIER_TIMEOUT
    if status_code == 409:
        return ErrorCode.ALREADY_BOOKED
    return ErrorCode.BOOKING_FAILED if status_code >= 500 else ErrorCode.INVALID_PARAMETER


class LetsFGError(Exception):
    """
    Base exception for LetsFG SDK.

    Attributes:
        message: Human-readable error description.
        status_code: HTTP status code (0 for client-side errors).
        error_code: Machine-readable code (e.g., 'OFFER_EXPIRED'). See ErrorCode.
        error_category: One of 'transient', 'validation', 'business'. See ErrorCategory.
        response: Raw error response dict from the API.
        is_retryable: True if the error is transient (safe to retry after delay).
    """

    def __init__(
        self,
        message: str,
        status_code: int = 0,
        response: dict | None = None,
        error_code: str = "",
    ):
        self.message = message
        self.status_code = status_code
        self.response = response or {}
        self.error_code = error_code or self.response.get("error_code", "")
        self.error_category = _CODE_TO_CATEGORY.get(self.error_code, ErrorCategory.BUSINESS)
        self.is_retryable = self.error_category == ErrorCategory.TRANSIENT
        super().__init__(message)


class AuthenticationError(LetsFGError):
    """API key is missing or invalid."""
    pass


class PaymentRequiredError(LetsFGError):
    """Payment method not set up or payment declined."""
    pass


class OfferExpiredError(LetsFGError):
    """Offer is no longer available — search again."""
    pass


class ValidationError(LetsFGError):
    """Request parameters are invalid — fix input and retry."""
    pass


class LetsFG:
    """
    LetsFG API client — for autonomous agents.

    Auth options:
      - PFS Bearer token (free search): run `letsfg auth` once, or set LETSFG_BEARER_TOKEN.
      - Developer API key (look-to-book search, plus hotels): set LETSFG_API_KEY or pass api_key=.

    Pricing:
      - Search: FREE (unlimited, requires Bearer token or API key)
      - Unlock: Developer API only, legacy. Not part of the agent flow.
      - Book: Ticket price via Stripe. Developer API only.
    """

    def __init__(
        self,
        api_key: str | None = None,
        base_url: str | None = None,
        timeout: int = 30,
        client_type: str | None = None,
    ):
        self.base_url = (base_url or os.environ.get("LETSFG_BASE_URL") or DEFAULT_BASE_URL).rstrip("/")
        self.timeout = timeout
        self._client_type = client_type or "python-sdk"

        # Key resolution order: explicit arg > LETSFG_API_KEY env var > saved config
        # No silent registration — callers must authenticate explicitly.
        key = api_key or os.environ.get("LETSFG_API_KEY") or ""
        if not key:
            key = _saved_api_key()
        self.api_key = key

    def _require_api_key(self) -> None:
        if not self.api_key:
            raise AuthenticationError(
                "API key required for this operation. Set api_key parameter or "
                "LETSFG_API_KEY env var. Get one: letsfg register"
            )

    # ── Cloud search (server-side engine, Bearer token) ────────────────────────

    def search_local(
        self,
        origin: str,
        destination: str,
        date_from: str,
        *,
        return_date: str | None = None,
        adults: int = 1,
        children: int = 0,
        infants: int = 0,
        cabin_class: str | None = None,
        currency: str = "EUR",
        limit: int = 50,
        max_stopovers: int | None = None,
        sort: str | None = None,
        **_kwargs,
    ) -> FlightSearchResult:
        """
        Search flights via the LetsFG cloud engine using a Bearer token.

        Requires a Bearer token — run `letsfg auth` once (zero-amount card setup).
        Polling is handled internally, including the late split-ticket
        merge that lands after the search first reports `completed`.

        Args:
            origin: IATA code (e.g., "SHA", "GDN", "JFK")
            destination: IATA code (e.g., "CTU", "BER", "LAX")
            date_from: Departure date "YYYY-MM-DD"
            return_date: Return date for round-trip (omit for one-way)
            adults / children / infants: Passenger counts
            cabin_class: "M" (economy), "W" (premium), "C" (business), "F" (first)
            currency: 3-letter currency code
            limit: Max results (1-200)
            max_stopovers: Max connections per direction (0-4)
            sort: "price" or "duration"

        Returns:
            FlightSearchResult with offers from the cloud engine.
        """
        import asyncio
        from letsfg.local import search_local as _search

        result_dict = asyncio.run(_search(
            origin=origin,
            destination=destination,
            date_from=date_from,
            return_date=return_date,
            adults=adults,
            children=children,
            infants=infants,
            cabin_class=cabin_class,
            currency=currency,
            limit=limit,
            max_stopovers=max_stopovers,
            sort=sort,
        ))
        return FlightSearchResult.from_dict(result_dict)

    def book_local(
        self,
        search_id: str,
        offer_id: str,
        passenger: dict,
        contact_email: str,
        *,
        offer_ref: str | None = None,
    ) -> dict:
        """
        Book an offer from a cloud (Bearer token) search. FREE — ticket price only.

        Requires a Bearer token — run `letsfg auth` once. Use REAL passenger
        details — names must match the traveller's ID, and the airline sends
        e-tickets to contact_email.

        Args:
            search_id: The search_id from search_local()'s result.
            offer_id: The offer ID from search results.
            passenger: A single passenger dict (given_name, family_name, born_on,
                gender, phone_number, ...).
            contact_email: Contact email for the booking.
            offer_ref: Optional offer reference, if your search result included one.

        Returns:
            Either {"ok": True, "booked": True, "order_id": ..., "charged": 0} or,
            when the booking genuinely could not complete,
            {"ok": False, "booked": False, "booking_url": ..., "charged": 0} —
            a normal outcome, not an error. Nothing is charged either way beyond
            the ticket price itself.
        """
        import asyncio
        from letsfg.local import book_offer as _book_offer

        return asyncio.run(_book_offer(
            search_id=search_id,
            offer_id=offer_id,
            passenger=passenger,
            contact_email=contact_email,
            offer_ref=offer_ref,
        ))

    # ── Core API methods (requires API key) ───────────────────────────────

    def search(
        self,
        origin: str,
        destination: str,
        date_from: str,
        *,
        return_date: str | None = None,
        adults: int = 1,
        children: int = 0,
        infants: int = 0,
        cabin_class: str | None = None,
        max_stopovers: int = 2,
        currency: str = "EUR",
        limit: int = 20,
        sort: str = "price",
        departure_time_from: str | None = None,
        departure_time_to: str | None = None,
    ) -> FlightSearchResult:
        """
        Search for flights via the LetsFG cloud engine. FREE.

        Uses a Bearer token (PFS path) if available — run `letsfg auth` once or set
        LETSFG_BEARER_TOKEN. Falls back to the Developer API (LETSFG_API_KEY) if no
        Bearer token is present.

        Args:
            origin: IATA code (e.g., "LON", "GDN", "JFK")
            destination: IATA code (e.g., "BCN", "BER", "LAX")
            date_from: Departure date "YYYY-MM-DD"
            return_date: Return date for round-trip (omit for one-way)
            adults: Number of adult passengers (1-9)
            children: Number of children (0-9)
            infants: Number of infants (0-9)
            cabin_class: "M" (economy), "W" (premium), "C" (business), "F" (first)
            max_stopovers: Max connections per direction (0-4)
            currency: 3-letter currency code
            limit: Max results (1-100)
            sort: "price" or "duration"
            departure_time_from: Earliest departure time "HH:MM" (e.g. "06:00")
            departure_time_to: Latest departure time "HH:MM" (e.g. "14:00")

        Returns:
            FlightSearchResult with offers, passenger_ids, and metadata.
        """
        from letsfg.connectors.auth import get_bearer_token, BearerTokenError
        try:
            get_bearer_token()
            return self.search_local(
                origin=origin,
                destination=destination,
                date_from=date_from,
                return_date=return_date,
                adults=adults,
                children=children,
                infants=infants,
                cabin_class=cabin_class,
                currency=currency,
                limit=limit,
                max_stopovers=max_stopovers,
                sort=sort,
            )
        except BearerTokenError:
            pass

        self._require_api_key()
        body: dict = {
            "origin": origin,
            "destination": destination,
            "date_from": date_from,
            "adults": adults,
            "children": children,
            "currency": currency,
            "limit": limit,
            "sort": sort,
            "max_stopovers": max_stopovers,
        }
        if return_date:
            body["return_date"] = return_date
        if cabin_class:
            body["cabin_class"] = cabin_class
        if infants:
            body["infants"] = infants
        if departure_time_from:
            body["departure_time_from"] = departure_time_from
        if departure_time_to:
            body["departure_time_to"] = departure_time_to
        data = self._post("/api/v1/flights/search", body)
        return FlightSearchResult.from_dict(data)

    def resolve_location(self, query: str) -> list[dict]:
        """
        Resolve a city/airport name to IATA codes.

        Requires a Developer API key. There is NO location endpoint on the PFS
        Bearer lane.

        This used to try `GET {base}/api/locations?q=...` first whenever a
        Bearer token was present. That route has never existed on letsfg.co: it
        returns the 404 HTML page, so `json.loads` raised, and because the only
        thing caught here was BearerTokenError the failure surfaced as a raw
        HTTPError/JSONDecodeError instead of falling through to the working
        path below. Verified against production 2026-08-16 (404, text/html).
        If a PFS-lane resolver is ever added, restore this branch — but point
        it at a route that exists and check the status before parsing.

        Args:
            query: City or airport name (e.g., "London", "Berlin")

        Returns:
            List of matching locations with IATA codes.
        """
        self._require_api_key()
        data = self._get(f"/api/v1/flights/locations/{quote(query, safe='')}")
        if isinstance(data, dict) and "locations" in data:
            return data["locations"]
        if isinstance(data, list):
            return data
        return [data] if data else []

    def unlock(self, offer_id: str) -> UnlockResult:
        """
        RETIRED 2026-09-08. Raises instead of calling the server.

        There is no unlock step on any lane. Unlock existed to confirm a live price before
        charging; booking now HOLDS the fare on the connected payment method and captures only
        once a real airline PNR exists, so a fare that moved cannot become a charge for a ticket
        you did not get. If it moves at checkout you get a `price_change` question to accept or
        decline instead.

        Kept as a method, and raising locally rather than making the request, so an older caller
        gets one clear sentence at the line that is actually wrong — not a 410 body to decode, and
        not an AttributeError somewhere else.

        Raises:
            LetsFGError: always.
        """
        raise LetsFGError(
            "unlock() was retired on 2026-09-08 and the endpoint answers 410 Gone. There is no "
            "unlock step: call book() directly. The fare is held on the connected payment method "
            "and captured only against a real airline PNR. "
            "See https://letsfg.co/developers/api/docs",
            410,
        )

    def book(
        self,
        offer_id: str,
        passengers: list[dict | Passenger],
        contact_email: str,
        contact_phone: str = "",
        idempotency_key: str = "",
        search_id: str | None = None,
    ) -> BookingResult | dict:
        """
        Book a flight.

        Uses a Bearer token (PFS path) if available — run `letsfg auth` once or
        set LETSFG_BEARER_TOKEN. Free search; no booking fee and no transaction fee
        on booking (our margin is already in the price you saw) — pass
        search_id (from search_local()'s result) and only the first entry in
        passengers is used (one passenger per PFS booking). Returns a dict:
        either {"ok": True, "booked": True, "order_id": ...} or
        {"ok": False, "booked": False, "booking_url": ...} — the latter means
        the booking genuinely did not complete and nothing was charged; hand
        the link to the user, don't retry the same offer.

        Falls back to the Developer API (LETSFG_API_KEY) if no Bearer token is
        present. That path needs `search_id` as well (an offer is bookable only
        inside the search that produced it), takes NO unlock step, and returns
        the 202 dict: {ok, booking_id, state, held, charged: 0, poll_url}. Poll
        get_booking(booking_id) until `terminal`, or use book_and_wait().

        IMPORTANT (Developer API path): Always provide an idempotency_key to
        prevent double-bookings if your agent retries this call. Use any unique
        string (UUID, session ID, or deterministic hash of offer_id + passenger
        names).

        Args:
            offer_id: The offer ID from search results.
            passengers: List of passenger dicts or Passenger objects.
                Developer API: each must include id (pas_xxx from search),
                given_name, family_name, born_on (YYYY-MM-DD), gender, title.
            contact_email: Contact email for the booking.
            contact_phone: Contact phone (optional; Developer API only).
            idempotency_key: Unique key for this booking attempt (Developer API
                only). If the same key is sent twice, the second call returns
                the original booking instead of creating a duplicate.
            search_id: REQUIRED on both paths — the search_id the search returned.
                An offer can only be booked inside its own search. (Before
                2026-09-08 the Developer API path ignored this.)

        Returns:
            A dict on the PFS path, or a BookingResult on the Developer API path.
        """
        from letsfg.connectors.auth import get_bearer_token, BearerTokenError
        try:
            get_bearer_token()
            if not search_id:
                raise ValueError("search_id is required to book via PFS (pass the search_id from search_local()'s result).")
            passenger = passengers[0]
            if isinstance(passenger, Passenger):
                passenger = passenger.to_dict()
            if contact_phone and not passenger.get("phone_number"):
                passenger = {**passenger, "phone_number": contact_phone}
            return self.book_local(
                search_id=search_id,
                offer_id=offer_id,
                passenger=passenger,
                contact_email=contact_email,
            )
        except BearerTokenError:
            pass

        self._require_api_key()
        if not search_id:
            raise ValueError(
                "search_id is required to book on the Developer API — pass the search_id from "
                "search()'s result. An offer can only be booked inside the search that produced "
                "it. (Before 2026-09-08 this argument was ignored on this path; the retired "
                "/bookings/book route took an offer_id alone.)"
            )
        pax_list = []
        for p in passengers:
            if isinstance(p, Passenger):
                pax_list.append(p.to_dict())
            else:
                pax_list.append(p)
        if contact_phone and pax_list and not pax_list[0].get("phone_number"):
            pax_list[0] = {**pax_list[0], "phone_number": contact_phone}

        body: dict[str, Any] = {
            "search_id": search_id,
            "offer_id": offer_id,
            "passengers": pax_list,
            "contact_email": contact_email,
        }
        if idempotency_key:
            body["idempotency_key"] = idempotency_key
        # 202 + booking_id. The booking itself takes 4-11 minutes; poll get_booking().
        return self._post("/api/v1/flights/book", body)

    def get_booking(self, booking_id: str) -> dict:
        """
        Poll a Developer API flight booking.

        Poll every few seconds until `terminal` is true. The poll is ALSO how LetsFG knows you are
        still there, which is what keeps a booking paused on a question alive - so do not back off
        to minutes.

        States: authorised, card_issued, booking_in_progress, awaiting_settlement, then
        completed (with `pnr` and `charged_amount`), failed (hold released, nothing charged) or
        needs_attention (a human at LetsFG is on it - do not book again).

        While booking_in_progress, `question` may carry a seat map, a paid extra or a fare
        increase; answer it with answer_booking() within its window.
        """
        self._require_api_key()
        return self._get(f"/api/v1/flights/bookings/{booking_id}")

    def answer_booking(self, booking_id: str, round: int, *, seats: list[dict] | None = None,
                       confirm: bool = False, skip: bool = False) -> dict:
        """
        Answer the open `question` on a booking.

        Echo the question's `round`. A stale round is refused with 409 rather than guessed at, so
        an answer to an old question can never be applied to a new one.

        Seat question: seats=[...] to choose, or skip=True.
        Price change or paid extra: confirm=True to accept, skip=True to decline. Declining an
        extra still completes the booking, without it.
        """
        self._require_api_key()
        body: dict[str, Any] = {"round": round}
        if seats is not None:
            body["seats"] = seats
        if confirm:
            body["confirm"] = True
        if skip:
            body["skip"] = True
        return self._post(f"/api/v1/flights/bookings/{booking_id}/answer", body)

    def book_and_wait(self, offer_id: str, passengers: list, contact_email: str,
                      search_id: str, *, contact_phone: str = "", idempotency_key: str = "",
                      poll_seconds: float = 5.0, timeout_seconds: float = 900.0,
                      on_question: Callable[[dict], dict] | None = None) -> dict:
        """
        Book and poll to a terminal state. Mirrors book_hotel_and_wait().

        Blocks for as long as the booking takes (4-11 minutes typically), so use book() +
        get_booking() instead if your caller has a request timeout.

        `on_question` is called with the question dict and must return the kwargs for
        answer_booking() (e.g. {"confirm": True}). Without it, a fare increase is ACCEPTED and a
        paid extra is DECLINED - the conservative reading of "the traveller asked for this
        flight", and the same default the docs describe.
        """
        import time as _time

        started = self.book(offer_id=offer_id, passengers=passengers, contact_email=contact_email,
                            contact_phone=contact_phone, idempotency_key=idempotency_key,
                            search_id=search_id)
        if not isinstance(started, dict) or not started.get("ok"):
            return started  # a refusal: nothing was charged

        booking_id = started["booking_id"]
        deadline = _time.monotonic() + timeout_seconds
        while _time.monotonic() < deadline:
            _time.sleep(poll_seconds)
            state = self.get_booking(booking_id)
            question = state.get("question")
            if question:
                if on_question is not None:
                    kwargs = on_question(question)
                else:
                    kwargs = ({"skip": True} if question.get("kind") == "extra"
                              else {"confirm": True})
                self.answer_booking(booking_id, question["round"], **kwargs)
                continue
            if state.get("terminal"):
                return state
        return self.get_booking(booking_id)

    # ── Hotels ────────────────────────────────────────────────────────────────
    #
    # A card on file is required for EVERY hotel call, search included. That is
    # deliberate, not a bug: a hotel search opens a real session at the supplier
    # and booking blocks a real rate, so we will not let a caller reach the point
    # of commitment only to discover it cannot pay. The same card that authorises
    # flight booking authorises hotels; there is no separate hotel enrolment.
    #
    # Only free-cancellation, pay-later rates are sold. That is a commercial
    # choice: those are the rates where the guest's balance can safely be settled
    # with the supplier after booking, which is what makes 5%-now/rest-later
    # work at all. It also means the result set is smaller than a metasearch's,
    # and every row in it can actually be booked.

    HOTEL_SEARCH_TIMEOUT = 240
    HOTEL_CANCEL_TIMEOUT = 300

    def hotel_destinations(self, text: str) -> list[dict]:
        """
        Resolve a place name to the city id that :meth:`search_hotels` needs.

        Args:
            text: A place name, e.g. "Warsaw" or "Paris".

        Returns:
            Matches, best first. Use ``Id`` from the first entry as ``city_id``
            and ``Name`` as ``city_name``.
        """
        self._require_api_key()
        data = self._post("/api/v1/hotels/destinations", {"text": text}, timeout=60)
        return (data or {}).get("results", [])

    def search_hotels(
        self,
        city_id: int,
        city_name: str,
        check_in: str,
        check_out: str,
        adults: int = 2,
        children: int = 0,
        child_ages: list[int] | None = None,
        nationality: str = "PL",
        limit: int = 40,
        with_images: bool = True,
    ) -> dict:
        """
        Search real, bookable hotel inventory.

        Slow by nature — the supplier streams a whole city and every rate is
        priced — so this call gets its own generous timeout rather than the
        client default.

        Args:
            city_id: From :meth:`hotel_destinations`.
            city_name: From :meth:`hotel_destinations`.
            check_in: yyyy-MM-dd.
            check_out: yyyy-MM-dd.
            adults: Adult guests.
            children: Child guests. Pass ``child_ages`` when non-zero.
            child_ages: Age of each child, required by the supplier for pricing.
            nationality: Guest nationality, two-letter code. Rates and taxes
                genuinely differ by nationality, so this changes prices.
            limit: Maximum hotels to return.
            with_images: Include photo URLs.

        Returns:
            ``{"session_id", "currency", "count", "hotels": [...], "terms"}``.
            Each offer carries ``price`` (what the guest pays),
            ``reservation_fee_now`` (the 5% taken at booking),
            ``balance_to_supplier``, ``balance_due_by`` and
            ``free_cancellation_until``. There is no wholesale figure to quote
            by mistake.

            Keep ``session_id`` and the chosen offer's ``combination_id_v2``:
            together they identify the exact rate, and booking needs both.
        """
        self._require_api_key()
        body = {
            "city_id": city_id, "city_name": city_name,
            "check_in": check_in, "check_out": check_out,
            "adults": adults, "children": children,
            "nationality": nationality, "limit": limit,
            "with_images": with_images,
        }
        if child_ages:
            body["child_ages"] = child_ages
        return self._post("/api/v1/hotels/search", body,
                          timeout=self.HOTEL_SEARCH_TIMEOUT)

    def book_hotel(
        self,
        session_id: str,
        hotel_code: int,
        combination_id_v2: str,
        expected_price: float,
        expected_balance: float,
        city_id: int,
        city_name: str,
        check_in: str,
        check_out: str,
        guests: list[dict],
        email: str,
        phone: str,
        adults: int = 2,
        combination_id: int | None = None,
        hotel_name: str | None = None,
        phone_country_code: str = "48",
        special_requests: list[str] | None = None,
    ) -> dict:
        """
        Start a booking. Returns a job immediately — it does NOT book inline.

        A booking takes minutes: the rate is re-blocked at the supplier, every
        price and date rail is checked, the 5% reservation fee is charged to
        your card, and only then is the room committed. No proxy holds a
        connection that long, so this returns at once and you poll
        :meth:`hotel_booking` for the outcome. Use
        :meth:`book_hotel_and_wait` if you would rather block.

        Because the fee is taken BEFORE the commit, a declined card costs
        nothing to unwind: no reservation exists and nothing is charged.

        Args:
            session_id: From :meth:`search_hotels`.
            hotel_code: From the chosen hotel.
            combination_id_v2: From the chosen offer. Identifies that exact
                rate — room name alone is ambiguous, since the same room exists
                refundable and non-refundable at different prices.
            expected_price: The offer's ``price``, sent back verbatim. The
                booking is refused if the supplier has moved beyond tolerance,
                so a guest is never charged a price they did not agree to.
            expected_balance: The offer's ``balance_to_supplier``, verbatim.
            guests: ``[{"title": "Mr", "first_name": ..., "last_name": ...}]``.
            email: The voucher and the pay link go here. A typo loses the
                booking, so this is validated before anything is charged.
            phone: Guest contact number.

        Returns:
            ``{"booking_job_id", "status": "in_progress", "poll", ...}``.

            Do NOT call this again for the same rate while a job is running:
            that books the room twice and charges two reservation fees.
        """
        self._require_api_key()
        body = {
            "session_id": session_id, "hotel_code": hotel_code,
            "combination_id_v2": combination_id_v2,
            "expected_price": expected_price,
            "expected_balance": expected_balance,
            "city_id": city_id, "city_name": city_name,
            "check_in": check_in, "check_out": check_out, "adults": adults,
            "guests": guests, "email": email, "phone": phone,
            "phone_country_code": phone_country_code,
            "special_requests": special_requests or [],
        }
        if combination_id is not None:
            body["combination_id"] = combination_id
        if hotel_name:
            body["hotel_name"] = hotel_name
        return self._post("/api/v1/hotels/book", body, timeout=90)

    def hotel_booking(self, booking_job_id: str) -> dict:
        """
        Collect the result of a booking started with :meth:`book_hotel`.

        Returns:
            ``status`` is ``"in_progress"``, ``"succeeded"`` or ``"failed"``.
            On success: ``confirmation``, ``reservation_fee_charged``,
            ``pay_link``, ``balance_due``, ``balance_due_by`` and ``terms``
            (including the full cancellation ladder). On failure: ``error``,
            written to be shown to whoever asked for the booking.
        """
        self._require_api_key()
        return self._get(f"/api/v1/hotels/booking/{quote(booking_job_id, safe='')}",
                         timeout=60)

    def book_hotel_and_wait(
        self,
        *,
        poll_interval: int = 20,
        max_wait: int = 600,
        **kwargs: Any,
    ) -> dict:
        """
        :meth:`book_hotel`, then poll until the booking settles.

        Convenience only — it is the same two calls. Takes every argument
        :meth:`book_hotel` does.

        Args:
            poll_interval: Seconds between polls.
            max_wait: Give up waiting after this many seconds. Giving up does
                NOT cancel anything: the booking may still complete. The result
                carries the ``booking_job_id`` so you can keep polling, and the
                confirmation is emailed to the guest regardless.

        Returns:
            The final :meth:`hotel_booking` payload. ``status`` may still be
            ``"in_progress"`` if ``max_wait`` elapsed first.
        """
        job = self.book_hotel(**kwargs)
        job_id = job.get("booking_job_id")
        if not job_id:
            return job
        waited = 0
        result = job
        while waited < max_wait:
            time.sleep(poll_interval)
            waited += poll_interval
            result = self.hotel_booking(job_id)
            if result.get("status") in ("succeeded", "failed"):
                return result
        result.setdefault("booking_job_id", job_id)
        return result

    def cancel_hotel(self, confirmation: str) -> dict:
        """
        Release a reservation at the supplier.

        Free until ``balance_due_by``; after that the hotel's own cancellation
        ladder applies and can reach 100%. The ladder ships in the booking's
        ``terms``, so you can always see the cost before calling this.

        The 5% reservation fee is NOT refunded.

        This drives a browser at the supplier and takes over a minute. If it
        times out, do not assume it failed — re-check before retrying.

        Args:
            confirmation: The ``confirmation`` from the completed booking.
        """
        self._require_api_key()
        return self._post("/api/v1/hotels/cancel", {"confirmation": confirmation},
                          timeout=self.HOTEL_CANCEL_TIMEOUT)

    def connect_payment(self) -> dict:
        """
        [Developer API] Mint a one-time link for connecting a Revolut payment method.

        This replaced setup_payment on 2026-09-08. Nothing is charged to connect, and
        card details never touch LetsFG: the returned ``connect_url`` opens a hosted
        page where the developer saves a card, Revolut Pay or Google Pay.

        A person must open that URL in a browser — there is no endpoint that takes card
        details, so do not ask a user for a card number and do not try to automate it.

        Returns:
            Dict with ``status``, ``connect_url``, ``expires_in_seconds``, and ``payment``
            (the currently connected method, if any).
        """
        self._require_api_key()
        return self._post("/api/v1/agents/connect-payment", {})

    def setup_payment(self, token: str = "") -> dict:
        """
        RETIRED 2026-09-08 with Stripe. Raises instead of calling the server.

        ``/agents/setup-payment`` answers 410 Gone. Payment enrolment moved onto the same
        Revolut rail the rest of the product uses: call :meth:`connect_payment` and open
        the ``connect_url`` it returns.

        Kept as a method, and raising locally rather than making the request, for the same
        reason as :meth:`unlock` — an older caller gets one clear sentence at the line that
        is actually wrong, not a 410 body to decode and not an AttributeError elsewhere.

        Raises:
            LetsFGError: always.
        """
        raise LetsFGError(
            "setup_payment() was retired on 2026-09-08 with Stripe and the endpoint answers "
            "410 Gone. Call connect_payment() instead and open the connect_url it returns; "
            "nothing is charged to connect. "
            "See https://letsfg.co/developers/api/docs",
            410,
        )

    def start_checkout(
        self,
        offer_id: str,
        passengers: list[dict | Passenger] | None = None,
        *,
        checkout_token: str = "",
    ) -> CheckoutProgress:
        """
        RETIRED 2026-09-08. Raises instead of calling the server.

        ``/bookings/start-checkout`` answers 410 Gone, and its ``checkout_token``
        came from :meth:`unlock`, which was retired the same day. Booking runs
        server-side now: :meth:`book` holds the fare on the connected payment
        method, a LetsFG agent buys the ticket, and the hold is captured only once
        a real airline PNR exists.

        Kept as a method, and raising locally rather than making the request, for the
        same reason as :meth:`unlock` — an older caller gets one clear sentence at the
        line that is actually wrong.

        Raises:
            LetsFGError: always.
        """
        raise LetsFGError(
            "start_checkout() was retired on 2026-09-08 and the endpoint answers 410 Gone. "
            "Its checkout_token came from unlock(), which was retired the same day. Call "
            "book() instead: the fare is held on the connected payment method and captured "
            "only against a real airline PNR. "
            "See https://letsfg.co/developers/api/docs",
            410,
        )

    def start_checkout_local(self, *args, **kwargs) -> CheckoutProgress:
        """Removed — booking now runs server-side. Use book() instead."""
        raise NotImplementedError(
            "start_checkout_local() has been removed. "
            "Booking is handled server-side — use bt.book(offer_id, passengers, email)."
        )

    def me(self) -> AgentProfile:
        """Get the current agent's profile, usage, and payment status."""
        self._require_api_key()
        data = self._get("/api/v1/agents/me")
        return AgentProfile.from_dict(data)

    # ── Static methods (no auth needed) ───────────────────────────────────

    @staticmethod
    def register(
        agent_name: str,
        email: str,
        *,
        base_url: str | None = None,
        owner_name: str = "",
        description: str = "",
    ) -> dict:
        """
        Register a new agent — no API key needed.

        Args:
            agent_name: Your agent's name
            email: Contact email for billing
            base_url: API base URL (default: production)
            owner_name: Person/org name (optional)
            description: What your agent does (optional)

        Returns:
            Dict with agent_id, api_key, and instructions.
        """
        url = (base_url or DEFAULT_BASE_URL).rstrip("/")
        body = {
            "agent_name": agent_name,
            "email": email,
            "owner_name": owner_name,
            "description": description,
        }
        data = json.dumps(body).encode()
        req = Request(
            f"{url}/api/v1/agents/register",
            data=data,
            headers={"Content-Type": "application/json", "User-Agent": "LetsFG-Python-SDK/1.0.3"},
            method="POST",
        )
        try:
            with urlopen(req, timeout=30) as resp:
                return json.loads(resp.read().decode())
        except HTTPError as e:
            body_text = e.read().decode() if e.fp else ""
            try:
                err = json.loads(body_text)
            except Exception:
                err = {"detail": body_text}
            raise LetsFGError(
                err.get("detail", f"Registration failed ({e.code})"),
                status_code=e.code,
                response=err,
            ) from e

    # ── Internals ─────────────────────────────────────────────────────────

    def _headers(self) -> dict[str, str]:
        return {
            "Content-Type": "application/json",
            "X-API-Key": self.api_key,
            "User-Agent": "LetsFG-Python-SDK/1.0.3",
            "X-Client-Type": self._client_type,
        }

    def _post(self, path: str, body: dict, timeout: int | None = None) -> Any:
        url = f"{self.base_url}{path}"
        data = json.dumps(body).encode()
        req = Request(url, data=data, headers=self._headers(), method="POST")
        return self._do_request(req, timeout)

    def _get(self, path: str, timeout: int | None = None) -> Any:
        url = f"{self.base_url}{path}"
        req = Request(url, headers=self._headers(), method="GET")
        return self._do_request(req, timeout)

    def _do_request(self, req: Request, timeout: int | None = None) -> Any:
        # Per-call timeout, because one number cannot serve every endpoint here:
        # a flight search answers in seconds, a hotel search streams a whole
        # city's inventory, and a cancellation drives a browser at the supplier.
        try:
            with urlopen(req, timeout=timeout or self.timeout) as resp:
                return json.loads(resp.read().decode())
        except HTTPError as e:
            body_text = e.read().decode() if e.fp else ""
            try:
                err = json.loads(body_text)
            except Exception:
                err = {"detail": body_text}

            detail = err.get("detail", f"API error ({e.code})")
            code = err.get("error_code") or _infer_error_code(e.code, detail)

            if e.code == 401:
                raise AuthenticationError(detail, status_code=401, response=err, error_code=code) from e
            elif e.code == 402:
                raise PaymentRequiredError(detail, status_code=402, response=err, error_code=code) from e
            elif e.code == 410:
                raise OfferExpiredError(detail, status_code=410, response=err, error_code=code) from e
            elif e.code == 422:
                raise ValidationError(detail, status_code=422, response=err, error_code=code) from e
            else:
                raise LetsFGError(detail, status_code=e.code, response=err, error_code=code) from e
        except URLError as e:
            raise LetsFGError(
                f"Connection failed: {e.reason}",
                error_code=ErrorCode.NETWORK_ERROR,
            ) from e

    def __repr__(self) -> str:
        masked = self.api_key[:8] + "..." if len(self.api_key) > 8 else "***"
        return f"LetsFG(base_url={self.base_url!r}, api_key={masked!r})"


# Backward-compat aliases (deprecated — use LetsFG / LetsFGError directly)
BoostedTravel = LetsFG
BoostedTravelError = LetsFGError
