"""
Kiwi.com website connector — LCC flights + virtual interlining.

Scrapes the kiwi.com frontend GraphQL API (umbrella/v2/graphql) which powers
their search results page. Zero auth required — just needs the Referer header.

The old Skypicker REST API (api.skypicker.com/flights) and Tequila API both
require paid API keys. This connector uses the same GraphQL endpoint that the
kiwi.com website uses, which is free and rate-limit-friendly.

Supports one-way and return itineraries, airport or city-level searches,
and all the Kiwi virtual interlining magic (combining LCC one-way fares).
"""

from __future__ import annotations

import asyncio
import re
import hashlib
import logging
import time
from datetime import date, datetime
from typing import Any, Optional

import httpx

from ..models.flights import (
    FlightOffer,
    FlightRoute,
    FlightSearchRequest,
    FlightSearchResponse,
    FlightSegment,
)

logger = logging.getLogger(__name__)

_ancillary_cache: dict[str, tuple[float, dict]] = {}
_ANCILLARY_CACHE_TTL = 1800  # 30 min
KIWI_GRAPHQL_URL = "https://api.skypicker.com/umbrella/v2/graphql"
KIWI_LOCATIONS_URL = "https://api.skypicker.com/locations"

# Cache IATA → Kiwi city slug (e.g. LHR → "london-united-kingdom")
_slug_cache: dict[str, str] = {}

_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
    "Content-Type": "application/json",
    "Accept": "application/json",
    "Accept-Language": "en-GB,en;q=0.9",
    "Referer": "https://www.kiwi.com/",
    "Origin": "https://www.kiwi.com",
}

# Minimal GraphQL query for one-way flights
_ONEWAY_QUERY = """query SearchOnewayItinerariesQuery(
  $search: SearchOnewayInput
  $filter: ItinerariesFilterInput
  $options: ItinerariesOptionsInput
) {
  onewayItineraries(search: $search, filter: $filter, options: $options) {
    __typename
    ... on AppError { error: message }
    ... on Itineraries {
      metadata { itinerariesCount hasMorePending }
      itineraries {
        __typename
        ... on ItineraryOneWay {
          id
          price { amount }
          priceEur { amount }
          provider { name code }
          duration
          sector {
            sectorSegments {
              segment {
                source { localTime utcTimeIso station { code name city { name } } }
                destination { localTime utcTimeIso station { code name city { name } } }
                duration
                type
                code
                carrier { name code }
                operatingCarrier { name code }
                cabinClass
              }
              layover { duration }
            }
            duration
          }
          bookingOptions { edges { node { bookingUrl price { amount } } } }
          travelHack { isVirtualInterlining isThrowawayTicket isTrueHiddenCity }
          paidGuaranteePrice { amount }
          bagsInfo {
            includedHandBags
            includedCheckedBags
            includedPersonalItem
            hasNoCheckedBaggage
            checkedBagTiers { tierPrice { amount } }
            handBagTiers { tierPrice { amount } }
            personalItemTiers { tierPrice { amount } }
          }
          benefitsData {
            guaranteeAvailable
            guaranteeFee { amount }
          }
          extendedFareOptionsPricing {
            standardFarePriceOnly { amount }
            flexiFarePriceOnly { amount }
          }
        }
      }
    }
  }
}"""

# Minimal GraphQL query for return flights
_RETURN_QUERY = """query SearchReturnItinerariesQuery(
  $search: SearchReturnInput
  $filter: ItinerariesFilterInput
  $options: ItinerariesOptionsInput
) {
  returnItineraries(search: $search, filter: $filter, options: $options) {
    __typename
    ... on AppError { error: message }
    ... on Itineraries {
      metadata { itinerariesCount hasMorePending }
      itineraries {
        __typename
        ... on ItineraryReturn {
          id
          price { amount }
          priceEur { amount }
          provider { name code }
          duration
          outbound {
            sectorSegments {
              segment {
                source { localTime utcTimeIso station { code name city { name } } }
                destination { localTime utcTimeIso station { code name city { name } } }
                duration
                type
                code
                carrier { name code }
                operatingCarrier { name code }
                cabinClass
              }
              layover { duration }
            }
            duration
          }
          inbound {
            sectorSegments {
              segment {
                source { localTime utcTimeIso station { code name city { name } } }
                destination { localTime utcTimeIso station { code name city { name } } }
                duration
                type
                code
                carrier { name code }
                operatingCarrier { name code }
                cabinClass
              }
              layover { duration }
            }
            duration
          }
          bookingOptions { edges { node { bookingUrl price { amount } } } }
          travelHack { isVirtualInterlining isThrowawayTicket isTrueHiddenCity }
          paidGuaranteePrice { amount }
          bagsInfo {
            includedHandBags
            includedCheckedBags
            includedPersonalItem
            hasNoCheckedBaggage
            checkedBagTiers { tierPrice { amount } }
            handBagTiers { tierPrice { amount } }
            personalItemTiers { tierPrice { amount } }
          }
          benefitsData {
            guaranteeAvailable
            guaranteeFee { amount }
          }
          extendedFareOptionsPricing {
            standardFarePriceOnly { amount }
            flexiFarePriceOnly { amount }
          }
        }
      }
    }
  }
}"""

# Cabin class mapping
_CABIN_MAP = {"M": "ECONOMY", "W": "PREMIUM_ECONOMY", "C": "BUSINESS", "F": "FIRST"}


class KiwiConnectorClient:
    """
    Kiwi.com website connector — scrapes their frontend GraphQL API.

    No API key required. Uses the same endpoint as kiwi.com website.
    Supports IATA codes (e.g. STN, BCN) and city-level searches.
    """

    def __init__(self, timeout: float = 25.0):
        self.timeout = timeout
        self._http: Optional[httpx.AsyncClient] = None
        # Booking token captured from last _search_ow call (for live ancillary price fetch)
        self._last_token: str = ""

    @property
    def available(self) -> bool:
        return True

    async def _client(self) -> httpx.AsyncClient:
        if self._http is None or self._http.is_closed:
            from .browser import get_httpx_proxy_url
            self._http = httpx.AsyncClient(
                timeout=self.timeout,
                headers=_HEADERS,
                follow_redirects=True,
                proxy=get_httpx_proxy_url(),
            )
        return self._http

    async def close(self):
        if self._http and not self._http.is_closed:
            await self._http.aclose()

    _CURRENCY_MARKET = {
        "PLN": "pl", "EUR": "de", "GBP": "gb", "USD": "us",
        "CZK": "cz", "HUF": "hu", "SEK": "se", "NOK": "no",
        "DKK": "dk", "CHF": "ch", "RON": "ro", "BGN": "bg",
        "HRK": "hr", "TRY": "tr", "RUB": "ru", "UAH": "ua",
    }

    def _guess_market(self, currency: str) -> str:
        return self._CURRENCY_MARKET.get(currency.upper(), "gb")

    async def _resolve_slug(self, iata: str) -> str:
        """Resolve an IATA code to a Kiwi city slug via their locations API.

        Returns e.g. 'london-united-kingdom' for LHR, cached after first lookup.
        Falls back to empty string on failure.
        """
        iata = iata.strip().upper()
        if iata in _slug_cache:
            return _slug_cache[iata]
        try:
            client = await self._client()
            resp = await client.get(
                KIWI_LOCATIONS_URL,
                params={"term": iata, "locale": "en-US", "location_types": "airport", "limit": "1", "active_only": "true"},
                headers={"Referer": "https://www.kiwi.com/", "Accept": "application/json"},
            )
            if resp.status_code == 200:
                data = resp.json()
                locs = data.get("locations", [])
                if locs:
                    slug = locs[0].get("city", {}).get("slug", "")
                    if slug:
                        _slug_cache[iata] = slug
                        return slug
        except Exception as e:
            logger.debug("Kiwi slug resolution failed for %s: %s", iata, e)
        _slug_cache[iata] = ""
        return ""

    # IATA city codes that map to multiple airports.
    # Kiwi's GraphQL needs individual airport IDs; city codes like "LON"
    # are NOT valid Station:airport: values and silently return 0 results.
    _CITY_AIRPORTS: dict[str, list[str]] = {
        "LON": ["LHR", "LGW", "STN", "LCY", "LTN", "SEN"],
        "NYC": ["JFK", "LGA", "EWR"],
        "PAR": ["CDG", "ORY"],
        "MIL": ["MXP", "LIN", "BGY"],
        "BER": ["BER"],
        "TYO": ["NRT", "HND"],
        "OSA": ["KIX", "ITM"],
        "MOW": ["SVO", "DME", "VKO"],
        "BUE": ["EZE", "AEP"],
        "SAO": ["GRU", "CGH", "VCP"],
        "WAS": ["IAD", "DCA", "BWI"],
        "CHI": ["ORD", "MDW"],
        "SEL": ["ICN", "GMP"],
        "BJS": ["PEK", "PKX"],
        "SHA": ["PVG", "SHA"],  # Shanghai city code
        "DEL": ["DEL"],
        "BOM": ["BOM"],
        "STO": ["ARN", "BMA", "NYO"],
        "ROM": ["FCO", "CIA"],
        "DXB": ["DXB", "DWC"],
        "IST": ["IST", "SAW"],
        "BKK": ["BKK", "DMK"],
        "JKT": ["CGK", "HLP"],
        "KUL": ["KUL", "SZB"],
        "MEX": ["MEX", "NLU"],
        "YTO": ["YYZ", "YTZ", "YHM"],
        "YMQ": ["YUL", "YMX"],
    }

    def _location_ids(self, code: str) -> list[str]:
        """Convert IATA code to list of Kiwi location IDs.

        City codes (LON, NYC, etc.) are expanded to their constituent airports
        because Kiwi's GraphQL only accepts airport-level Station IDs.
        """
        code = code.strip().upper()
        if code in self._CITY_AIRPORTS:
            return [f"Station:airport:{a}" for a in self._CITY_AIRPORTS[code]]
        if len(code) == 3 and code.isalpha():
            return [f"Station:airport:{code}"]
        return [code]

    def _build_variables(self, req: FlightSearchRequest, is_return: bool) -> dict:
        """Build GraphQL variables from FlightSearchRequest."""
        date_str = f"{req.date_from.isoformat()}T00:00:00"
        date_end = f"{req.date_from.isoformat()}T23:59:59"

        itinerary: dict[str, Any] = {
            "source": {"ids": self._location_ids(req.origin)},
            "destination": {"ids": self._location_ids(req.destination)},
            "outboundDepartureDate": {"start": date_str, "end": date_end},
        }

        if is_return and req.return_from:
            ret_str = f"{req.return_from.isoformat()}T00:00:00"
            ret_end = f"{req.return_from.isoformat()}T23:59:59"
            itinerary["inboundDepartureDate"] = {"start": ret_str, "end": ret_end}

        cabin = _CABIN_MAP.get(req.cabin_class, "ECONOMY") if req.cabin_class else "ECONOMY"

        return {
            "search": {
                "itinerary": itinerary,
                "passengers": {
                    "adults": req.adults,
                    "children": req.children,
                    "infants": req.infants,
                    "adultsHoldBags": [0] * req.adults,
                    "adultsHandBags": [0] * req.adults,
                    "childrenHoldBags": [0] * req.children,
                    "childrenHandBags": [0] * req.children,
                },
                "cabinClass": {
                    "cabinClass": cabin,
                    "applyMixedClasses": False,
                },
            },
            "filter": {
                "transportTypes": ["FLIGHT"],
                "limit": min(req.limit or 100, 100),
                "enableSelfTransfer": True,
                "enableThrowAwayTicketing": True,
                "enableTrueHiddenCity": True,
                **({"maxStopsCount": req.max_stopovers} if req.max_stopovers is not None else {}),
            },
            "options": {
                "currency": req.currency.lower(),
                "locale": req.locale.split("-")[0] if req.locale else "en",
                "market": self._guess_market(req.currency),
                "partner": "skypicker",
                "sortBy": "PRICE",
            },
        }

    async def search_flights(self, req: FlightSearchRequest) -> FlightSearchResponse:
        ob_result = await self._search_ow(req)
        if req.return_from and ob_result.total_results > 0:
            # When return_from is set, _search_ow fires the RT GraphQL query
            # (_RETURN_QUERY with inboundDepartureDate). The response already
            # contains complete RT offers (total price + inbound leg populated).
            # Combining again would double-count the return leg cost.
            if any(o.inbound is not None for o in ob_result.offers):
                return ob_result
            ib_req = req.model_copy(update={"origin": req.destination, "destination": req.origin, "date_from": req.return_from, "return_from": None})
            ib_result = await self._search_ow(ib_req)
            if ib_result.total_results > 0:
                ob_result.offers = self._combine_rt(ob_result.offers, ib_result.offers, req)
                ob_result.total_results = len(ob_result.offers)
        if ob_result.offers:
            segs = ob_result.offers[0].outbound.segments if ob_result.offers[0].outbound else []
            anc_origin = segs[0].origin if segs else req.origin
            anc_dest = segs[-1].destination if segs else req.destination
            _ = anc_origin, anc_dest  # noqa: F841 — kept for future logging
        return ob_result

    async def _fetch_ancillaries(
        self, origin: str, dest: str, date_str: str, adults: int, currency: str,
        booking_token: str = "",
    ) -> dict | None:
        """Fetch live bag/seat prices from Kiwi's check_flights API.

        If a booking_token is available (extracted from search results), calls the
        check_flights endpoint to get route-specific live prices.  Falls back to
        conservative static estimates on failure.  Results are cached 30 min.

        Always surfaces:
        - kiwi_guarantee: Kiwi adds ~9 € at checkout by default (can deselect)
        - carry_on: personal item vs cabin bag add-on price
        - checked_bag: hold bag add-on price (Kiwi markup — higher than airline direct)
        - seat: seat selection add-on range at Kiwi checkout
        """
        cache_key = f"{origin}:{dest}:{date_str}:{currency}"
        cached = _ancillary_cache.get(cache_key)
        if cached and (time.monotonic() - cached[0] < _ANCILLARY_CACHE_TTL):
            return cached[1]

        if booking_token:
            try:
                result = await self._live_ancillary_prices(booking_token, adults, currency)
                if result:
                    _ancillary_cache[cache_key] = (time.monotonic(), result)
                    logger.debug("Kiwi live ancillary prices fetched for %s→%s", origin, dest)
                    return result
            except Exception as _e:
                logger.debug("Kiwi check_flights failed for %s→%s: %s", origin, dest, _e)

        # Static fallback — based on observed Kiwi checkout prices (LHR→BCN May 2026)
        static: dict = {
            "kiwi_guarantee": (
                "Kiwi adds ~9 € Guarantee fee by default at checkout —"
                " select \u2018Basic\u2019 to remove it"
            ),
            "carry_on": (
                "personal item (under-seat): free; "
                "cabin bag add-on from ~49 € at Kiwi checkout"
            ),
            "checked_bag": (
                "checked bag: add-on from ~52 € at Kiwi checkout —"
                " Kiwi markup; book airline direct for lower fees"
            ),
            "seat": (
                "seat selection: standard from ~21 €, extra legroom from ~50 € up to ~65 € at Kiwi checkout"
                " (skip to get a free random seat)"
            ),
            # Numeric fields used by _apply_ancillaries to set bags_price
            # (prevents engine from applying wrong airline-direct ref prices)
            "cabin_bag_price": 49.0,
            "checked_bag_price": 52.0,
            "seat_from_price": 21.0,
            "currency": "EUR",
        }
        _ancillary_cache[cache_key] = (time.monotonic(), static)
        return static

    async def _live_ancillary_prices(self, booking_token: str, adults: int, currency: str) -> dict | None:
        """Call Kiwi check_flights API to get actual bag prices for this offer.

        Endpoint: GET https://api.skypicker.com/check_flights
        Returns bags_fee (hold bags) and hand_bags (cabin bags) with prices.
        """
        client = await self._client()
        resp = await client.get(
            "https://api.skypicker.com/check_flights",
            params={
                "booking_token": booking_token,
                "bnum": "0",
                "adults": str(adults),
                "children": "0",
                "infants": "0",
                "curr": currency.lower(),
                "v": "2",
            },
            headers={"Referer": "https://www.kiwi.com/", "Accept": "application/json"},
        )
        if resp.status_code != 200:
            return None

        try:
            data = resp.json()
        except Exception:
            return None

        if not data.get("flights_checked"):
            return None

        curr_key = currency.lower()

        def _price_from(entry: dict) -> float | None:
            """Extract a price value from a Kiwi price dict, trying multiple keys."""
            for k in (curr_key, "amount", "eur"):
                v = entry.get(k)
                if v is not None:
                    try:
                        return float(v)
                    except (TypeError, ValueError):
                        pass
            return None

        # bags_fee: checked hold bags {"1": {"eur": 49.93}, ...}
        bags_fee = data.get("bags_fee") or {}
        bag1_entry = bags_fee.get("1") or bags_fee.get(1) or {}
        bag1_price = _price_from(bag1_entry) if isinstance(bag1_entry, dict) else None

        # hand_bags: cabin bags {"0": {"eur": 0}, "1": {"eur": 48.57}}
        hand_bags = data.get("hand_bags") or {}
        hb1_entry = hand_bags.get("1") or hand_bags.get(1) or {}
        hb1_price = _price_from(hb1_entry) if isinstance(hb1_entry, dict) else None

        if bag1_price is None and hb1_price is None:
            return None

        curr_sym = currency.upper()
        result: dict = {
            "kiwi_guarantee": (
                "Kiwi adds ~9 € Guarantee fee by default at checkout —"
                " select \u2018Basic\u2019 to remove it"
            ),
            "seat": (
                "seat selection: standard from ~21 €, extra legroom from ~50 € up to ~65 € at Kiwi checkout"
                " (skip to get a free random seat)"
            ),
        }

        if hb1_price is not None:
            if hb1_price == 0:
                result["carry_on"] = "personal item & cabin bag: included"
                result["cabin_bag_price"] = 0.0
            else:
                result["carry_on"] = (
                    f"personal item (under-seat): free; "
                    f"cabin bag: +{hb1_price:.0f} {curr_sym} at Kiwi checkout"
                )
                result["cabin_bag_price"] = hb1_price
        else:
            result["carry_on"] = "personal item: free; cabin bag add-on from ~49 € at Kiwi checkout"
            result["cabin_bag_price"] = 49.0

        if bag1_price is not None:
            result["checked_bag"] = (
                f"checked bag: +{bag1_price:.0f} {curr_sym} at Kiwi checkout —"
                " Kiwi markup; book airline direct for lower fees"
            )
            result["checked_bag_price"] = bag1_price
        else:
            result["checked_bag"] = (
                "checked bag: add-on from ~52 € at Kiwi checkout —"
                " Kiwi markup; book airline direct for lower fees"
            )
            result["checked_bag_price"] = 52.0

        result["seat_from_price"] = 21.0
        result["currency"] = currency.upper()
        return result

    def _apply_ancillaries(self, offers: list, ancillary: dict) -> None:
        carry_on = ancillary.get("carry_on") or ancillary.get("bags_note")
        checked_bag = ancillary.get("checked_bag") or ancillary.get("checked_bag_note")
        seat = ancillary.get("seat") or ancillary.get("seat_note")
        kiwi_guarantee = ancillary.get("kiwi_guarantee")
        cabin_bag_price = ancillary.get("cabin_bag_price")
        checked_bag_price = ancillary.get("checked_bag_price")
        seat_from_price = ancillary.get("seat_from_price")
        anc_currency = ancillary.get("currency", "EUR")
        for offer in offers:
            currency_matches = getattr(offer, "currency", "").upper() == anc_currency.upper()
            # Set numeric bags_price values — prevents engine from applying wrong
            # airline-direct ref prices (e.g., Vueling 6 EUR seat) to Kiwi offers
            # where Kiwi charges its own higher prices (21 EUR+ for seats).
            if currency_matches:
                if cabin_bag_price is not None:
                    offer.bags_price["carry_on"] = cabin_bag_price
                if checked_bag_price is not None:
                    offer.bags_price["checked_bag"] = checked_bag_price
                if seat_from_price is not None:
                    offer.bags_price["seat_selection"] = seat_from_price
            # Always set descriptive condition notes
            if carry_on:
                offer.conditions["cabin_bag"] = carry_on
            if checked_bag:
                offer.conditions["checked_bag"] = checked_bag
            if seat:
                offer.conditions["seat"] = seat
            if kiwi_guarantee:
                offer.conditions["kiwi_guarantee"] = kiwi_guarantee

    async def _search_ow(self, req: FlightSearchRequest) -> FlightSearchResponse:
        """Search flights via Kiwi.com's frontend GraphQL API."""
        client = await self._client()
        is_return = bool(req.return_from)

        query = _RETURN_QUERY if is_return else _ONEWAY_QUERY
        feature = "SearchReturnItinerariesQuery" if is_return else "SearchOnewayItinerariesQuery"
        variables = self._build_variables(req, is_return)

        t0 = time.monotonic()

        try:
            resp = await client.post(
                f"{KIWI_GRAPHQL_URL}?featureName={feature}",
                json={"query": query, "variables": variables},
            )
        except httpx.TimeoutException:
            logger.warning("Kiwi.com GraphQL timed out")
            return self._empty(req)
        except Exception as e:
            logger.error("Kiwi.com GraphQL error: %s", e)
            return self._empty(req)

        elapsed = time.monotonic() - t0

        if resp.status_code == 429:
            logger.warning("Kiwi.com rate limited (429)")
            return self._empty(req)

        if resp.status_code != 200:
            logger.warning("Kiwi.com returned %d: %s", resp.status_code, resp.text[:300])
            return self._empty(req)

        try:
            raw = resp.json()
        except Exception:
            logger.warning("Kiwi.com returned non-JSON")
            return self._empty(req)

        # Extract itineraries from response
        data = raw.get("data", {})
        root_key = "returnItineraries" if is_return else "onewayItineraries"
        result = data.get(root_key, {})

        if result.get("__typename") == "AppError":
            logger.warning("Kiwi.com error: %s", result.get("error", "unknown"))
            return self._empty(req)

        itineraries = result.get("itineraries", [])
        total = result.get("metadata", {}).get("itinerariesCount", len(itineraries))

        logger.info(
            "Kiwi.com %s→%s returned %d offers (total %d) in %.1fs",
            req.origin, req.destination, len(itineraries), total, elapsed,
        )

        offers = []
        # Resolve Kiwi city slugs for booking URLs (one call per unique IATA)
        origin_slug = await self._resolve_slug(req.origin)
        dest_slug = await self._resolve_slug(req.destination)

        # Extract first booking token for live ancillary pricing (check_flights API)
        self._last_token = ""
        for _itin in itineraries:
            edges = (_itin.get("bookingOptions") or {}).get("edges") or []
            if edges:
                _raw_url = (edges[0].get("node") or {}).get("bookingUrl", "")
                _m = re.search(r'[?&](?:booking_)?token=([^&\s]+)', _raw_url)
                if _m:
                    self._last_token = _m.group(1)
                    break

        for itin in itineraries:
            try:
                offer = self._parse_itinerary(itin, req, is_return, origin_slug, dest_slug)
                if offer:
                    offers.append(offer)
            except Exception as e:
                logger.debug("Failed to parse Kiwi itinerary: %s", e)
                continue

        search_hash = hashlib.md5(
            f"kiwiscrape{req.origin}{req.destination}{req.date_from}".encode()
        ).hexdigest()[:12]

        return FlightSearchResponse(
            search_id=f"fs_{search_hash}",
            origin=req.origin,
            destination=req.destination,
            currency=req.currency,
            offers=offers,
            total_results=total,
        )

    def _parse_kiwi_ancillaries(self, itin: dict, currency: str) -> tuple[dict, dict]:
        """Extract live bag/fee conditions and bags_price from a Kiwi GraphQL itinerary.

        Returns (conditions_dict, bags_price_dict).  All prices are per-offer live values
        fetched inline from the search response — no extra API call required.
        """
        curr = currency.upper()
        conditions: dict = {}
        bags_price: dict = {}

        bags_info = itin.get("bagsInfo") or {}
        hand_included = int(bags_info.get("includedHandBags") or 0)
        checked_included = int(bags_info.get("includedCheckedBags") or 0)

        # Cabin bag (hand baggage)
        hand_tiers = bags_info.get("handBagTiers") or []
        if hand_included > 0:
            conditions["carry_on"] = "personal item & cabin bag: included"
            bags_price["carry_on"] = 0.0
        elif hand_tiers:
            try:
                hp = float(hand_tiers[0].get("tierPrice", {}).get("amount", 0))
                conditions["carry_on"] = (
                    f"personal item (under-seat): free; "
                    f"cabin bag add-on: +{hp:.0f} {curr} at Kiwi checkout"
                )
                bags_price["carry_on"] = hp
            except (TypeError, ValueError):
                conditions["carry_on"] = "personal item: free; cabin bag: add-on at Kiwi checkout"
        else:
            conditions["carry_on"] = "personal item (under-seat): free; cabin bag: add-on at Kiwi checkout"

        # Checked bag
        checked_tiers = bags_info.get("checkedBagTiers") or []
        if checked_included > 0:
            conditions["checked_bag"] = f"{checked_included} checked bag(s): included"
            bags_price["checked_bag"] = 0.0
        elif checked_tiers:
            try:
                cp = float(checked_tiers[0].get("tierPrice", {}).get("amount", 0))
                conditions["checked_bag"] = (
                    f"checked bag add-on: +{cp:.0f} {curr} at Kiwi checkout"
                )
                bags_price["checked_bag"] = cp
            except (TypeError, ValueError):
                conditions["checked_bag"] = "checked bag: add-on at Kiwi checkout"
        else:
            conditions["checked_bag"] = "checked bag: add-on at Kiwi checkout"

        # Kiwi Guarantee fee — use benefitsData.guaranteeFee (what user actually pays)
        # paidGuaranteePrice is the coverage payout amount, NOT the fee charged to user
        benefits = itin.get("benefitsData") or {}
        gfee_raw = (benefits.get("guaranteeFee") or {}).get("amount")
        if gfee_raw is not None:
            try:
                gf = float(gfee_raw)
                conditions["kiwi_guarantee"] = (
                    f"Kiwi Guarantee: +{gf:.2f} {curr} (optional at checkout; choose \u2018Basic\u2019 to skip)"
                )
            except (TypeError, ValueError):
                pass

        if "kiwi_guarantee" not in conditions:
            # Fallback: paidGuaranteePrice is an approximation (coverage amount, not fee)
            guarantee_price = itin.get("paidGuaranteePrice") or {}
            gf_raw = guarantee_price.get("amount")
            if gf_raw is not None:
                try:
                    gf = float(gf_raw)
                    conditions["kiwi_guarantee"] = (
                        f"Kiwi Guarantee: ~{gf:.0f} {curr} (optional at checkout; choose \u2018Basic\u2019 to skip)"
                    )
                except (TypeError, ValueError):
                    pass

        # Fare flexibility upgrades
        fare_pricing = itin.get("extendedFareOptionsPricing") or {}
        std_up_raw = (fare_pricing.get("standardFarePriceOnly") or {}).get("amount")
        flexi_up_raw = (fare_pricing.get("flexiFarePriceOnly") or {}).get("amount")
        parts = []
        if std_up_raw is not None:
            try:
                parts.append(f"Standard +{float(std_up_raw):.0f} {curr}")
            except (TypeError, ValueError):
                pass
        if flexi_up_raw is not None:
            try:
                parts.append(f"Flexi +{float(flexi_up_raw):.0f} {curr}")
            except (TypeError, ValueError):
                pass
        if parts:
            conditions["fare_options"] = "Flexibility upgrades: " + ", ".join(parts)

        # Seat selection — prices not in search API; static estimate to block ancillary_ref.py
        conditions["seat"] = (
            "seat selection: standard from ~\u20ac21, extra legroom from ~\u20ac50 "
            "(optional at Kiwi checkout; skip for free random seat)"
        )
        bags_price["seat_selection"] = 21.0

        return conditions, bags_price

    def _parse_itinerary(
        self, itin: dict, req: FlightSearchRequest, is_return: bool,
        origin_slug: str = "", dest_slug: str = "",
    ) -> Optional[FlightOffer]:
        """Parse a Kiwi.com GraphQL itinerary into a FlightOffer."""
        price = float(itin.get("price", {}).get("amount", 0))
        if price <= 0:
            return None

        currency = req.currency

        # Parse outbound
        if is_return:
            outbound_data = itin.get("outbound", {})
            inbound_data = itin.get("inbound", {})
        else:
            outbound_data = itin.get("sector", {})
            inbound_data = None

        outbound = self._parse_sector(outbound_data, req) if outbound_data else None
        inbound = self._parse_sector(inbound_data, req) if inbound_data else None

        if not outbound:
            return None

        # Collect airlines — use outbound only for offer-level summary;
        # inbound airlines would mislead when this offer is used as a one-way leg
        airlines = sorted({s.airline for s in outbound.segments if s.airline})

        # owner_airline = first outbound segment carrier (deterministic)
        owner_airline = outbound.segments[0].airline if outbound.segments else (airlines[0] if airlines else "")

        # Travel hack info
        travel_hack = itin.get("travelHack", {}) or {}
        conditions = {}
        if travel_hack.get("isVirtualInterlining"):
            conditions["virtual_interlining"] = "Different airlines combined for best price"
        if travel_hack.get("isThrowawayTicket"):
            conditions["throwaway_ticket"] = "Only using first leg of ticket"
        if travel_hack.get("isTrueHiddenCity"):
            conditions["hidden_city"] = "Hidden city ticketing"

        # Extract booking URL — ensure it's a full URL
        booking_url = ""
        booking_options = itin.get("bookingOptions", {}).get("edges", [])
        if booking_options:
            raw_url = booking_options[0].get("node", {}).get("bookingUrl", "")
            if raw_url:
                # Kiwi sometimes returns relative paths — prefix with base URL
                if raw_url.startswith("/"):
                    booking_url = f"https://www.kiwi.com{raw_url}"
                elif raw_url.startswith("http"):
                    booking_url = raw_url
                else:
                    booking_url = f"https://www.kiwi.com/{raw_url}"

        # Build a stable search deeplink (token URLs expire in minutes)
        if outbound and outbound.segments and origin_slug and dest_slug:
            first_seg = outbound.segments[0]
            dep_date = first_seg.departure.strftime("%Y-%m-%d") if first_seg.departure.year > 2000 else ""
            search_deeplink = (
                f"https://www.kiwi.com/en/search/results"
                f"/{origin_slug}/{dest_slug}/{dep_date}"
            )
            if inbound and inbound.segments:
                ret_first = inbound.segments[0]
                ret_date = ret_first.departure.strftime("%Y-%m-%d") if ret_first.departure.year > 2000 else ""
                if ret_date:
                    search_deeplink += f"/{ret_date}"
            else:
                # One-way search — append no-return so Kiwi doesn't default to return
                search_deeplink += "/no-return"

            # Add query params: direct filter if applicable, sort by price
            params = ["sortBy=price"]
            if outbound.stopovers == 0:
                params.append("stopNumber=0")
            search_deeplink += "?" + "&".join(params)

            # Always use stable search deeplink — token URLs expire in minutes
            # and are useless by the time the user clicks
            booking_url = search_deeplink

        itin_id = itin.get("id", "")
        offer_id = f"ks_{hashlib.md5(itin_id.encode()).hexdigest()[:12]}" if itin_id else f"ks_{hashlib.md5(f'{price}{airlines}'.encode()).hexdigest()[:12]}"

        # Parse live ancillary prices (bag/guarantee/fare tiers) from search response
        anc_conditions, anc_bags_price = self._parse_kiwi_ancillaries(itin, currency)
        conditions.update(anc_conditions)

        return FlightOffer(
            id=offer_id,
            price=price,
            currency=currency,
            price_formatted=f"{price:.2f} {currency}",
            outbound=outbound,
            inbound=inbound,
            airlines=airlines,
            owner_airline=owner_airline,
            booking_url=booking_url,
            is_locked=False,
            conditions=conditions,
            bags_price=anc_bags_price,
            source="kiwi_connector",
            source_tier="free",
        )

    def _parse_sector(self, sector: dict, req: FlightSearchRequest) -> Optional[FlightRoute]:
        """Parse a sector (outbound/inbound) into a FlightRoute."""
        sector_segments = sector.get("sectorSegments", [])
        if not sector_segments:
            return None

        segments = []
        for ss in sector_segments:
            seg = ss.get("segment", {})
            if not seg:
                continue

            source = seg.get("source", {})
            dest = seg.get("destination", {})

            dep_dt = self._parse_dt(source.get("localTime", ""))
            arr_dt = self._parse_dt(dest.get("localTime", ""))

            carrier = seg.get("carrier", {}) or {}
            op_carrier = seg.get("operatingCarrier", {}) or {}

            segments.append(FlightSegment(
                airline=carrier.get("code", ""),
                airline_name=carrier.get("name", ""),
                flight_no=f"{carrier.get('code', '')}{seg.get('code', '')}",
                origin=source.get("station", {}).get("code", ""),
                destination=dest.get("station", {}).get("code", ""),
                origin_city=source.get("station", {}).get("city", {}).get("name", ""),
                destination_city=dest.get("station", {}).get("city", {}).get("name", ""),
                departure=dep_dt,
                arrival=arr_dt,
                duration_seconds=int(seg.get("duration", 0)),
                cabin_class=seg.get("cabinClass", "ECONOMY"),
            ))

        total_dur = int(sector.get("duration", 0))
        if not total_dur and segments:
            total_dur = int((segments[-1].arrival - segments[0].departure).total_seconds())

        return FlightRoute(
            segments=segments,
            total_duration_seconds=max(total_dur, 0),
            stopovers=max(len(segments) - 1, 0),
        )

    def _parse_dt(self, s: str) -> datetime:
        if not s:
            return datetime(2000, 1, 1)
        try:
            return datetime.fromisoformat(s.replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            try:
                return datetime.strptime(s[:19], "%Y-%m-%dT%H:%M:%S")
            except Exception:
                return datetime(2000, 1, 1)

    def _empty(self, req: FlightSearchRequest) -> FlightSearchResponse:
        search_hash = hashlib.md5(
            f"kiwiscrape{req.origin}{req.destination}{req.date_from}".encode()
        ).hexdigest()[:12]
        return FlightSearchResponse(
            search_id=f"fs_{search_hash}",
            origin=req.origin,
            destination=req.destination,
            currency=req.currency,
            offers=[],
            total_results=0,
        )


    @staticmethod
    def _combine_rt(
        ob: list[FlightOffer], ib: list[FlightOffer], req,
    ) -> list[FlightOffer]:
        combos: list[FlightOffer] = []
        for o in ob[:15]:
            for i in ib[:10]:
                price = round(o.price + i.price, 2)
                cid = hashlib.md5(f"{o.id}_{i.id}".encode()).hexdigest()[:12]
                combos.append(FlightOffer(
                    id=f"rt_kiwi_{cid}", price=price, currency=o.currency,
                    outbound=o.outbound, inbound=i.outbound,
                    airlines=list(dict.fromkeys(o.airlines + i.airlines)),
                    owner_airline=o.owner_airline,
                    booking_url=o.booking_url, is_locked=False,
                    source=o.source, source_tier=o.source_tier,
                ))
        combos.sort(key=lambda c: c.price)
        return combos[:20]
