"""
Base class for bookable airline connectors.

Connectors that support automated checkout inherit from BookableConnector
and implement `start_checkout()`.  This base class handles:

  - Unlock token verification via the BoostedTravel backend ($1 paywall)
  - Common Playwright helpers (click, fill, screenshot, overlay dismissal)
  - Progress tracking through checkout steps
  - Dry-run safety (never submits payment)

The unlock token MUST be obtained from the backend before checkout begins.
This ensures the $1 fee is paid and prevents open-source workarounds:
the signing key lives only on the closed-source backend.

Flow:
  1. Agent searches (free) → gets offers with booking_url
  2. Agent calls bt.unlock(offer_id) → $1 charged → gets unlock token
  3. Agent calls bt.start_checkout(offer_id, passengers, unlock_token)
     → connector drives the airline checkout up to the payment page
  4. Returns CheckoutProgress with status, screenshot, and booking_url
     for manual completion
"""

from __future__ import annotations

import json
import logging
import os
import random
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Optional
from urllib.request import Request, urlopen
from urllib.error import HTTPError

logger = logging.getLogger(__name__)

# ── Checkout progress model ──────────────────────────────────────────────

CHECKOUT_STEPS = [
    "started",
    "page_loaded",
    "flights_selected",
    "fare_selected",
    "login_bypassed",
    "passengers_filled",
    "extras_skipped",
    "seats_skipped",
    "payment_page_reached",   # This is the FINAL safe step — never go past this
]


@dataclass
class CheckoutProgress:
    """Progress report from a connector checkout attempt."""
    status: str = "not_started"        # started, in_progress, payment_page_reached, failed, error
    step: str = "started"              # Current step from CHECKOUT_STEPS
    step_index: int = 0                # Numeric step (0-8) for progress tracking
    airline: str = ""                  # Airline name (e.g., "Ryanair")
    source: str = ""                   # Source tag (e.g., "ryanair_direct")
    offer_id: str = ""
    total_price: float = 0.0           # Price shown on the checkout page
    currency: str = "EUR"
    booking_url: str = ""              # Direct URL for manual completion
    screenshot_b64: str = ""           # Base64-encoded screenshot of current state
    message: str = ""                  # Human-readable status message
    can_complete_manually: bool = True  # Whether user can finish in browser
    elapsed_seconds: float = 0.0
    details: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "status": self.status,
            "step": self.step,
            "step_index": self.step_index,
            "airline": self.airline,
            "source": self.source,
            "offer_id": self.offer_id,
            "total_price": self.total_price,
            "currency": self.currency,
            "booking_url": self.booking_url,
            "screenshot_b64": self.screenshot_b64,
            "message": self.message,
            "can_complete_manually": self.can_complete_manually,
            "elapsed_seconds": round(self.elapsed_seconds, 1),
            "details": self.details,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "CheckoutProgress":
        return cls(
            status=d.get("status", "not_started"),
            step=d.get("step", "started"),
            step_index=d.get("step_index", 0),
            airline=d.get("airline", ""),
            source=d.get("source", ""),
            offer_id=d.get("offer_id", ""),
            total_price=d.get("total_price", 0.0),
            currency=d.get("currency", "EUR"),
            booking_url=d.get("booking_url", ""),
            screenshot_b64=d.get("screenshot_b64", ""),
            message=d.get("message", ""),
            can_complete_manually=d.get("can_complete_manually", True),
            elapsed_seconds=d.get("elapsed_seconds", 0.0),
            details=d.get("details", {}),
        )


# ── Fake passenger data for safe testing ─────────────────────────────────

FAKE_PASSENGER = {
    "given_name": "Test",
    "family_name": "Traveler",
    "born_on": "1990-06-15",
    "gender": "m",
    "title": "mr",
    "email": "test@example.com",
    "phone_number": "+441234567890",
}


# ── Unlock token verification ────────────────────────────────────────────

_DEFAULT_API_URL = "https://api.boostedchat.com"


def verify_checkout_token(
    offer_id: str,
    token: str,
    api_key: str,
    base_url: str | None = None,
) -> dict:
    """
    Verify a checkout unlock token with the BoostedTravel backend.

    The backend checks:
      - Token signature is valid (HMAC-SHA256, server-side secret)
      - Token belongs to this offer_id
      - Token hasn't expired (30 min window from unlock)
      - $1 fee was successfully charged

    Returns: {"valid": True, "offer_id": "...", "expires_at": "..."}
    Raises on failure.
    """
    url = (base_url or os.environ.get("BOOSTEDTRAVEL_BASE_URL", _DEFAULT_API_URL)).rstrip("/")
    body = json.dumps({"offer_id": offer_id, "token": token}).encode()
    req = Request(
        f"{url}/api/v1/bookings/verify-checkout",
        data=body,
        headers={
            "Content-Type": "application/json",
            "X-API-Key": api_key,
        },
        method="POST",
    )
    try:
        with urlopen(req, timeout=10) as resp:
            return json.loads(resp.read().decode())
    except HTTPError as e:
        body_text = e.read().decode() if e.fp else ""
        raise RuntimeError(
            f"Checkout token verification failed ({e.code}): {body_text}"
        ) from e


# ── Playwright helpers ───────────────────────────────────────────────────

async def dismiss_overlays(page) -> None:
    """Dismiss cookie banners & modals common on airline sites."""
    for selector in [
        "button:has-text('Accept')",
        "button:has-text('Accept All')",
        "button:has-text('Accept all cookies')",
        "button:has-text('Agree')",
        "button:has-text('Yes, I agree')",
        "button:has-text('Got it')",
        "button:has-text('OK')",
        "[class*='cookie'] button",
        "[id*='cookie'] button",
        "button[data-ref='cookie.accept-all']",
    ]:
        try:
            btn = page.locator(selector).first
            if await btn.is_visible(timeout=600):
                await btn.click()
                await page.wait_for_timeout(300)
        except Exception:
            continue


async def safe_click(page, selector: str, timeout: int = 10000, desc: str = "") -> bool:
    """Click an element safely — returns True if clicked, False if not found."""
    try:
        el = page.locator(selector).first
        await el.wait_for(state="visible", timeout=timeout)
        await el.scroll_into_view_if_needed()
        await page.wait_for_timeout(random.randint(150, 400))
        await el.click()
        logger.debug("Clicked %s (%s)", selector, desc)
        return True
    except Exception as e:
        logger.debug("Could not click %s (%s): %s", selector, desc, e)
        return False


async def safe_fill(page, selector: str, value: str, timeout: int = 8000) -> bool:
    """Fill an input safely — returns True if filled."""
    try:
        el = page.locator(selector).first
        await el.wait_for(state="visible", timeout=timeout)
        await el.scroll_into_view_if_needed()
        await page.wait_for_timeout(random.randint(100, 250))
        await el.click()
        await el.fill(value)
        return True
    except Exception as e:
        logger.debug("Could not fill %s: %s", selector, e)
        return False


async def take_screenshot_b64(page) -> str:
    """Take a screenshot and return it as base64."""
    import base64
    try:
        raw = await page.screenshot(full_page=False)
        return base64.b64encode(raw).decode("ascii")
    except Exception:
        return ""


# ── Base class ───────────────────────────────────────────────────────────

class BookableConnector:
    """
    Base class for airline connectors that support automated checkout.

    Subclasses implement `_run_checkout()` which drives the airline's
    checkout flow up to (but NOT including) payment submission.

    The base class handles:
    - Token verification with the backend
    - Error handling and progress tracking
    - Browser lifecycle
    """

    # Override in subclass
    AIRLINE_NAME: str = ""
    SOURCE_TAG: str = ""

    async def start_checkout(
        self,
        offer: dict,
        passengers: list[dict],
        checkout_token: str,
        api_key: str,
        *,
        base_url: str | None = None,
    ) -> CheckoutProgress:
        """
        Drive checkout up to (not including) payment submission.

        Args:
            offer: The FlightOffer dict (must include id, booking_url, price, currency, outbound).
            passengers: Passenger details (use FAKE_PASSENGER for testing).
            checkout_token: Token from bt.unlock() — proves $1 was paid.
            api_key: BoostedTravel API key for token verification.
            base_url: Override API base URL.

        Returns:
            CheckoutProgress with status, screenshot, and booking_url.
        """
        t0 = time.monotonic()
        offer_id = offer.get("id", "")
        booking_url = offer.get("booking_url", "")

        # Stash credentials for subclass use (needed by generic engine)
        self._last_token = checkout_token
        self._last_api_key = api_key
        self._last_base_url = base_url

        # Verify the checkout token with the backend
        try:
            verification = verify_checkout_token(
                offer_id=offer_id,
                token=checkout_token,
                api_key=api_key,
                base_url=base_url,
            )
            if not verification.get("valid"):
                return CheckoutProgress(
                    status="failed",
                    airline=self.AIRLINE_NAME,
                    source=self.SOURCE_TAG,
                    offer_id=offer_id,
                    message="Checkout token is invalid or expired. Call unlock() first ($1 fee).",
                    booking_url=booking_url,
                )
        except Exception as e:
            return CheckoutProgress(
                status="failed",
                airline=self.AIRLINE_NAME,
                source=self.SOURCE_TAG,
                offer_id=offer_id,
                message=f"Token verification failed: {e}",
                booking_url=booking_url,
            )

        # Run the airline-specific checkout
        try:
            progress = await self._run_checkout(offer, passengers)
            progress.elapsed_seconds = time.monotonic() - t0
            progress.offer_id = offer_id
            return progress
        except Exception as e:
            logger.error("%s checkout error: %s", self.AIRLINE_NAME, e, exc_info=True)
            return CheckoutProgress(
                status="error",
                airline=self.AIRLINE_NAME,
                source=self.SOURCE_TAG,
                offer_id=offer_id,
                message=f"Checkout error: {e}",
                booking_url=booking_url,
                elapsed_seconds=time.monotonic() - t0,
            )

    async def _run_checkout(
        self,
        offer: dict,
        passengers: list[dict],
    ) -> CheckoutProgress:
        """
        Airline-specific checkout logic — override in subclass.

        MUST stop before payment submission. Return a CheckoutProgress
        with step="payment_page_reached" on success.
        """
        raise NotImplementedError(f"{self.AIRLINE_NAME} checkout not implemented")
