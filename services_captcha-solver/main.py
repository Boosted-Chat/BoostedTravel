"""
LetsFG Captcha Solver — Standalone Cloud Run microservice.

Proxies captcha screenshots to Gemini via Vertex AI so local airline
connectors can solve captchas without users needing their own LLM key.
Uses Application Default Credentials — no API keys needed.

Deploy:
  gcloud run deploy captcha-solver \
    --source=. --project=sms-caller --region=us-central1 \
    --memory=256Mi --cpu=1 --concurrency=80 --max-instances=3 \
    --min-instances=0 --cpu-throttling \
    --timeout=60 --allow-unauthenticated \
    --update-env-vars LETSFG_API_URL=https://api.letsfg.co,GCP_PROJECT=sms-caller,VERTEX_REGION=us-central1

Environment variables:
  GCP_PROJECT       — GCP project ID (default: sms-caller)
  VERTEX_REGION     — Vertex AI region (default: us-central1)
  LETSFG_API_URL    — Main API URL for key validation (default: https://api.letsfg.co)

Security:
  - Validates LETSFG_API_KEY via main API (/api/v1/agents/me)
  - Caches valid keys for 5 min (avoids per-request overhead)
  - Rate limited: 10 req/min per agent (in-memory sliding window)
  - Image payload: max 500KB base64 PNG, magic-byte validated
  - Instruction text: sanitized regex whitelist, 200 char cap
  - No external API keys — Vertex AI auth via IAM/ADC
  - No PII stored — screenshots processed and immediately discarded
"""

import asyncio
import base64
import logging
import os
import re
import time
from collections import defaultdict

import httpx
import uvicorn
from fastapi import FastAPI, HTTPException, Request, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, field_validator
from google.auth import default as google_auth_default
from google.auth.transport.requests import Request as GoogleAuthRequest

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
)
logger = logging.getLogger("captcha-solver")

GCP_PROJECT = os.environ.get("GCP_PROJECT", "sms-caller")
VERTEX_REGION = os.environ.get("VERTEX_REGION", "us-central1")
LETSFG_API_URL = os.environ.get("LETSFG_API_URL", "https://api.letsfg.co")
VERTEX_MODEL = "gemini-2.5-flash-lite"

# ── Vertex AI auth (cached credentials) ──────────────────────────────────────
_credentials = None


def _get_vertex_token() -> str:
    """Get a valid access token for Vertex AI using ADC."""
    global _credentials
    if _credentials is None:
        creds, _ = google_auth_default(
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        _credentials = creds
    if not _credentials.valid:
        _credentials.refresh(GoogleAuthRequest())
    return _credentials.token


# ── API key validation cache ─────────────────────────────────────────────────
_KEY_CACHE: dict[str, tuple[str, float]] = {}
_KEY_CACHE_TTL = 300.0  # 5 minutes


async def validate_api_key(api_key: str) -> str:
    """
    Validate a LETSFG_API_KEY by calling the main API.
    Returns agent_id on success, raises HTTPException on failure.
    Caches valid keys for 5 minutes.
    """
    if not api_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing X-API-Key header.",
        )

    # Check cache
    now = time.time()
    cached = _KEY_CACHE.get(api_key)
    if cached and cached[1] > now:
        return cached[0]

    # Validate against main API
    try:
        async with httpx.AsyncClient(timeout=10.0, follow_redirects=True) as client:
            resp = await client.get(
                f"{LETSFG_API_URL}/api/v1/agents/me",
                headers={"X-API-Key": api_key},
            )
    except Exception as e:
        logger.error("Failed to validate API key against %s: %s %s", LETSFG_API_URL, type(e).__name__, e)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Unable to validate API key. Try again.",
        )

    if resp.status_code == 401:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API key.",
        )
    if resp.status_code != 200:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Unable to validate API key. Try again.",
        )

    agent_id = resp.json().get("agent_id", "unknown")

    _KEY_CACHE[api_key] = (agent_id, now + _KEY_CACHE_TTL)
    return agent_id


# ── Rate limiting ────────────────────────────────────────────────────────────
_RATE_LIMIT = 10  # requests per minute per agent
_RATE_WINDOW = 60  # seconds
_rate_hits: dict[str, list[float]] = defaultdict(list)


def check_rate_limit(agent_id: str) -> bool:
    """Returns True if allowed, False if rate limited."""
    now = time.monotonic()
    hits = _rate_hits[agent_id]
    cutoff = now - _RATE_WINDOW
    _rate_hits[agent_id] = [t for t in hits if t > cutoff]

    if len(_rate_hits[agent_id]) >= _RATE_LIMIT:
        return False

    _rate_hits[agent_id].append(now)
    return True


# ── Request / Response models ────────────────────────────────────────────────
_MAX_IMAGE_B64_LENGTH = 500_000
_MAX_INSTRUCTION_LENGTH = 200


def _detect_mime(b64_data: str) -> str:
    """Detect MIME type from base64 image data."""
    try:
        raw = base64.b64decode(b64_data[:16], validate=True)
        if raw[:3] == b"\xff\xd8\xff":
            return "image/jpeg"
    except Exception:
        pass
    return "image/png"


def _validate_image_b64(v: str, field_name: str = "image") -> str:
    if len(v) > _MAX_IMAGE_B64_LENGTH:
        raise ValueError(f"{field_name} too large")
    try:
        raw = base64.b64decode(v, validate=True)
    except Exception:
        raise ValueError(f"invalid base64 in {field_name}")
    # Accept PNG (\x89PNG) or JPEG (\xFF\xD8\xFF)
    if not (raw[:4] == b"\x89PNG" or raw[:3] == b"\xff\xd8\xff"):
        raise ValueError(f"{field_name} must be PNG or JPEG")
    return v


class CaptchaSolveRequest(BaseModel):
    image_b64: str = Field(
        ...,
        description="Base64-encoded PNG/JPEG of the captcha area.",
        max_length=_MAX_IMAGE_B64_LENGTH,
    )
    reference_b64: str | None = Field(
        default=None,
        description="Base64-encoded PNG/JPEG of the reference/target image (e.g. which object to click).",
        max_length=_MAX_IMAGE_B64_LENGTH,
    )
    instruction: str = Field(
        default="请点击图中的目标物体",
        description="Captcha instruction text.",
        max_length=_MAX_INSTRUCTION_LENGTH,
    )

    @field_validator("image_b64")
    @classmethod
    def validate_image(cls, v: str) -> str:
        return _validate_image_b64(v, "image_b64")

    @field_validator("reference_b64")
    @classmethod
    def validate_reference(cls, v: str | None) -> str | None:
        if v is None:
            return v
        return _validate_image_b64(v, "reference_b64")

    @field_validator("instruction")
    @classmethod
    def sanitize_instruction(cls, v: str) -> str:
        sanitized = re.sub(
            r"[^\u4e00-\u9fff\u3000-\u303fa-zA-Z0-9\s.,!?()（）《》、。，！？\-]",
            "",
            v,
        )
        sanitized = re.sub(r"\s+", " ", sanitized).strip()
        return sanitized[:_MAX_INSTRUCTION_LENGTH] if sanitized else "请点击图中的目标物体"


class CaptchaSolveResponse(BaseModel):
    x: float = Field(..., ge=0.0, le=1.0)
    y: float = Field(..., ge=0.0, le=1.0)


# ── FastAPI app ──────────────────────────────────────────────────────────────
app = FastAPI(
    title="LetsFG Captcha Solver",
    description="Internal microservice — solves visual captchas for local airline connectors.",
    version="1.0.0",
    docs_url=None,
    redoc_url=None,
)


@app.get("/health")
async def health():
    return {"status": "ok", "service": "captcha-solver"}


@app.post("/api/v1/captcha/solve", response_model=CaptchaSolveResponse)
async def solve_captcha(req: CaptchaSolveRequest, request: Request):
    """Solve a visual captcha. Requires X-API-Key header."""

    # ── Auth ──
    api_key = request.headers.get("X-API-Key", "")
    agent_id = await validate_api_key(api_key)

    # ── Rate limit ──
    if not check_rate_limit(agent_id):
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Rate limit exceeded (10 req/min). Wait and retry.",
            headers={"Retry-After": "60"},
        )

    # ── Get Vertex AI token ──
    try:
        token = _get_vertex_token()
    except Exception as e:
        logger.error("Failed to get Vertex AI credentials: %s", e)
        raise HTTPException(status_code=503, detail="Captcha solving temporarily unavailable.")

    # ── Build prompt ──
    has_ref = req.reference_b64 is not None
    if has_ref:
        prompt_text = (
            f"Visual click captcha. Two images provided:\n"
            f"IMAGE 1: Main scene with several small objects floating in it.\n"
            f"IMAGE 2: Small reference icon showing WHICH object to click in IMAGE 1.\n\n"
            f'Instruction: "{req.instruction}"\n\n'
            f"Steps:\n"
            f"1. Look at IMAGE 2 (the small reference icon) and identify what object it shows.\n"
            f"2. Scan IMAGE 1 for ALL objects visible in the scene.\n"
            f"3. Find the ONE object in IMAGE 1 that matches IMAGE 2.\n"
            f"4. Return the CENTER coordinates of that matching object.\n\n"
            f"Return JSON: {{\"x\": 0.XX, \"y\": 0.YY}}\n"
            f"x = horizontal position (0.0=left edge, 1.0=right edge)\n"
            f"y = vertical position (0.0=top edge, 1.0=bottom edge)\n"
            f"Coordinates must be within IMAGE 1. Return ONLY the JSON."
        )
    else:
        prompt_text = (
            f"This is a visual captcha screenshot.\n"
            f'The instruction says: "{req.instruction}"\n\n'
            f"Identify where to click to solve this captcha.\n"
            f'Return ONLY a JSON object: {{"x": 0.XX, "y": 0.YY}}\n'
            f"where x,y are percentages of image width/height from top-left (0.0 to 1.0)."
        )

    # ── Build message parts ──
    parts = [
        {"text": prompt_text},
        {"inlineData": {"mimeType": _detect_mime(req.image_b64), "data": req.image_b64}},
    ]
    if has_ref:
        parts.append({"inlineData": {"mimeType": _detect_mime(req.reference_b64), "data": req.reference_b64}})

    # ── Call Vertex AI ──
    vertex_url = (
        f"https://{VERTEX_REGION}-aiplatform.googleapis.com/v1/projects/{GCP_PROJECT}"
        f"/locations/{VERTEX_REGION}/publishers/google/models/{VERTEX_MODEL}:generateContent"
    )
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(
                vertex_url,
                headers={
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "application/json",
                },
                json={
                    "contents": [{"role": "user", "parts": parts}],
                    "generationConfig": {
                        "temperature": 1,
                        "topP": 0.95,
                        "maxOutputTokens": 1024,
                    },
                    "safetySettings": [
                        {"category": "HARM_CATEGORY_HATE_SPEECH", "threshold": "OFF"},
                        {"category": "HARM_CATEGORY_DANGEROUS_CONTENT", "threshold": "OFF"},
                        {"category": "HARM_CATEGORY_SEXUALLY_EXPLICIT", "threshold": "OFF"},
                        {"category": "HARM_CATEGORY_HARASSMENT", "threshold": "OFF"},
                    ],
                },
            )
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="Captcha solving timed out.")
    except httpx.HTTPError as e:
        logger.warning("Vertex AI error for %s: %s", agent_id, e)
        raise HTTPException(status_code=502, detail="Captcha solving failed.")

    if resp.status_code != 200:
        logger.warning("Vertex AI %s for %s: %s", resp.status_code, agent_id, resp.text[:300])
        raise HTTPException(status_code=502, detail="Captcha solving failed.")

    # ── Parse response ──
    try:
        data = resp.json()
        parts_list = (
            data.get("candidates", [{}])[0]
            .get("content", {})
            .get("parts", [])
        )
        # With thinking enabled, model returns thinking parts + text parts.
        # Find the last text part (skip thinking parts which have "thought": true).
        content = ""
        for part in parts_list:
            if "text" in part and not part.get("thought"):
                content = part["text"]
        # Fallback: if no non-thought text found, use any text part
        if not content:
            for part in parts_list:
                if "text" in part:
                    content = part["text"]
                    break
    except Exception:
        raise HTTPException(status_code=502, detail="Invalid solver response.")

    logger.info("Vertex raw for %s: %s", agent_id, content[:300])

    match = re.search(r'\{[^}]*"x"\s*:\s*([\d.]+)[^}]*"y"\s*:\s*([\d.]+)[^}]*\}', content)
    if not match:
        logger.warning("Unparseable Vertex response for %s: %s", agent_id, content[:200])
        raise HTTPException(status_code=502, detail="Solver returned unparseable response.")

    x = max(0.0, min(1.0, float(match.group(1))))
    y = max(0.0, min(1.0, float(match.group(2))))

    logger.info("Solved for %s: (%.3f, %.3f) [ref=%s]", agent_id, x, y, "yes" if has_ref else "no")
    return CaptchaSolveResponse(x=x, y=y)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    uvicorn.run(app, host="0.0.0.0", port=port)
