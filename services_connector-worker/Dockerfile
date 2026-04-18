# LetsFG Connector Worker — Cloud Run
# Runs ONE flight connector per request (concurrency=1).
# Requires Chrome/Playwright + Xvfb for headed browser connectors.

FROM python:3.13-slim-bookworm

# Install system deps for Chrome + Xvfb
RUN apt-get update && apt-get install -y --no-install-recommends \
    xvfb \
    wget \
    gnupg \
    ca-certificates \
    fonts-liberation \
    libasound2 \
    libatk-bridge2.0-0 \
    libatk1.0-0 \
    libcups2 \
    libdbus-1-3 \
    libdrm2 \
    libgbm1 \
    libgtk-3-0 \
    libnspr4 \
    libnss3 \
    libpango-1.0-0 \
    libxcomposite1 \
    libxdamage1 \
    libxfixes3 \
    libxrandr2 \
    libxshmfence1 \
    xdg-utils \
    libu2f-udev \
    libvulkan1 \
    psmisc \
    && rm -rf /var/lib/apt/lists/*

# Install Google Chrome stable (found by find_chrome at /usr/bin/google-chrome-stable)
RUN wget -q -O /tmp/chrome.deb https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb \
    && apt-get update && apt-get install -y --no-install-recommends /tmp/chrome.deb \
    && rm /tmp/chrome.deb && rm -rf /var/lib/apt/lists/*

# Chrome wrapper that adds --no-sandbox (required when running as root in containers)
RUN mv /usr/bin/google-chrome-stable /usr/bin/google-chrome-stable-real \
    && printf '#!/bin/bash\nexec /usr/bin/google-chrome-stable-real --no-sandbox "$@"\n' > /usr/bin/google-chrome-stable \
    && chmod +x /usr/bin/google-chrome-stable

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt && pip show letsfg | grep Version

# Overlay updated connector files (native round-trip URL support)
# These override the PyPI-installed versions with latest changes.
COPY connector_patches/*.py /usr/local/lib/python3.13/site-packages/letsfg/connectors/

# Install Playwright deps (for connectors using pw.chromium.launch)
RUN playwright install --with-deps chromium

# Install patchright browsers (for Wego connector)
RUN python -m patchright install chromium || true

COPY . .

ENV PORT=8080
ENV DISPLAY=:99
EXPOSE 8080

# Start Xvfb + gunicorn
CMD ["bash", "-c", "Xvfb :99 -screen 0 1920x1080x24 -nolisten tcp & sleep 0.5 && exec gunicorn --bind :$PORT --workers 1 --threads 1 --timeout 0 wsgi:app"]
