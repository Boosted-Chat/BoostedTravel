# LetsFG MCP Server
# For use with Glama.ai and other containerized MCP deployments

FROM node:22-slim

# Install dependencies for Playwright (needed for local connectors)
RUN apt-get update && apt-get install -y --no-install-recommends \
    wget \
    ca-certificates \
    fonts-liberation \
    libasound2t64 \
    libatk-bridge2.0-0 \
    libatk1.0-0 \
    libcups2 \
    libdbus-1-3 \
    libdrm2 \
    libgbm1 \
    libgtk-3-0 \
    libnspr4 \
    libnss3 \
    libxcomposite1 \
    libxdamage1 \
    libxfixes3 \
    libxrandr2 \
    xdg-utils \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install letsfg-mcp from npm
RUN npm install -g letsfg-mcp@latest

# Install Playwright browsers
RUN npx playwright install chromium

# Environment variables (optional - search works without API key)
ENV LETSFG_API_KEY=""

# MCP server runs on stdio
ENTRYPOINT ["letsfg-mcp"]
