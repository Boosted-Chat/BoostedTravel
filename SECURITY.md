# Security Policy

If you believe you've found a security vulnerability in BoostedTravel, please report it responsibly.

## Reporting a Vulnerability

**Email:** security@boostedchat.com

Please include:

1. **Description** of the vulnerability
2. **Steps to reproduce** the issue
3. **Impact assessment** — what can an attacker do?
4. **Affected component** — SDK, API, MCP server, etc.
5. **Your environment** — OS, SDK version, language version

We aim to acknowledge reports within 48 hours and provide a fix or mitigation within 7 days for critical issues.

## Supported Versions

| Package | Version | Supported |
|---------|---------|-----------|
| boostedtravel (Python) | 0.1.x | ✅ |
| boostedtravel (npm) | 0.1.x | ✅ |
| boostedtravel-mcp (npm) | 0.1.x | ✅ |

## Scope

### In Scope

- Authentication/authorization bypasses in the API
- SDK vulnerabilities (injection, credential leakage, etc.)
- MCP server security issues
- Personally identifiable information (PII) exposure
- Payment/billing vulnerabilities

### Out of Scope

- Rate limiting or denial-of-service against the public API
- Social engineering attacks
- Issues in third-party dependencies (report upstream, then let us know)
- Findings from automated scanners without a working proof of concept
- Issues requiring physical access to a user's machine

## API Security Model

- **API keys** authenticate all requests. Keep your key secret.
- **Stripe** handles all payment processing. BoostedTravel never stores card numbers.
- **Passenger data** (names, emails) is passed directly to the airline for booking. We do not store passenger PII beyond the booking transaction.
- **HTTPS only** — all API traffic is encrypted in transit.

## Responsible Disclosure

- Please do **not** publicly disclose vulnerabilities before we've had a chance to fix them.
- We will credit reporters in release notes (unless you prefer to remain anonymous).
- There is no bug bounty program at this time. We appreciate responsible disclosure and will acknowledge your contribution.

## Contact

- **Security issues:** security@boostedchat.com
- **General questions:** Open a [GitHub Issue](https://github.com/Boosted-Chat/BoostedTravel/issues)
