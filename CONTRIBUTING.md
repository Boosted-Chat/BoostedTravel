# Contributing to LFG (formerly BoostedTravel)

Thanks for your interest in contributing! 🚀

## Quick Links

- **GitHub:** https://github.com/Boosted-Chat/LetsFG
- **API Docs:** https://api.letsfg.co/docs
- **npm (JS SDK):** https://www.npmjs.com/package/boostedtravel
- **npm (MCP):** https://www.npmjs.com/package/boostedtravel-mcp
- **PyPI:** https://pypi.org/project/boostedtravel/

## How to Contribute

1. **Bugs & small fixes** → Open a PR directly
2. **New features / architecture changes** → Open a [GitHub Issue](https://github.com/Boosted-Chat/LetsFG/issues) first to discuss
3. **Questions** → Open a [GitHub Discussion](https://github.com/Boosted-Chat/LetsFG/discussions)

## Before You PR

- Test locally with your own API key (run `boostedtravel register` — see the [README](README.md#cli) or [API docs](https://api.letsfg.co/docs))
- Run the relevant SDK tests (see below)
- Keep PRs focused — one thing per PR
- Describe **what** you changed and **why**

## Development Setup

### Python SDK

```bash
cd sdk/python
pip install -e ".[dev]"
python -m pytest
```

### JS/TS SDK

```bash
cd sdk/js
npm install
npm run build
npm test
```

### MCP Server

```bash
cd sdk/mcp
npm install
npm run build
```

## Repository Structure

```
sdk/
├── python/    # Python SDK (PyPI: boostedtravel)
├── js/        # JavaScript/TypeScript SDK (npm: boostedtravel)
└── mcp/       # MCP Server (npm: boostedtravel-mcp)
```

The backend API is in a separate private repository. This repo contains the public SDKs, MCP server, and documentation only.

## Code Style

### Python
- Type hints everywhere
- `httpx` for HTTP requests
- `pydantic` for data models
- Follow existing patterns in `client.py`

### TypeScript
- Strict mode enabled
- Native `fetch` (no axios/got)
- Export types from `types.ts`
- Rebuild dist after changes: `npm run build`

## Commit Messages

Use concise, action-oriented messages:

```
fix: handle timeout in Python search client
feat: add returnUrl option to JS unlock method
docs: update MCP server README with new tool descriptions
```

## AI-Assisted PRs Welcome! 🤖

Built with Copilot, Claude, Cursor, or other AI tools? Great — just note it in your PR description so I know what to look for when reviewing.

## Important: Keep Messaging Consistent

When editing any agent-facing text (READMEs, SDK docstrings, MCP tool descriptions), please maintain:

1. **Zero price bias** messaging — this is a core differentiator
2. **Real passenger details** warning — critical for bookings
3. **Pricing accuracy** — everything is free (star GitHub repo for access)

## Report a Vulnerability

See [SECURITY.md](SECURITY.md) for our security policy and how to report vulnerabilities.

## License

MIT — see [LICENSE](LICENSE).
