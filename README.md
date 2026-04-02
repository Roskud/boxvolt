# BoxVolt VPN

BoxVolt VPN is a self-hosted Telegram-first VPN service built around one personal subscription URL, payment automation, a web cabinet, a Telegram Mini App, browser extensions and optional MTProto access.

The current production shape of the project is:

- Telegram bot with sales, onboarding, support entrypoints and admin actions
- personal subscription feed at `/sub/<token>`
- site cabinet and Mini App for payments, subscription access and key reissue
- multi-protocol VPN subscription for DE and RU nodes
- browser extension flow with Telegram login and BR/RU proxy delivery
- optional MTProto with health-check and auto-fallback

## What BoxVolt Delivers

- One subscription URL with paired DE/RU profiles.
- Current profile set: `VLESS XHTTP`, `VLESS Reality TCP`, `VLESS gRPC Reality`, `VMess TCP`, `Trojan Reality`, `Hysteria2`.
- Separate text keys for `AmneziaVPN`.
- Payment automation via `Platega`.
- Personal access control via `telegram_id`, `subscription_end`, `vless_uuid` and subscription tokens.
- Site cabinet, QR page, key reissue, order status polling and contact forms.
- Telegram Mini App with admin tooling.
- Browser extension backend for login and proxy delivery.
- MTProto device management with region health-check and fallback.

## Current Topology

BoxVolt is not a generic control panel wrapper. The source of truth remains the BoxVolt application itself.

- `bot.py` runs the Telegram bot and the `aiohttp` backend.
- `3x-ui` stores and serves Xray inbounds on VPN nodes.
- `nginx` exposes public domains and fake-site pages.
- `Hysteria2` is authenticated through BoxVolt HTTP auth, not a shared password.
- Payments, subscription state and token logic live in SQLite and BoxVolt code, not in `3DP-MANAGER`.

Current production pattern:

1. Primary app host with bot, site, Mini App, `/sub`, DE node and payment webhooks.
2. Secondary reserve node with RU inbounds and optional RU fake-site.
3. Shared BoxVolt backend that generates all client configs.

## Repository Map

- `bot.py` — main Telegram bot, backend routes, subscription generation, payment logic, web pages.
- `database.py` — DB helpers and migrations.
- `subscription_protocols.py` — link builders for `vmess://`, `trojan://`, `hy2://`, `vless://`.
- `profile_display.py`, `profile_card.py`, `pricing_display.py` — presentation helpers.
- `mtproto_utils.py`, `scripts/mtproto_manager.py` — MTProto support layer.
- `browser-extension/` — Chromium extension.
- `firefox-extension/` — Firefox extension.
- `frontend/` — modern site frontend source.
- `webapp/` — static legacy web assets and pages.
- `docs/` — operational guides and architecture notes.
- `deploy/` — service unit templates for MTProto components.
- `tests/` — focused unit tests for helper modules.

## Documentation Map

- [Self-hosting guide](docs/SELF_HOSTING_GUIDE.md)
- [Server onboarding guide](docs/SERVER_ONBOARDING.md)
- [Browser extension guide](docs/EXTENSION_GUIDE.md)
- [Safe Edge proxy production setup](docs/EDGE_EXTENSION_PROD_SETUP.md)
- [Security checklist](docs/SECURITY_CHECKLIST.md)
- [Threat model](docs/THREAT_MODEL_2026-03-04.md)

## Quick Start

### 1. Clone and prepare Python

```bash
git clone git@github.com:Roskud/boxvolt.git
cd boxvolt
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Create runtime config

```bash
cp .env.example .env
```

Fill the minimum required groups:

- Telegram: `BOT_TOKEN`
- main node / `3x-ui`: `XUI_URL`, `XUI_USERNAME`, `XUI_PASSWORD`, `INBOUND_ID`
- Reality TCP: `SERVER_IP`, `SERVER_PORT`, `PUBLIC_KEY`, `SHORT_ID`, `SNI`, `UTLS_FP`
- subscription: `SUBSCRIPTION_SECRET`, `SUBSCRIPTION_PATH`, `WEBAPP_PUBLIC_URL`, `PUBLIC_BASE_URL`, `PUBLIC_STATUS_URL`
- payments: `PLATEGA_ENABLED`, `PLATEGA_MERCHANT_ID`, `PLATEGA_SECRET`

Then enable optional groups as needed:

- DE/RU multi-protocol inbounds
- `Hysteria2`
- browser extension
- MTProto
- site e-mail auth
- mail bridge

### 3. Start the app

```bash
source .venv/bin/activate
python3 bot.py
```

### 4. Verify local health

- `GET /health`
- `GET /status`
- `GET /webapp`
- `GET /sub/<token>`

For a real deployment walkthrough, use [docs/SELF_HOSTING_GUIDE.md](docs/SELF_HOSTING_GUIDE.md).

## Production Notes

Recommended public domains:

- `web.<domain>` — main website and Mini App
- `connect.<domain>` — subscription feed, QR/profile page, status page
- `hook.<domain>` — hidden `3x-ui` reverse proxy and operational endpoints
- `<region>.<domain>` — optional node-local fake-site domains such as `ru.boxvolt.shop`

Recommended service split:

- `boxvolt-bot` — Python app
- `nginx` — public reverse proxy and fake-site layer
- `x-ui` — node control panel
- `hysteria-server` — UDP transport
- optional `boxvolt-mtg@` / `boxvolt-mtproxy@` — MTProto services

## Browser Extension

The repository contains two extension targets:

- `browser-extension/` for Chrome, Edge and Opera
- `firefox-extension/` for Firefox

Extension backend routes already live in `bot.py`:

- `POST /edge/api/auth/start`
- `POST /edge/api/auth/poll`
- `GET /edge/api/me`
- `POST /edge/api/logout`

Use [docs/EXTENSION_GUIDE.md](docs/EXTENSION_GUIDE.md) for the full flow, packaging, backend env and smoke checks.

## Adding New Servers

The current codebase supports:

- one primary node
- one reserve RU-style node with its own multi-protocol set

To reproduce the current production shape, follow [docs/SERVER_ONBOARDING.md](docs/SERVER_ONBOARDING.md).

Important limitation:

- adding a third arbitrary region is not just an `.env` change today
- the project currently models one primary node plus one reserve node pattern
- if you need `DE + RU + NL` or similar, extend the code first, then document the new region group

## Testing and Verification

Unit tests:

```bash
pytest tests -q
```

Targeted smoke:

```bash
python scripts/playwright_smoke.py --require-sub
```

Useful operational checks:

- `systemctl status boxvolt-bot nginx x-ui hysteria-server`
- `curl -I https://YOUR_DOMAIN/health`
- `curl -I https://YOUR_DOMAIN/status`
- live import of `/sub/<token>` into a supported client

## Security

- Never commit `.env`, mail credentials or real DB snapshots.
- Keep fake-site pages neutral and do not describe VPN logic on node hostnames.
- Keep `/sub` token handling centralized in BoxVolt.
- Use separate backup copies before changing `x-ui` DBs or nginx configs.
- Review [docs/SECURITY_CHECKLIST.md](docs/SECURITY_CHECKLIST.md) before production changes.

## Supported Clients

Current recommended clients:

- `Happ`
- `V2rayTun`
- `Hiddify`
- `AmneziaVPN` for separate text keys

The exact onboarding text shown to users is maintained inside the application and the instruction file in [`Инструкция/Инструкция.txt`](Инструкция/Инструкция.txt).
