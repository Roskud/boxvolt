# BoxVolt Self-Hosting Guide

This guide describes how to reproduce the current BoxVolt production model:

- one central BoxVolt application host
- one primary DE-style VPN node
- one reserve RU-style VPN node
- one personal subscription URL controlled by BoxVolt

It does not assume `3DP-MANAGER`.

## 1. Target Topology

Minimum reproducible layout:

1. App host with `bot.py`, SQLite, `nginx`, public domains, payment webhooks and subscription feed.
2. Primary node with `3x-ui`, Xray inbounds and `Hysteria2`.
3. Optional reserve node with the same transport family for RU or another second region.

In the current production shape, the app host and primary node can be the same server.

## 2. Prerequisites

Prepare before clone:

- Ubuntu or Debian server(s)
- public DNS records
- Telegram bot token
- active payment merchant credentials
- TLS certificates for public domains
- root or equivalent administrative access

Recommended domains:

- `web.<domain>` for the site and Mini App
- `connect.<domain>` for `/sub`, status and subscription profile pages
- `hook.<domain>` for hidden operational routing
- `<region>.<domain>` such as `ru.<domain>` for region-local fake-site masking

## 3. Clone and Python Runtime

```bash
git clone git@github.com:Roskud/boxvolt.git
cd boxvolt
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## 4. Base Application Config

Create runtime config:

```bash
cp .env.example .env
```

Fill the mandatory groups first.

### Telegram

- `BOT_TOKEN`
- `SUPPORT_CONTACT`
- `ADMIN_TELEGRAM_IDS`

### Public URLs

- `PUBLIC_BASE_URL`
- `PUBLIC_STATUS_URL`
- `WEBAPP_PUBLIC_URL`
- `SUBSCRIPTION_PATH`
- `SUBSCRIPTION_SECRET`

### Primary `3x-ui`

- `XUI_URL`
- `XUI_USERNAME`
- `XUI_PASSWORD`
- `INBOUND_ID`

### Primary Reality TCP

- `SERVER_IP`
- `SERVER_PORT`
- `SERVER_COUNTRY`
- `PUBLIC_KEY`
- `SHORT_ID`
- `SNI`
- `UTLS_FP`

### Payments

- `PLATEGA_ENABLED`
- `PLATEGA_MERCHANT_ID`
- `PLATEGA_SECRET`
- `PLATEGA_WEBHOOK_PATH`

## 5. Primary Node Requirements

On the primary node, create the transport set BoxVolt expects.

Required inbounds:

1. `VLESS Reality TCP` on `443/tcp`
2. `VLESS XHTTP` on `2053/tcp`
3. `VLESS gRPC Reality` on a dedicated TCP port
4. `VMess TCP` on a dedicated TCP port
5. `Trojan Reality TCP` on a dedicated TCP port
6. `Hysteria2` on `443/udp` or a fixed UDP fallback port

Recommended masking setup for the main Reality inbound:

- `dest` should point to local `127.0.0.1:8443`
- `serverNames` should include your neutral fake-site hostname
- the fake-site on `8443` should answer with a normal web page

Recommended masking setup for `Hysteria2`:

- use `masquerade.type: proxy`
- point it to a neutral HTTPS page such as `https://connect.<domain>`
- use `auth.type: http` and let BoxVolt validate the subscription token

## 6. App Host Nginx Layout

The current project expects nginx to separate public surfaces:

- `web.<domain>` for the site and Mini App
- `connect.<domain>` for `/sub`, profile page and `/status`
- `hook.<domain>` for hidden panel routing

Recommended internal app port:

- BoxVolt backend on `127.0.0.1:8080`

Recommended fake-site TLS port for node-local masking:

- `8443`

## 7. Start BoxVolt

Development launch:

```bash
source .venv/bin/activate
python3 bot.py
```

Recommended systemd service:

```ini
[Unit]
Description=BoxVolt VPN Bot
After=network.target

[Service]
WorkingDirectory=/root/boxvolt
ExecStart=/root/boxvolt/.venv/bin/python /root/boxvolt/bot.py
Restart=always
User=root

[Install]
WantedBy=multi-user.target
```

## 8. Enable Multi-Protocol Profiles

After the primary node works, enable the extra profile groups in `.env`:

- `MULTIPROTOCOL_SUBSCRIPTION_ENABLED=1`
- `GRPC_PROFILE_*`
- `VMESS_PROFILE_*`
- `TROJAN_PROFILE_*`
- `HY2_*`

Then restart BoxVolt.

## 9. Add the Reserve Node

The current codebase models one reserve node. To reproduce the existing production setup, fill:

- `ROUTE_RESERVE_*`
- `YOUTUBE_PROFILE_*` for RU XHTTP
- `RU_XUI_*`
- `RU_GRPC_PROFILE_*`
- `RU_VMESS_PROFILE_*`
- `RU_TROJAN_PROFILE_*`
- `RU_HY2_*`

Use the step-by-step region onboarding guide:

- [docs/SERVER_ONBOARDING.md](SERVER_ONBOARDING.md)

## 10. Optional Components

Optional but supported:

- browser extensions via `/edge/api/*`
- MTProto via `MTPROTO_*`
- site e-mail auth
- mail bridge
- admin Mini App controls

## 11. Smoke Checklist

Run after first boot and after every infrastructure change.

### Backend

- `GET /health` returns `200`
- `GET /status` returns `200`
- `GET /webapp` renders
- payment webhook path is reachable

### Subscription

- active `/sub/<token>` opens
- inactive token returns `403`
- profile page `/sub/<token>/profile` renders copy items and app links

### VPN

- supported clients can import the subscription
- DE and reserve node profiles appear in the intended order
- `Hysteria2` auth only works for active subscriptions

### Services

- `systemctl status boxvolt-bot nginx x-ui hysteria-server`
- if MTProto is enabled: `systemctl status boxvolt-mtg@*` or `boxvolt-mtproxy@*`

## 12. What This Guide Does Not Cover

This guide documents the current BoxVolt architecture, not every possible topology.

Not covered as a generic plug-and-play feature:

- arbitrary third and fourth regions
- `3DP-MANAGER` as a second source of truth
- AmneziaWG deployment
- Docker-first replatforming

If you extend the project beyond primary plus reserve node, update the code and then update the docs in the same change.
