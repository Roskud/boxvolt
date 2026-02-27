# Edge Extension: Safe Production Setup

## Goal
- Enable Edge extension with BR/RU proxy servers.
- Keep existing VPN sales/infra untouched (`x-ui`, `VLESS`, `nginx`, `443`).

## Safety Rules
- Do not change `INBOUND_ID`, `x-ui` settings, or existing VLESS inbounds.
- Do not reuse `443` for SOCKS5.
- Run SOCKS5 on dedicated new ports (example: `2081` for BR, `2082` for RU).

## 1) Deploy SOCKS5 on BR VPS

On BR server (root):

```bash
cd /root/boxvolt
LISTEN_PORT=2081 \
EDGE_LOGIN_USER=edge_br \
EDGE_LOGIN_PASS='CHANGE_ME_STRONG_BR_PASS' \
ALLOW_CIDR=0.0.0.0/0 \
scripts/install_edge_socks5.sh
```

## 2) Deploy SOCKS5 on RU VPS

On RU server (root):

```bash
cd /root/boxvolt
LISTEN_PORT=2082 \
EDGE_LOGIN_USER=edge_ru \
EDGE_LOGIN_PASS='CHANGE_ME_STRONG_RU_PASS' \
ALLOW_CIDR=0.0.0.0/0 \
scripts/install_edge_socks5.sh
```

## 3) Verify both proxies

From any Linux host with network access:

```bash
cd /root/boxvolt
scripts/check_edge_socks5.sh BR_HOST_OR_IP 2081 edge_br 'CHANGE_ME_STRONG_BR_PASS'
scripts/check_edge_socks5.sh RU_HOST_OR_IP 2082 edge_ru 'CHANGE_ME_STRONG_RU_PASS'
```

If both checks pass, proceed.

## 4) Fill bot `.env` for Edge

```env
EDGE_EXTENSION_ENABLED=1
EDGE_AUTH_PREFIX=edgeauth
EDGE_AUTH_REQUEST_TTL_SECONDS=600
EDGE_SESSION_TTL_SECONDS=2592000
EDGE_MAX_ACTIVE_SESSIONS_PER_USER=5

EDGE_SERVER_BR_LABEL=Brazil
EDGE_SERVER_BR_HOST=BR_HOST_OR_IP
EDGE_SERVER_BR_PORT=2081
EDGE_SERVER_BR_SCHEME=socks5
EDGE_SERVER_BR_USERNAME=edge_br
EDGE_SERVER_BR_PASSWORD=CHANGE_ME_STRONG_BR_PASS

EDGE_SERVER_RU_LABEL=Russia
EDGE_SERVER_RU_HOST=RU_HOST_OR_IP
EDGE_SERVER_RU_PORT=2082
EDGE_SERVER_RU_SCHEME=socks5
EDGE_SERVER_RU_USERNAME=edge_ru
EDGE_SERVER_RU_PASSWORD=CHANGE_ME_STRONG_RU_PASS
```

Restart bot:

```bash
cd /root/boxvolt
python3 bot.py
```

## 5) Disable demo mode in extension

File: `browser-extension/config.js`

Set:

```js
demoMode: false,
demoBypassSubscription: false,
demoServers: []
```

And keep:

```js
apiBaseUrl: "https://connect.boxvolt.shop"
```

## 6) Final smoke test
- Login in extension via Telegram.
- Test account with active subscription:
  - BR connect -> open websites
  - RU connect -> open websites
- Test account without subscription:
  - extension should login but deny server connect.

## Rollback (safe)
- In extension set `demoMode: true` and reload extension.
- Or set `EDGE_EXTENSION_ENABLED=0` in bot `.env` and restart bot.
- Existing VPN (VLESS via x-ui) remains untouched.
