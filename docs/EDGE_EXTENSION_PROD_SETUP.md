# Edge Extension Safe Production Setup

This is the lowest-risk way to launch the BoxVolt browser extension in production.

Goal:

- enable browser proxy access for authorized users
- keep the main VPN stack untouched
- avoid changes to `443`, `x-ui` and Xray VPN inbounds

## Safety Rules

- do not change `INBOUND_ID`
- do not reuse VPN transport ports for browser proxy
- do not bind SOCKS5 to `443`
- use dedicated new ports for extension traffic
- keep rollback simple

Recommended example ports:

- `2081` for the first proxy
- `2082` for the second proxy

## 1. Provision Dedicated Proxy Hosts

You can reuse existing VPSes, but the proxy ports must stay separate from the VPN ports.

## 2. Install SOCKS5 on the First Host

```bash
cd /root/boxvolt
LISTEN_PORT=2081 \
EDGE_LOGIN_USER=edge_br \
EDGE_LOGIN_PASS='CHANGE_ME_STRONG_BR_PASS' \
ALLOW_CIDR=0.0.0.0/0 \
scripts/install_edge_socks5.sh
```

## 3. Install SOCKS5 on the Second Host

```bash
cd /root/boxvolt
LISTEN_PORT=2082 \
EDGE_LOGIN_USER=edge_ru \
EDGE_LOGIN_PASS='CHANGE_ME_STRONG_RU_PASS' \
ALLOW_CIDR=0.0.0.0/0 \
scripts/install_edge_socks5.sh
```

## 4. Verify Both Proxies

```bash
cd /root/boxvolt
scripts/check_edge_socks5.sh BR_HOST_OR_IP 2081 edge_br 'CHANGE_ME_STRONG_BR_PASS'
scripts/check_edge_socks5.sh RU_HOST_OR_IP 2082 edge_ru 'CHANGE_ME_STRONG_RU_PASS'
```

Do not continue until both checks pass.

## 5. Fill Backend `.env`

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

Restart BoxVolt after the env change.

## 6. Disable Demo Mode in the Extension

File:

- `browser-extension/config.js`

Set:

```js
demoMode: false,
demoBypassSubscription: false,
demoServers: []
```

Keep:

```js
apiBaseUrl: "https://connect.boxvolt.shop"
```

## 7. Final Smoke Test

### Active subscription

- login through Telegram
- receive approved session
- connect through the first proxy
- connect through the second proxy

### Inactive subscription

- login may succeed
- proxy use must still be denied by backend policy

## 8. Rollback

Fast rollback options:

1. set `demoMode: true` in the extension and reload it
2. set `EDGE_EXTENSION_ENABLED=0` in backend `.env`
3. restart `boxvolt-bot`

This rollback does not touch:

- `3x-ui`
- Xray VPN inbounds
- main subscription URLs
- payment logic

For the broader extension flow and repo layout, use [EXTENSION_GUIDE.md](EXTENSION_GUIDE.md).
