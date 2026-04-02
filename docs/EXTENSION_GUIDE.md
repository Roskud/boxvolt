# Browser Extension Guide

BoxVolt ships two browser extension targets:

- `browser-extension/` for Chrome, Edge and Opera
- `firefox-extension/` for Firefox

The extension layer is separate from the VPN subscription layer. It uses dedicated proxy endpoints and Telegram-based authorization through the BoxVolt backend.

## 1. How the Flow Works

1. The user opens the extension.
2. The extension requests `POST /edge/api/auth/start`.
3. The backend returns a Telegram deep-link.
4. The user confirms login in the BoxVolt bot.
5. The extension polls `POST /edge/api/auth/poll`.
6. After approval, the extension receives a session and can load `GET /edge/api/me`.
7. The backend returns subscription state and allowed proxy endpoints.

## 2. Backend Requirements

The backend routes already exist in `bot.py`.

Required env flags:

```env
EDGE_EXTENSION_ENABLED=1
EDGE_AUTH_PREFIX=edgeauth
EDGE_AUTH_REQUEST_TTL_SECONDS=600
EDGE_SESSION_TTL_SECONDS=2592000
EDGE_MAX_ACTIVE_SESSIONS_PER_USER=5
```

You also need at least one configured proxy pair:

```env
EDGE_SERVER_BR_LABEL=Brazil
EDGE_SERVER_BR_HOST=BR_HOST_OR_IP
EDGE_SERVER_BR_PORT=2081
EDGE_SERVER_BR_SCHEME=socks5
EDGE_SERVER_BR_USERNAME=edge_br
EDGE_SERVER_BR_PASSWORD=CHANGE_ME

EDGE_SERVER_RU_LABEL=Russia
EDGE_SERVER_RU_HOST=RU_HOST_OR_IP
EDGE_SERVER_RU_PORT=2082
EDGE_SERVER_RU_SCHEME=socks5
EDGE_SERVER_RU_USERNAME=edge_ru
EDGE_SERVER_RU_PASSWORD=CHANGE_ME
```

## 3. Provision Proxy Servers

The safest production path in this repository uses dedicated SOCKS5 ports instead of reusing VPN ports.

Existing helper scripts:

- `scripts/install_edge_socks5.sh`
- `scripts/check_edge_socks5.sh`

Use the production supplement:

- [docs/EDGE_EXTENSION_PROD_SETUP.md](EDGE_EXTENSION_PROD_SETUP.md)

## 4. Configure the Extension

### Chromium extension

File:

- `browser-extension/config.js`

Set:

- `apiBaseUrl`
- `demoMode`
- `demoBypassSubscription`
- `demoServers`

For production:

```js
apiBaseUrl: "https://connect.boxvolt.shop",
demoMode: false,
demoBypassSubscription: false,
demoServers: []
```

### Firefox extension

File:

- `firefox-extension/config.js`

Use the same `apiBaseUrl` and disable demo mode for production.

## 5. Local Test Install

### Chrome

1. Open `chrome://extensions`
2. Enable developer mode
3. Click `Load unpacked`
4. Select `browser-extension/`

### Edge

1. Open `edge://extensions`
2. Enable developer mode
3. Click `Load unpacked`
4. Select `browser-extension/`

### Opera

1. Open `opera://extensions`
2. Enable developer mode
3. Click `Load unpacked`
4. Select `browser-extension/`

### Firefox

1. Open `about:debugging#/runtime/this-firefox`
2. Click `Load Temporary Add-on...`
3. Select `firefox-extension/manifest.json`

## 6. Production Packaging

The repository already contains example production zips, but your release flow should rebuild from the current source.

Before publishing:

- replace demo values in `config.js`
- limit `host_permissions` in `manifest.json`
- verify privacy policy text
- verify store descriptions match the subscription model
- verify Telegram login flow end-to-end

## 7. User-Facing Flow to Test

Test all of these before publishing.

### Active subscription

- user can start auth
- bot approves auth
- extension receives session
- extension sees available servers
- proxy activation works

### Inactive subscription

- user can authorize
- extension does not get access to paid proxy usage

### Logout

- session is revoked through `POST /edge/api/logout`
- extension returns to login state

## 8. Relationship to the Main VPN Service

The extension is intentionally separate from:

- `/sub/<token>`
- `3x-ui` VPN client provisioning
- `Hysteria2` token auth
- `AmneziaVPN` text keys

This separation keeps browser proxy rollout safer.

## 9. Common Mistakes

- reusing `443` or Xray ports for browser proxy
- leaving `demoMode: true` in production
- forgetting to restart `boxvolt-bot` after `.env` changes
- exposing broad `host_permissions` in the store build
- publishing the extension without testing Telegram login from a clean browser profile

## 10. Recommended Repo Workflow

When you change extension behavior:

1. Update the relevant extension folder.
2. Update this guide if the flow changed.
3. Re-test Chromium and Firefox targets.
4. Re-test `GET /edge/api/me` on the backend.
5. Rebuild your release packages.
