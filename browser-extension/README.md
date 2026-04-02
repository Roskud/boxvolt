# BoxVolt Browser Extension

Chromium target for:

- Google Chrome
- Microsoft Edge
- Opera

This extension authenticates through the BoxVolt Telegram bot and receives browser proxy endpoints from the BoxVolt backend.

## Start Here

- Full guide: [`../docs/EXTENSION_GUIDE.md`](../docs/EXTENSION_GUIDE.md)
- Safe production proxy setup: [`../docs/EDGE_EXTENSION_PROD_SETUP.md`](../docs/EDGE_EXTENSION_PROD_SETUP.md)

## Local Load

1. Open `chrome://extensions`, `edge://extensions` or `opera://extensions`
2. Enable developer mode
3. Click `Load unpacked`
4. Select the `browser-extension/` folder

## Important Config

Edit `config.js` before testing:

- set `apiBaseUrl`
- disable `demoMode` for real proxy testing

Production example:

```js
apiBaseUrl: "https://connect.boxvolt.shop",
demoMode: false,
demoBypassSubscription: false,
demoServers: []
```

## Backend Contract

The extension expects:

- `POST /edge/api/auth/start`
- `POST /edge/api/auth/poll`
- `GET /edge/api/me`
- `POST /edge/api/logout`

Those routes are implemented in `bot.py`.
