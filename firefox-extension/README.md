# BoxVolt Firefox Extension

Firefox target for the BoxVolt browser proxy flow.

This extension uses the same backend authorization logic as the Chromium build, but applies proxy settings through the Firefox extension API.

## Start Here

- Full guide: [`../docs/EXTENSION_GUIDE.md`](../docs/EXTENSION_GUIDE.md)
- Safe production proxy setup: [`../docs/EDGE_EXTENSION_PROD_SETUP.md`](../docs/EDGE_EXTENSION_PROD_SETUP.md)

## Local Load

1. Open `about:debugging#/runtime/this-firefox`
2. Click `Load Temporary Add-on...`
3. Select `firefox-extension/manifest.json`

## Important Config

Edit `config.js` before testing:

- set `apiBaseUrl`
- disable `demoMode` for real backend and proxy behavior

Production example:

```js
apiBaseUrl: "https://connect.boxvolt.shop",
demoMode: false,
demoBypassSubscription: false,
demoServers: []
```

## Backend Contract

The Firefox build uses the same backend routes:

- `POST /edge/api/auth/start`
- `POST /edge/api/auth/poll`
- `GET /edge/api/me`
- `POST /edge/api/logout`
