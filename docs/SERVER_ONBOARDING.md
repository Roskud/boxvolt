# Server Onboarding Guide

This guide explains how to add and wire a new VPN node into the current BoxVolt architecture.

Important scope:

- today BoxVolt supports one primary node and one reserve RU-style node pattern
- this guide is enough to reproduce the current `DE + RU` production shape
- adding a third arbitrary region requires code changes, not just `.env`

## 1. Decide the Node Role

Use one of these roles:

1. Primary node
2. Reserve node

Primary node responsibilities:

- main `VLESS Reality TCP`
- main `/sub` identity source
- optional co-location with app host

Reserve node responsibilities:

- second region profiles in the same subscription
- optional `XHTTP`, `gRPC`, `VMess`, `Trojan`, `Hysteria2`
- independent fake-site masking

## 2. Prepare the VPS

Minimum checklist:

- Ubuntu or Debian
- static public IP
- domain or subdomain pointing to the server
- SSH access
- firewall opened for the ports you plan to expose

Recommended public ports:

- `443/tcp` for `VLESS Reality TCP`
- `2053/tcp` for `XHTTP`
- dedicated high TCP ports for `gRPC`, `VMess`, `Trojan`
- `443/udp` for `Hysteria2`
- `8443/tcp` for the local fake-site target behind Reality

## 3. Install and Prepare `3x-ui`

Install `3x-ui` using the official upstream installation flow that matches your target version.

Then create inbounds matching the BoxVolt expectation.

### Required inbound set

1. `VLESS Reality TCP`
2. `VLESS XHTTP`
3. `VLESS gRPC Reality`
4. `VMess TCP`
5. `Trojan Reality TCP`

### Recommended conventions

- keep inbound remarks explicit, for example `BoxVolt RU gRPC Reality`
- keep ports fixed once published
- do not rotate ports silently after users already imported the subscription

## 4. Configure the Main Reality TCP Masking

For the node-local `VLESS Reality TCP` inbound:

- bind it to `443/tcp`
- set `dest` to `127.0.0.1:8443`
- include a neutral fake-site hostname in `serverNames`

Recommended fake-site domains:

- `ru.<domain>`
- `de.<domain>`
- another region-specific neutral hostname

Recommended `serverNames` order:

1. your node-local fake-site domain
2. one safe fallback such as `www.cloudflare.com`
3. optional extra neutral hostnames

This lets existing clients keep working while new subscription updates can prefer your own domain.

## 5. Create the Node Fake-Site

On the node, configure nginx:

- plain HTTP on `80`
- HTTPS fake-site on `8443`
- a neutral static page that does not describe the host as a VPN server

Fake-site content should look like:

- edge gateway
- service routes
- status ingress
- client resources

Avoid:

- “VPN”
- “subscription”
- “proxy”
- direct onboarding instructions on the node hostname

## 6. Configure `Hysteria2`

Recommended pattern:

- bind to `:443` on UDP
- use a valid TLS certificate for the node hostname
- use `auth.type: http`
- point auth to BoxVolt
- use `masquerade.type: proxy`
- point masquerade to a neutral page

Recommended auth target options:

1. local app host endpoint such as `http://127.0.0.1:8080/internal/hy2/auth?secret=...`
2. shared public auth endpoint such as `https://web.<domain>/site/api/hy2/auth?secret=...`

Recommended masquerade targets:

- `https://connect.<domain>` on the primary host
- or a neutral local page if the node serves one itself

## 7. Map the Node into `.env`

### If the node is the primary node

Fill:

- `XUI_*`
- `INBOUND_ID`
- `SERVER_*`
- `PUBLIC_KEY`
- `SHORT_ID`
- `SNI`
- `SPEED_PROFILE_*`
- `GRPC_PROFILE_*`
- `VMESS_PROFILE_*`
- `TROJAN_PROFILE_*`
- `HY2_*`

### If the node is the reserve node

Fill:

- `ROUTE_RESERVE_*`
- `RU_XUI_*`
- `YOUTUBE_PROFILE_*`
- `RU_GRPC_PROFILE_*`
- `RU_VMESS_PROFILE_*`
- `RU_TROJAN_PROFILE_*`
- `RU_HY2_*`

## 8. Restart and Verify

After `.env` is updated:

```bash
systemctl restart boxvolt-bot
```

Then verify:

- `systemctl status boxvolt-bot`
- node `nginx`
- node `x-ui`
- node `hysteria-server`

Live checks:

- fake-site on `https://127.0.0.1:8443/` returns `200`
- if default host is used for masquerade, `http://127.0.0.1/` returns the intended neutral page
- `/sub/<token>` now includes the new region profiles

## 9. Naming and Ordering Rules

Keep names honest:

- do not include duplicate profiles that differ only by name
- make protocol visible in the label
- keep DE/RU paired order when possible

Current preferred subscription style:

1. `Hysteria2`
2. `XHTTP`
3. `Reality TCP`
4. `gRPC Reality`
5. `VMess TCP`
6. `Trojan Reality`

Within each transport, keep the region pairing:

- `DE`
- `RU`

## 10. Extension and MTProto Impact

Adding a VPN node does not automatically add:

- browser extension proxy nodes
- MTProto regions

Those are separate layers.

For extension nodes:

- follow [docs/EXTENSION_GUIDE.md](EXTENSION_GUIDE.md)

For MTProto:

- update the `MTPROTO_REGION_*` blocks and manager layer separately

## 11. Reproducibility Notes

To let another operator repeat the same node setup, always record:

- hostname
- IP
- inbound IDs
- ports
- fake-site hostname
- `Hysteria2` auth mode
- which `.env` variables were filled
- which tests passed after rollout

If you change the actual architecture, update this file in the same pull request.
