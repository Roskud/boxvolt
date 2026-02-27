#!/usr/bin/env bash
set -euo pipefail

# Safe SOCKS5 setup for Edge extension.
# This script creates a separate danted-edge service on a dedicated port.
# It does NOT modify x-ui, nginx, or port 443.

if [[ "${EUID}" -ne 0 ]]; then
  echo "[error] run as root"
  exit 1
fi

LISTEN_PORT="${LISTEN_PORT:-}"
EDGE_LOGIN_USER="${EDGE_LOGIN_USER:-}"
EDGE_LOGIN_PASS="${EDGE_LOGIN_PASS:-}"
ALLOW_CIDR="${ALLOW_CIDR:-0.0.0.0/0}"
SERVICE_NAME="${SERVICE_NAME:-danted-edge}"
CONFIG_PATH="${CONFIG_PATH:-/etc/danted-edge.conf}"
DAEMON_USER="${DAEMON_USER:-sockd}"
WORKERS="${WORKERS:-8}"

usage() {
  cat <<'USAGE'
Usage:
  LISTEN_PORT=2081 EDGE_LOGIN_USER=br_edge EDGE_LOGIN_PASS='strong_pass' ./install_edge_socks5.sh

Optional:
  ALLOW_CIDR=0.0.0.0/0
  SERVICE_NAME=danted-edge
  CONFIG_PATH=/etc/danted-edge.conf
  DAEMON_USER=sockd

Notes:
  - Creates/updates one isolated SOCKS5 service for Edge extension.
  - Does not touch x-ui/nginx/443.
USAGE
}

if [[ -z "${LISTEN_PORT}" || -z "${EDGE_LOGIN_USER}" || -z "${EDGE_LOGIN_PASS}" ]]; then
  usage
  exit 1
fi

if ! [[ "${LISTEN_PORT}" =~ ^[0-9]+$ ]] || (( LISTEN_PORT < 1 || LISTEN_PORT > 65535 )); then
    echo "[error] LISTEN_PORT must be 1..65535"
    exit 1
fi

if ! [[ "${WORKERS}" =~ ^[0-9]+$ ]] || (( WORKERS < 1 || WORKERS > 64 )); then
  echo "[error] WORKERS must be 1..64"
  exit 1
fi

if ! [[ "${EDGE_LOGIN_USER}" =~ ^[a-zA-Z0-9_.-]{3,32}$ ]]; then
  echo "[error] EDGE_LOGIN_USER has invalid format"
  exit 1
fi

if [[ "${#EDGE_LOGIN_PASS}" -lt 8 ]]; then
  echo "[error] EDGE_LOGIN_PASS must be at least 8 characters"
  exit 1
fi

if ! command -v apt-get >/dev/null 2>&1; then
  echo "[error] only apt-based systems are supported by this script"
  exit 1
fi

echo "[step] installing dante-server"
export DEBIAN_FRONTEND=noninteractive
apt-get update -y
apt-get install -y dante-server

if ! id -u "${DAEMON_USER}" >/dev/null 2>&1; then
  echo "[step] creating daemon user: ${DAEMON_USER}"
  useradd --system --no-create-home --shell /usr/sbin/nologin "${DAEMON_USER}"
fi

if ! id -u "${EDGE_LOGIN_USER}" >/dev/null 2>&1; then
  echo "[step] creating auth user: ${EDGE_LOGIN_USER}"
  useradd --create-home --shell /usr/sbin/nologin "${EDGE_LOGIN_USER}"
fi
echo "${EDGE_LOGIN_USER}:${EDGE_LOGIN_PASS}" | chpasswd

EXT_IFACE="$(ip route get 1.1.1.1 | awk '{for (i=1; i<=NF; i++) if ($i=="dev") {print $(i+1); exit}}')"
if [[ -z "${EXT_IFACE}" ]]; then
  echo "[error] failed to detect external interface"
  exit 1
fi

echo "[step] writing ${CONFIG_PATH}"
cat > "${CONFIG_PATH}" <<EOF
logoutput: /var/log/${SERVICE_NAME}.log

internal: 0.0.0.0 port = ${LISTEN_PORT}
external: ${EXT_IFACE}

socksmethod: username

user.privileged: root
user.unprivileged: ${DAEMON_USER}

client pass {
  from: ${ALLOW_CIDR} to: 0.0.0.0/0
  log: connect disconnect error
}
client block {
  from: 0.0.0.0/0 to: 0.0.0.0/0
  log: error
}

socks pass {
  from: ${ALLOW_CIDR} to: 0.0.0.0/0
  command: bind connect udpassociate
  socksmethod: username
  log: connect disconnect error
}
socks block {
  from: 0.0.0.0/0 to: 0.0.0.0/0
  log: error
}
EOF

touch "/var/log/${SERVICE_NAME}.log"
chmod 640 "/var/log/${SERVICE_NAME}.log"

UNIT_PATH="/etc/systemd/system/${SERVICE_NAME}.service"
echo "[step] writing ${UNIT_PATH}"
cat > "${UNIT_PATH}" <<EOF
[Unit]
Description=Isolated Dante SOCKS5 for Edge extension
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
ExecStart=/usr/sbin/danted -f ${CONFIG_PATH} -N ${WORKERS}
ExecReload=/bin/kill -HUP \$MAINPID
Restart=on-failure
RestartSec=2
LimitNOFILE=65535

[Install]
WantedBy=multi-user.target
EOF

echo "[step] enabling ${SERVICE_NAME}"
systemctl daemon-reload
systemctl enable --now "${SERVICE_NAME}"

echo "[step] status"
systemctl --no-pager --full status "${SERVICE_NAME}" | sed -n '1,50p'

echo
echo "[ok] SOCKS5 is ready"
echo "  host: $(curl -4 -s https://ifconfig.me || echo 'YOUR_SERVER_IP')"
echo "  port: ${LISTEN_PORT}"
echo "  username: ${EDGE_LOGIN_USER}"
echo "  service: ${SERVICE_NAME}"
echo
echo "[next] run check script from any machine:"
echo "  scripts/check_edge_socks5.sh <host> ${LISTEN_PORT} ${EDGE_LOGIN_USER} '<password>'"
