#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 4 ]]; then
  cat <<'USAGE'
Usage:
  check_edge_socks5.sh <host> <port> <username> <password> [test_url]

Example:
  check_edge_socks5.sh 203.0.113.10 2081 br_edge 'strong_pass' https://www.youtube.com/generate_204
USAGE
  exit 1
fi

HOST="$1"
PORT="$2"
USERNAME="$3"
PASSWORD="$4"
TEST_URL="${5:-https://www.youtube.com/generate_204}"

if ! [[ "${PORT}" =~ ^[0-9]+$ ]] || (( PORT < 1 || PORT > 65535 )); then
  echo "[error] bad port: ${PORT}"
  exit 1
fi

echo "== SOCKS5 Handshake Check =="
python3 - "$HOST" "$PORT" "$USERNAME" "$PASSWORD" <<'PY'
import socket
import sys

host = sys.argv[1]
port = int(sys.argv[2])
user = sys.argv[3].encode("utf-8")
pw = sys.argv[4].encode("utf-8")

def fail(msg):
    print(f"[fail] {msg}")
    raise SystemExit(1)

if len(user) > 255 or len(pw) > 255:
    fail("username/password too long for SOCKS5 auth packet")

try:
    s = socket.create_connection((host, port), timeout=8)
    s.settimeout(8)
except Exception as exc:
    fail(f"connect error: {exc}")

try:
    # greeting: version 5, one method, username/password (0x02)
    s.sendall(b"\x05\x01\x02")
    r = s.recv(2)
    if r != b"\x05\x02":
        fail(f"unexpected greeting reply: {r!r}")
    print("[ok] greeting accepted")

    # username/password auth: version 1
    auth = b"\x01" + bytes([len(user)]) + user + bytes([len(pw)]) + pw
    s.sendall(auth)
    r = s.recv(2)
    if r != b"\x01\x00":
        fail(f"auth failed: {r!r}")
    print("[ok] username/password auth accepted")

    # CONNECT to example.com:443
    host_bytes = b"example.com"
    req = b"\x05\x01\x00\x03" + bytes([len(host_bytes)]) + host_bytes + (443).to_bytes(2, "big")
    s.sendall(req)
    hdr = s.recv(4)
    if len(hdr) < 4:
        fail("short reply header")
    if hdr[0] != 5:
        fail(f"bad reply version: {hdr[0]}")
    if hdr[1] != 0:
        fail(f"connect rejected, code={hdr[1]}")
    atyp = hdr[3]
    if atyp == 1:
        _ = s.recv(4 + 2)
    elif atyp == 3:
        ln = s.recv(1)
        if not ln:
            fail("short domain len")
        _ = s.recv(ln[0] + 2)
    elif atyp == 4:
        _ = s.recv(16 + 2)
    else:
        fail(f"unknown atyp: {atyp}")
    print("[ok] CONNECT example.com:443 accepted")
finally:
    s.close()
PY

echo
echo "== Curl Through SOCKS5 =="
curl -4 -sS -m 15 \
  --socks5-hostname "${HOST}:${PORT}" \
  --proxy-user "${USERNAME}:${PASSWORD}" \
  -o /dev/null \
  -w 'http_code=%{http_code} connect=%{time_connect}s ttfb=%{time_starttransfer}s total=%{time_total}s\n' \
  "${TEST_URL}"

echo "[ok] check finished"
