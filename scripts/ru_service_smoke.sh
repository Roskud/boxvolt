#!/usr/bin/env bash
set -euo pipefail

run_check() {
  local name="$1"
  local url="$2"
  local raw
  local code
  local connect
  local ttfb
  local total

  if raw="$(curl -4 -m 12 -sS -o /dev/null -w '%{http_code}|%{time_connect}|%{time_starttransfer}|%{time_total}' "$url" 2>/dev/null)"; then
    :
  else
    raw="000|NA|NA|NA"
  fi
  IFS='|' read -r code connect ttfb total <<<"$raw"
  code="${code:-000}"
  connect="${connect:-NA}"
  ttfb="${ttfb:-NA}"
  total="${total:-NA}"

  printf '%-18s code=%-4s connect=%-8ss ttfb=%-8ss total=%-8ss url=%s\n' \
    "$name" "$code" "$connect" "$ttfb" "$total" "$url"
}

echo "== RU Service Smoke =="
date -Is
echo

echo "-- DNS --"
for d in youtube.com discord.com t.me twitch.tv brawlstars.com clashroyale.com; do
  ip="$(getent ahostsv4 "$d" | awk '{print $1}' | head -n1 || true)"
  printf '%-18s -> %s\n' "$d" "${ip:-NO_A}"
done
echo

echo "-- HTTP reachability/latency --"
run_check "youtube_204" "https://www.youtube.com/generate_204"
run_check "youtube_home" "https://www.youtube.com/"
run_check "discord_api" "https://discord.com/api/v9/experiments"
run_check "telegram_web" "https://web.telegram.org/"
run_check "twitch_home" "https://www.twitch.tv/"
run_check "brawlstars" "https://brawlstars.com/"
run_check "clashroyale" "https://clashroyale.com/"
echo

echo "-- Network quick checks --"
ping -4 -c 4 -W 1 1.1.1.1 | tail -n 2 || true
echo

echo "-- zapret status --"
systemctl is-active zapret || true
systemctl is-enabled zapret 2>/dev/null || true
