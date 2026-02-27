# RU Zapret: Test and Tuning Plan

## Goal
- Keep RU profile stable for: YouTube, Discord, Telegram, Twitch, Brawl Stars, Clash Royale.
- Minimize breakage for non-target traffic.
- Lock one reproducible configuration with rollback path.

## Scope
- This plan applies only to RU VPS/profile.
- DE profiles must remain untouched.

## Acceptance Criteria
- YouTube pages/videos load without transport timeouts.
- Discord API and voice gateway are reachable.
- Telegram works normally (text/media).
- Twitch homepage/stream directory opens without long stalls.
- Mobile games connect and keep stable sessions (manual checks).
- No global internet degradation after zapret enable.

## Test Environment
- RU VPS with current zapret build.
- One test account with active RU subscription profile.
- 2 client networks:
  - Home Wi-Fi
  - Mobile data
- 2 client devices:
  - Android
  - Windows/macOS

## Phase 1: Baseline (Before Changes)
1. Save current config and service states.
2. Run service smoke script on RU VPS.
3. Run client checks from app side (manual).
4. Store metrics in a dated log file.

### Server commands
```bash
mkdir -p /root/boxvolt/tests
date -Is | tee -a /root/boxvolt/tests/ru_baseline.log
systemctl status zapret --no-pager | sed -n '1,80p' | tee -a /root/boxvolt/tests/ru_baseline.log
/root/boxvolt/scripts/ru_service_smoke.sh | tee -a /root/boxvolt/tests/ru_baseline.log
```

## Phase 2: Controlled Profiles
Use 3 profiles, test one at a time:
- `P1_BALANCED`: minimal desync, target hostlists only (youtube/discord).
- `P2_AGGRESSIVE_YT`: stronger desync only for YouTube domains.
- `P3_FALLBACK_STABLE`: conservative profile if P1/P2 degrade other services.

Rules:
- Change one variable set per iteration.
- Keep max 1 active candidate at a time.
- Collect metrics after each restart.

## Phase 3: Measurement Matrix
For each candidate profile, run:

### Automated
- `/root/boxvolt/scripts/ru_service_smoke.sh`
- `speedtest --secure --simple` (optional, non-blocking)
- `journalctl -u zapret -n 150 --no-pager`

### Manual (Client side)
- YouTube:
  - open homepage
  - open 1080p video
  - open Shorts
  - note preload/start delay
- Discord:
  - open channels
  - test voice connect (1-2 min)
- Telegram:
  - send media + open links
- Twitch:
  - open stream page + change quality
- Games:
  - login, enter match/lobby, 5-10 min session

## Scoring Model (Simple)
- Reachability (40%): timeout/error rate by service.
- Latency (20%): connect/ttfb from smoke tests.
- Stability (30%): reconnects, freezes, app errors.
- Side effects (10%): non-target services degradation.

Pick profile with best total score, not just fastest YouTube.

## Rollback
Rollback conditions:
- Two or more core services fail.
- Discord voice breaks repeatedly.
- YouTube works but Telegram/Twitch degrades noticeably.

Rollback steps:
1. Restore last known-good zapret config.
2. Restart zapret service.
3. Re-run smoke script and verify client checks.

## Change Control
- Every profile change must record:
  - timestamp
  - config diff summary
  - smoke output
  - manual notes per service

## Final Handover Output
- Final selected profile name.
- Exact config files/flags used.
- Before/after smoke metrics.
- Known limitations and fallback profile ID.
