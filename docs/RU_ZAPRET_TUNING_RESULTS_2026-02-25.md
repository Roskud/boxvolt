# RU Zapret Tuning Results (2026-02-25)

## Scope
- Server: `178.159.94.114` (RU VPS)
- Goal: improve YouTube reachability while keeping Discord/Telegram/Twitch stable.
- Method: repeated curl smoke runs (3 passes per profile).

## Baseline and candidates tested

### Baseline (current before retune, UDP-only)
- `NFQWS_PORTS_TCP=`
- `NFQWS_PORTS_UDP=443,19294-19344,50000-50100`
- YouTube result (3 runs):  
  - `yt_204`: `0/3` success  
  - `yt_home`: `0/3` success  
  - `yt_watch`: `0/3` success  
- Discord/Telegram/Twitch: stable (`3/3`).

### Candidate A (TCP+UDP, moderate)
- `NFQWS_PORTS_TCP=80,443`
- `NFQWS_PORTS_UDP=443,19294-19344,50000-50100`
- YouTube result:
  - `yt_204`: `0/3`
  - `yt_home`: `0/3`
  - `yt_watch`: `1/3`
- Discord/Telegram/Twitch: stable (`3/3`).

### Candidate C (TCP+UDP, stronger TCP desync)
- `NFQWS_PORTS_TCP=80,443`
- `NFQWS_PORTS_UDP=443,19294-19344,50000-50100`
- YouTube result:
  - `yt_204`: `1/3`
  - `yt_home`: `2/3`
  - `yt_watch`: `0/3`
- Discord/Telegram/Twitch: stable (`3/3`).

### Candidate D (TCP only for YouTube + Discord UDP)
- `NFQWS_PORTS_TCP=80,443`
- `NFQWS_PORTS_UDP=19294-19344,50000-50100`
- YouTube result:
  - `yt_204`: `1/3`
  - `yt_home`: `0/3`
  - `yt_watch`: `1/3`
- Discord/Telegram/Twitch: stable (`3/3`).

## Selected profile (active)
Selected: **Candidate C** (best `yt_home` success ratio in this session).

Active config block:

```bash
NFQWS_PORTS_TCP=80,443
NFQWS_PORTS_UDP=443,19294-19344,50000-50100
NFQWS_OPT="
--filter-tcp=80 --hostlist=/opt/zapret/hostlists/list-google.txt --dpi-desync=fake,multisplit --dpi-desync-split-pos=method+2 --dpi-desync-fooling=md5sig --new
--filter-tcp=443 --hostlist=/opt/zapret/hostlists/list-google.txt --dpi-desync=fake,multidisorder --dpi-desync-split-pos=1,midsld --dpi-desync-fooling=badseq,md5sig --new
--filter-udp=443 --hostlist=/opt/zapret/hostlists/list-google.txt --dpi-desync=fake --dpi-desync-repeats=6 --dpi-desync-fake-quic=/opt/zapret/files/fake/quic_initial_www_google_com.bin --new
--filter-udp=19294-19344,50000-50100 --filter-l7=discord,stun --dpi-desync=fake --dpi-desync-repeats=3
"
```

## Rollback
- Backup created: `/opt/zapret/config.backup.20260225_223745`
- Quick rollback commands:

```bash
cp /opt/zapret/config.backup.20260225_223745 /opt/zapret/config
systemctl restart zapret
```

## Notes
- YouTube from RU remains partially unstable at network path level (timeouts persist even with `zapret` disabled on some runs).
- Current profile improves success ratio but does not guarantee 100% YouTube availability.
