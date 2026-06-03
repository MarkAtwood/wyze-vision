# wyze-snapshot sidecar (Hassio-8qm)

Periodically writes a still JPEG for every **online** Wyze camera to
`/config/wyze_snapshots/<cam>.jpg`, pulled over the Amazon Kinesis Video Streams
(KVS) WebRTC **cloud** path via go2rtc. This is the only reliable Wyze still
source here — every local path is blocked (symmetric NAT breaks TUTK/IOTC P2P,
and `ha-wyzeapi`'s `camera_proxy` is WebRTC-live-only and 500s for stills). See
the proof in `../wyze-go2rtc-proof/` and memories `wyze-go2rtc-still-proof`,
`go2rtc-supports-kvs-webrtc`, `wyze-bridge-status`.

The JPEGs are surfaced into HA as `local_file` cameras (`ha-packages/wyze_snapshots.yaml`,
Hassio-5pj) and shown on a "Cameras" dashboard (Hassio-bas). `local_file` re-reads
the file every request, so a tile always shows the **last-good** frame and never
greys out when KVS creds rotate.

## How it works
One container (built `FROM alexxit/go2rtc`, which already bundles the static
go2rtc 1.9.14 binary + ffmpeg + python3) runs `snapshot.py`, which every
`REFRESH_SECONDS` (default 600, **must stay < 1800** = KVS `X-Amz-Expires`):

1. reads the Wyze access/refresh tokens from `/config/.storage/core.config_entries`
   (the `wyzeapi` entry) — **no Wyze password or HA token needed**;
2. enumerates cameras with `wyzeapy` and calls `get_stream_info` on each
   (online vs offline);
3. builds a go2rtc kinesis source line per online cam
   (`webrtc:<signaling_url>#format=kinesis#client_id=<id>#ice_servers=<json>`,
   ice key `url` singular, `%25`→`%`) and writes `go2rtc.yaml`;
4. (re)starts the go2rtc child process (restart each cycle is intentional —
   reliable, no docker socket, avoids HA's managed go2rtc on port 11984);
5. GETs `http://127.0.0.1:1984/api/frame.jpeg?src=<cam>` for each and **atomically**
   writes `/config/wyze_snapshots/<cam>.jpg` (`os.replace`, so HA never reads a
   half-written file).

**Offline cameras** (Hassio-on2) get a generated placeholder JPEG written to the
same `<cam>.jpg` path, showing `<camera name>` and `offline since <date>`, so the
dashboard tile clearly reads "offline" rather than greying out or showing a stale
frame. The "since" date is the last time the sidecar observed the camera online,
tracked in `/config/wyze_snapshots/.offline_state.json` (falls back to first time
seen offline if never observed online). The placeholder is a JPEG (not a PNG) so it
flows through the existing `local_file` camera and its `image/jpeg` content type.

`client_id` is the hardcoded constant `ada06f08-…` — `get_stream_info` exposes no
per-session client id, and go2rtc passes it verbatim as the signaling
`recipientClientId` without needing it to match the URL's `X-Amz-ClientId`
(verified; the proof used this same value).

## Deploy (hassio VM)
```bash
scp -r infra/hassio/wyze-snapshot/ hassio:~/homeassistant/wyze-snapshot/
ssh hassio 'cd ~/homeassistant/wyze-snapshot && docker compose up -d --build'
ssh hassio 'docker logs -f wyze-snapshot'        # watch the first cycle
ssh hassio 'ls -l ~/homeassistant/config/wyze_snapshots/'
```
No `.env` is required (see `.env.example` for the optional tunables).

## Manage
```bash
ssh hassio 'docker {logs,restart} wyze-snapshot'
```
