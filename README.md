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

## Wyze cloud conn-state bridge (Hassio-708, optional)
The Wyze device list carries `conn_state` (1=online/0=offline) and `conn_state_ts`
(epoch ms of the last connection-state change) — a **durable** last-contact time,
unlike HA's `last_updated` which only moves on a state change and so pins offline
cams to the last HA restart. `ha-wyzeapi` does **not** expose `conn_state_ts` as an
HA attribute, and only this sidecar (via `wyzeapy`) has Wyze cloud access, so it
bridges the value over MQTT to the `device-inventory-sheet` sidecar (on `dokr`),
which uses it to date the inventory's **Wyze tab "Last Seen"**.

Each cycle, when `MQTT_HOST`/`MQTT_USER`/`MQTT_PASS` are all set, it publishes a
**retained** `wyze/<mac>/status` message (`{"conn_state","conn_state_ts"}`,
`<mac>` lower-case no-colon) per camera. The subscriber maps online→now,
offline→`conn_state_ts`, no-bridge→HA `last_updated`. The publisher is **opt-in**:
with no `MQTT_PASS` it disables itself and the sidecar stays secretless.

Credentials: a dedicated, publish-only broker user `wyzesnap` (password in macOS
Keychain svc `mqtt-wyzesnap-password`, mirrored to the inventory Secrets/API Keys
tabs). Note the `dokr` broker has no `acl_file`, so the publish-only restriction
is by convention (separate credential), not broker-enforced.

## Deploy (hassio VM)
```bash
scp -r infra/hassio/wyze-snapshot/ hassio:~/homeassistant/wyze-snapshot/
# enable the MQTT bridge (Hassio-708): write the wyzesnap password to .env
ssh hassio 'umask 077; printf "MQTT_PASS=%s\n" "<wyzesnap-pw>" > ~/homeassistant/wyze-snapshot/.env'
ssh hassio 'cd ~/homeassistant/wyze-snapshot && docker compose up -d --build'
ssh hassio 'docker logs -f wyze-snapshot'        # watch the first cycle
ssh hassio 'ls -l ~/homeassistant/config/wyze_snapshots/'
```
The `.env` is only needed to enable the MQTT bridge (see `.env.example`); the
snapshot loop itself needs no secrets.

## Manage
```bash
ssh hassio 'docker {logs,restart} wyze-snapshot'
```
