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

## Event-driven refresh (Hassio-i0w, optional)
The periodic cycle is the baseline refresher, but a tile can be up to
`REFRESH_SECONDS` (600s) stale. When `HA_TOKEN` is set, the sidecar also runs a
websocket listener thread that subscribes to HA's `wyze_camera_event` bus event
(fired by `ha-wyzeapi` on every motion / Cam Plus AI detection) and grabs a
**fresh still for that one camera immediately**, off the 600s grid.

No go2rtc restart or KVS re-mint is needed: go2rtc runs **continuously** between
cycles with the last cycle's source lines, and the cycle period (600s) is well
under the KVS `X-Amz-Expires` (1800s), so any camera online at the last cycle
still has a valid stream. An event grab is just one `frame.jpeg` fetch against
the already-running go2rtc, serialised against the cycle's go2rtc restart by a
lock. Each event is handed to a bounded queue drained by a small worker pool, so
the websocket read loop never blocks on a slow camera (see resilience below).

Details:
- **Opt-in / secretless default:** the listener starts only when `HA_TOKEN` is
  set (an HA long-lived access token named `wyze-snapshot`). Unset ⇒ logs
  `event listener disabled` and behaves exactly as before (timer only).
- **Debounce:** at most one grab per camera per `EVENT_MIN_INTERVAL` (default
  15s), so a motion burst doesn't hammer go2rtc.
- **Sick-camera resilience (Hassio-3hd):** a camera whose go2rtc/KVS stream is
  timing out can't stall the pipeline. Events go to a bounded queue
  (`EVENT_QUEUE_MAX`, default 64) drained by `EVENT_WORKERS` tasks (default 3),
  so the read loop never serialises behind one cam. The event path uses a tight
  fetch budget (`EVENT_FRAME_TIMEOUT`=8s × `EVENT_FRAME_ATTEMPTS`=1) instead of
  the patient periodic budget, bounds its wait for the go2rtc lock
  (`EVENT_LOCK_TIMEOUT`=20s), and skips any camera that failed its last periodic
  frame grab until a later cycle recovers it.
- **State file untouched:** event grabs write **only** the JPEG, never
  `.offline_state.json` — the periodic cycle owns that bookkeeping.
- **Limitation (v1):** a camera that was **offline** at the last periodic cycle
  has no go2rtc stream, so its event is skipped (logged) and picked up by the
  next cycle.

## Detection-screenshot fast path (Hassio-zcm)
A live go2rtc grab is always **late**: `ha-wyzeapi` polls the Wyze cloud event
list every ~30s, so `wyze_camera_event` can fire long after the real detection,
and the KVS connect adds more. By the time we grab a "fresh" frame the subject has
usually left. But the event payload **already carries Wyze's own cloud-AI
screenshot** (`event_screenshot`), captured **at detection time** — it actually
contains the subject.

When `EVENT_USE_SCREENSHOT=1` (default) and the payload has a screenshot, the
sidecar fetches it once and uses it as the still for the **tile, the ZFS archive,
and the Gemini vision read** (a single 640×360 detection frame plus the existing
empty-scene baseline), skipping the live grab entirely. When the screenshot is
absent or unfetchable it falls back to the live `event_grab` / vision burst, so
behaviour degrades gracefully. Set `EVENT_USE_SCREENSHOT=0` to force the old live
grab.

- **Auth (Hassio-2ph):** the `event_screenshot` URL
  (`prod-sight-safe-auth.wyze.com`) is **self-authenticating** via its signed `st`
  query token — **no** Wyze access token is sent. Wyze's gateway runs a
  User-Agent allowlist and returns a misleading `401 "Access token is invalid."`
  for unknown/bot UAs, so the fetch sends `EVENT_MEDIA_UA` (default `okhttp/4.9.3`,
  the Wyze Android app UA). **No `Authorization` header** is sent (the Azure-blob
  backend `400`s on one). `EVENT_MEDIA_TIMEOUT` (default 10s) bounds the fetch.
- **Debounce shared:** uses the same `EVENT_MIN_INTERVAL` clock as `event_grab`,
  so a screenshot and a live grab never double-write a tile within the window.
- **`event_video`:** also present in the payload but typically `404` for these
  cams (no Cam Plus cloud clip), so only the still is used.
- **Green detection box:** Wyze draws a green bounding box on `event_screenshot`
  around the region it flagged as moving. When the vision frame is a screenshot the
  Gemini prompt is told about the box so it focuses on the trigger — while being
  told the box is a software overlay, not a real object (so it isn't described as
  part of the scene). Live-fallback frames have no box, so the hint is omitted.

## Event-still archive (Hassio-5sa / -bp8 / -ud0, optional)
The event listener also archives a **history of selected camera events** to durable
ZFS storage. When a `wyze_camera_event` arrives, the sidecar consults `ARCHIVE_RULES`
(JSON `{stream_key: [label, …]}`); if the cam is listed and the event matches a
wanted label, it copies the **current** `/config/wyze_snapshots/<key>.jpg` to a
timestamped file `<key>/<UTC-timestamp>.jpg` under `ARCHIVE_DIR`. Filenames carry
microseconds to avoid same-second collisions. This rides on the existing event
listener — **no new container, no second token, no `camera_proxy` fetch** (it copies
the on-disk still the event grab just refreshed).

- **Configurable cams + event types (req #3):** `ARCHIVE_RULES` is JSON
  `{stream_key: [label, …]}`. Labels are `person`/`pet`/`vehicle`/`package` (mapped
  to `tag_list` codes 101/102/103/104, or matched against `ai_tag_list` names; see
  memory `wyze-event-tag-list-mapping`), or `"any"`/`"*"` for **every** event on that
  cam. Default `{"front_door": ["person"]}`. Edit the env in `docker-compose.yml` to
  add/change archived cams or event types — **no code change**. A malformed
  `ARCHIVE_RULES` logs once and disables the archive (fail-safe).
- **Auto-created subdirs (req #2):** there are **no pre-created dirs**. The per-cam
  subdir `<ARCHIVE_DIR>/<key>/` is created on first write via
  `os.makedirs(…, exist_ok=True)`, so adding or renaming a camera (new `stream_key`)
  just works the next time it fires a matching event.
- **NFS-safe sentinel guard (req #1):** the archive is active only when the marker
  file `ARCHIVE_MARKER` (default `.archive_root`) exists **inside** `ARCHIVE_DIR`.
  Create it once on the ZFS: `touch /mnt/tank/shared/wyze/.archive_root`. If the NFS
  mount is down, docker auto-creates an empty *local* `/mnt/tank-shared/wyze` that
  **lacks** the marker, so the archive disables (logs once, re-arms when the marker
  returns) and stills never land on local disk. This replaces the old bare
  `isdir()` opt-in.
- **Storage:** the proxmox ZFS dataset `tank/shared`, already NFS-exported
  (`sharenfs rw=@10.69.40.0/21`, which covers this host) and mounted on `hassio`
  at `/mnt/tank-shared` (fstab `10.69.42.12:/mnt/tank/shared … nfs _netdev,nofail,vers=4`).
  `/mnt/tank-shared/wyze` is bind-mounted into the container at `/archive`.
- **Retention:** on each write, that cam's `<key>/` is pruned of `*.jpg` older than
  `ARCHIVE_RETENTION_DAYS` (default 100) by mtime (best-effort; a prune error is
  logged, never fatal).
- **Bind-ordering caveat:** the NFS mount must be **up before** the container
  starts — docker auto-creates an empty *local* `/mnt/tank-shared/wyze` if the
  source is absent. The marker guard above keeps writes off local disk, but to
  actually archive you still need the mount: the fstab `_netdev` mount handles this
  across reboots; after a manual NFS outage, `mount -a` then `docker compose up -d`.

## Gemini vision analysis (Hassio-5sk, optional)
The event listener can also **describe what the camera saw** and surface it as an
HA sensor. On a matching `wyze_camera_event`, the sidecar pulls a short
**multi-frame burst** from the already-warm go2rtc stream (default 4 frames ~1.5s
apart), sends them in one request to **Gemini** (`gemini-2.5-flash`) for a
structured description, and publishes the result to an MQTT-discovery sensor
`sensor.wyze_<key>_vision`. The burst (not a single still) is the point: it lets
the model report **what CHANGES across the frames** — who moved, in which
direction, what they were doing — which a lone frame usually misses.

- **Opt-in / secretless default:** active only when `GEMINI_API_KEY` is set **and**
  the MQTT publisher is configured (the sensor rides the same `dokr` broker). Unset
  either ⇒ logs `vision disabled …` and the rest of the sidecar is unaffected. Use
  a **paid-tier** AI Studio key — the free tier trains on your images. Key in macOS
  Keychain svc `gemini-api-key`, supplied via the gitignored `.env`.
- **Sensor shape:** state is the **event timestamp** (`device_class: timestamp`);
  the description lives in the entity **attributes** — `change_detected` (did
  anything differ from the background), `summary`, `description`, `motion` (the
  cross-frame narrative), `people` count, the `*_present` booleans
  (person/package/vehicle/pet), optional `notable`, plus meta (`camera`, `model`,
  `frames`, `analyzed_at`). The discovery config is published **once per run**
  (lazy, on first result); state + attributes are retained so HA shows the last
  result across restarts.
- **Recurring-background suppression (Hassio-i02, `VISION_BASELINE=1`, default on):**
  by default the model would re-describe the *fixed* scene every event (porch, tree,
  parked car, furniture). Instead, the periodic cycle caches each cam's ambient
  timer-driven still as a **background reference** (`<OUT_DIR>/baselines/<key>.jpg`),
  and the burst sends it to Gemini as image 1 with instructions to **ignore anything
  also in it** and report only what's new/moving/changed — adding a `change_detected`
  boolean (false + `summary:"no change"` when the burst matches the background). The
  baseline refreshes every cycle, so it tracks lighting/season; until the first cycle
  caches one for a cam, that cam falls back to describing the raw burst. Set
  `VISION_BASELINE=0` to disable.
- **Configurable cams + event types:** `VISION_RULES` is JSON
  `{stream_key|"*": [label|"any", …]}` (same shape as `ARCHIVE_RULES`, plus a `"*"`
  **camera** key matching any cam). Default `{"*": ["any"]}` analyses every camera
  on every event. Bad JSON logs once and disables vision (fail-safe).
- **Cost control:** a per-cam debounce `VISION_MIN_INTERVAL` (default 30s) means a
  motion burst costs **one** analysis, not dozens; byte-identical frames in a burst
  are de-duplicated before sending. One 4-frame analysis is ~a few tenths of a cent.
- **Privacy / safety:** the API key rides in the request query string, so the code
  **never logs the request URL** — only the HTTP status and a short error tail. The
  vision call runs on an executor and is fully best-effort (its own try/except), so
  a Gemini outage never drops the websocket or the still-grab/archive paths.

## Deploy (hassio VM)
```bash
scp -r infra/hassio/wyze-snapshot/ hassio:~/homeassistant/wyze-snapshot/
# enable the MQTT bridge (Hassio-708): write the wyzesnap password to .env
ssh hassio 'umask 077; printf "MQTT_PASS=%s\n" "<wyzesnap-pw>" > ~/homeassistant/wyze-snapshot/.env'
ssh hassio 'cd ~/homeassistant/wyze-snapshot && docker compose up -d --build'
ssh hassio 'docker logs -f wyze-snapshot'        # watch the first cycle
ssh hassio 'ls -l ~/homeassistant/config/wyze_snapshots/'
```
The `.env` is only needed to enable the optional features — the MQTT bridge
(`MQTT_PASS`), the event-driven grab (`HA_TOKEN`), and/or Gemini vision
(`GEMINI_API_KEY`, which also needs `MQTT_PASS`); see `.env.example`. Append each
to the same `.env`, e.g. `printf 'GEMINI_API_KEY=%s\n' "$(security
find-generic-password -a "$USER" -s gemini-api-key -w)" >> .env`. The snapshot loop
itself needs no secrets.

## Manage
```bash
ssh hassio 'docker {logs,restart} wyze-snapshot'
```
