# wyze-vision

A Home Assistant sidecar for Wyze cameras: it pulls still JPEGs over the Wyze
**cloud** path (Amazon Kinesis Video Streams WebRTC) via go2rtc, grabs a fresh
still the instant a camera fires an event, archives selected event stills to a
durable share, and (optionally) describes what each camera saw using Google
Gemini vision — surfacing it all on a Home Assistant dashboard.

> **Requires the [`ha-wyzeapi`](https://github.com/SecKatie/ha-wyzeapi) integration.**
> wyze-vision does **not** log into Wyze itself — it builds on `ha-wyzeapi`, which
> must already be installed and signed in to your Wyze account in Home Assistant.
> That integration holds your Wyze login; this sidecar only reads the tokens it
> stored. See [Prerequisites & credentials](#prerequisites--credentials).

Periodically writes a still JPEG for every **online** Wyze camera to
`/config/wyze_snapshots/<cam>.jpg`, pulled over the Amazon Kinesis Video Streams
(KVS) WebRTC **cloud** path via go2rtc. This is often the only reliable Wyze
still source — the local paths are commonly blocked (symmetric NAT breaks
TUTK/IOTC P2P, and `ha-wyzeapi`'s `camera_proxy` is WebRTC-live-only and 500s for
stills).

The JPEGs are surfaced into HA as `local_file` cameras (provisioned by
`deploy/provision_local_file_cameras.sh`) and shown on a "Cameras" dashboard
(`deploy/cameras-dashboard.yaml`). `local_file` re-reads the file every request,
so a tile always shows the **last-good** frame and never greys out when KVS creds
rotate.

## Prerequisites & credentials
**Where do I log into Wyze?** You don't — not in this sidecar. wyze-vision is a
**companion to the [`ha-wyzeapi`](https://github.com/SecKatie/ha-wyzeapi)
integration**, which must already be installed and signed in to your Wyze account
in Home Assistant. That integration is the **only** place your Wyze email +
password (+ 2FA) are entered, and it's a hard prerequisite — wyze-vision also
relies on the `wyze_camera_event` bus event that `ha-wyzeapi` fires.

**Where is the Wyze password kept?** Not here, and not by this sidecar at all.
`ha-wyzeapi` performs the login; Home Assistant stores the resulting Wyze OAuth
**access/refresh tokens** (not the password) in
`/config/.storage/core.config_entries`. wyze-vision mounts HA's config dir
(`../config:/config` in `docker-compose.yml`) and **reads those tokens** at
runtime (`CONFIG_ENTRIES=/config/.storage/core.config_entries`). No Wyze
credential is ever entered into, logged by, or stored in this repo, its `.env`, or
its environment.

**The only secrets wyze-vision itself takes** are optional feature-gates, kept in a
gitignored `.env` on the host (never committed; see `.env.example`) — and **none of
them is a Wyze credential**:
- `MQTT_PASS` — your broker password, enabling the conn-state bridge + vision sensor.
- `HA_TOKEN` — a Home Assistant long-lived access token, enabling the event-driven grab.
- `GEMINI_API_KEY` — a Google AI Studio key, enabling Gemini vision.

Leave any of them unset and that feature disables itself; with none set, the
sidecar runs **secretless** (periodic stills only).

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

**Offline cameras** get a generated placeholder JPEG written to the same
`<cam>.jpg` path, showing `<camera name>` and `offline since <date>`, so the
dashboard tile clearly reads "offline" rather than greying out or showing a stale
frame. The "since" date is the last time the sidecar observed the camera online,
tracked in `/config/wyze_snapshots/.offline_state.json` (falls back to first time
seen offline if never observed online). The placeholder is a JPEG (not a PNG) so it
flows through the existing `local_file` camera and its `image/jpeg` content type.

`client_id` is the hardcoded constant `ada06f08-…` — `get_stream_info` exposes no
per-session client id, and go2rtc passes it verbatim as the signaling
`recipientClientId` without needing it to match the URL's `X-Amz-ClientId`
(verified).

## Wyze cloud conn-state bridge (optional)
The Wyze device list carries `conn_state` (1=online/0=offline) and `conn_state_ts`
(epoch ms of the last connection-state change) — a **durable** last-contact time,
unlike HA's `last_updated` which only moves on a state change and so pins offline
cams to the last HA restart. `ha-wyzeapi` does **not** expose `conn_state_ts` as an
HA attribute, and only this sidecar (via `wyzeapy`) has Wyze cloud access, so it
can bridge the value over MQTT to any downstream consumer (for example, a
device-inventory tracker that dates each camera's "last seen").

Each cycle, when `MQTT_HOST`/`MQTT_USER`/`MQTT_PASS` are all set, it publishes a
**retained** `wyze/<mac>/status` message (`{"conn_state","conn_state_ts"}`,
`<mac>` lower-case no-colon) per camera. A subscriber maps online→now,
offline→`conn_state_ts`, no-bridge→HA `last_updated`. The publisher is **opt-in**:
with no `MQTT_PASS` it disables itself and the sidecar stays secretless.

Credentials: use a dedicated, publish-only broker user, with its password
supplied via the gitignored `.env` (`MQTT_PASS`). Note that if your broker has no
ACL file, the publish-only restriction is by convention (a separate credential),
not broker-enforced.

## Event-driven refresh (optional)
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
  set (a Home Assistant long-lived access token). Unset ⇒ logs
  `event listener disabled` and behaves exactly as before (timer only).
- **Debounce:** at most one grab per camera per `EVENT_MIN_INTERVAL` (default
  15s), so a motion burst doesn't hammer go2rtc.
- **Sick-camera resilience:** a camera whose go2rtc/KVS stream is
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

## Detection-screenshot fast path
A live go2rtc grab is always **late**: `ha-wyzeapi` polls the Wyze cloud event
list every ~30s, so `wyze_camera_event` can fire long after the real detection,
and the KVS connect adds more. By the time we grab a "fresh" frame the subject has
usually left. But the event payload **already carries Wyze's own cloud-AI
screenshot** (`event_screenshot`), captured **at detection time** — it actually
contains the subject.

When `EVENT_USE_SCREENSHOT=1` (default) and the payload has a screenshot, the
sidecar fetches it once and uses it as the still for the **tile, the archive,
and the Gemini vision read** (a single 640×360 detection frame plus the existing
empty-scene baseline), skipping the live grab entirely. When the screenshot is
absent or unfetchable it falls back to the live `event_grab` / vision burst, so
behaviour degrades gracefully. Set `EVENT_USE_SCREENSHOT=0` to force the old live
grab.

- **Auth:** the `event_screenshot` URL is **self-authenticating** via its signed
  `st` query token — **no** Wyze access token is sent. Wyze's gateway runs a
  User-Agent allowlist and returns a misleading `401 "Access token is invalid."`
  for unknown/bot UAs, so the fetch sends `EVENT_MEDIA_UA` (default `okhttp/4.9.3`,
  the Wyze Android app UA). **No `Authorization` header** is sent (the storage
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

## Event-still archive (optional)
The event listener also archives a **history of selected camera events** to durable
storage (e.g. an NFS-mounted NAS). When a `wyze_camera_event` arrives, the sidecar
consults `ARCHIVE_RULES` (JSON `{stream_key: [label, …]}`); if the cam is listed
and the event matches a wanted label, it copies the **current**
`/config/wyze_snapshots/<key>.jpg` to a timestamped file `<key>/<UTC-timestamp>.jpg`
under `ARCHIVE_DIR`. Filenames carry microseconds to avoid same-second collisions.
This rides on the existing event listener — **no new container, no second token, no
`camera_proxy` fetch** (it copies the on-disk still the event grab just refreshed).

- **Configurable cams + event types:** `ARCHIVE_RULES` is JSON
  `{stream_key: [label, …]}`. Labels are `person`/`pet`/`vehicle`/`package` (mapped
  to `tag_list` codes 101/102/103/104, or matched against `ai_tag_list` names), or
  `"any"`/`"*"` for **every** event on that cam. Default `{"front_door": ["person"]}`.
  Edit the env in `docker-compose.yml` to add/change archived cams or event types —
  **no code change**. A malformed `ARCHIVE_RULES` logs once and disables the archive
  (fail-safe).
- **Auto-created subdirs:** there are **no pre-created dirs**. The per-cam
  subdir `<ARCHIVE_DIR>/<key>/` is created on first write via
  `os.makedirs(…, exist_ok=True)`, so adding or renaming a camera (new `stream_key`)
  just works the next time it fires a matching event.
- **Mount-safe sentinel guard:** the archive is active only when the marker
  file `ARCHIVE_MARKER` (default `.archive_root`) exists **inside** `ARCHIVE_DIR`.
  Create it once on the share: `touch /mnt/nas/wyze/.archive_root`. If the mount
  is down, docker auto-creates an empty *local* `/archive` that **lacks** the
  marker, so the archive disables (logs once, re-arms when the marker returns) and
  stills never land on local disk.
- **Storage:** any durable share works; bind-mount it into the container at
  `/archive` (see `docker-compose.yml`). An NFS export mounted via fstab with
  `_netdev,nofail` is a common choice.
- **Retention:** on each write, that cam's `<key>/` is pruned of `*.jpg` older than
  `ARCHIVE_RETENTION_DAYS` (default 100) by mtime (best-effort; a prune error is
  logged, never fatal).
- **Bind-ordering caveat:** the share must be **up before** the container
  starts — docker auto-creates an empty *local* dir if the source is absent. The
  marker guard above keeps writes off local disk, but to actually archive you still
  need the mount: an fstab `_netdev` entry handles this across reboots; after a
  manual outage, `mount -a` then `docker compose up -d`.

## Gemini vision analysis (optional)
The event listener can also **describe what the camera saw** and surface it as an
HA sensor. On a matching `wyze_camera_event`, the sidecar pulls a short
**multi-frame burst** from the already-warm go2rtc stream (default 4 frames ~1.5s
apart), sends them in one request to **Gemini** (`gemini-2.5-flash`) for a
structured description, and publishes the result to an MQTT-discovery sensor
`sensor.wyze_<key>_vision`. The burst (not a single still) is the point: it lets
the model report **what CHANGES across the frames** — who moved, in which
direction, what they were doing — which a lone frame usually misses.

- **Opt-in / secretless default:** active only when `GEMINI_API_KEY` is set **and**
  the MQTT publisher is configured (the sensor rides the same broker). Unset
  either ⇒ logs `vision disabled …` and the rest of the sidecar is unaffected. Use
  a **paid-tier** AI Studio key — the free tier trains on your images. Supply the
  key via the gitignored `.env`.
- **Sensor shape:** state is the **event timestamp** (`device_class: timestamp`);
  the description lives in the entity **attributes** — `change_detected` (did
  anything differ from the background), `summary`, `description`, `motion` (the
  cross-frame narrative), `people` count, the `*_present` booleans
  (person/package/vehicle/pet), optional `notable`, plus meta (`camera`, `model`,
  `frames`, `analyzed_at`). The discovery config is published **once per run**
  (lazy, on first result); state + attributes are retained so HA shows the last
  result across restarts.
- **Recurring-background suppression (`VISION_BASELINE=1`, default on):**
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

## Deploy
The image is **built where it runs** (`docker compose build` uses `build: .`),
so deploying is just copying the build context to the host that runs your Home
Assistant container, then bringing it up:
```bash
# from the root of this repo (HA_HOST = the host running Home Assistant):
scp Dockerfile requirements.txt snapshot.py docker-compose.yml \
    user@HA_HOST:~/wyze-vision/
# (optional) enable the MQTT bridge / event grab / Gemini vision: write the
# secrets to a gitignored .env on the host (see .env.example):
ssh user@HA_HOST 'umask 077; printf "MQTT_PASS=%s\n" "<broker-pw>" > ~/wyze-vision/.env'
ssh user@HA_HOST 'cd ~/wyze-vision && docker compose up -d --build'
ssh user@HA_HOST 'docker logs -f wyze-vision'   # watch the first cycle
```
The HA-side artifacts (`cameras-dashboard.yaml`, `hide_live_wyze_cams.sh`,
`provision_local_file_cameras.sh`) live under `deploy/` — they configure the HA
side and are not part of the container build.

The container only writes JPEGs; it never touches Home Assistant's Lovelace
config. First create the camera entities, then build the dashboard one of two ways:

- `deploy/provision_local_file_cameras.sh` creates the `local_file` camera entities
  (`camera.wyze_<key>_snapshot`) that read those JPEGs. Run it once (admin token).

**Build the dashboard automatically (recommended).**
`deploy/build_cameras_dashboard.py` discovers your real `camera.wyze_<key>_snapshot`
tiles from HA's `/api/states`, splits them Live/Offline by each live `camera.<key>`
state, and pushes a **storage-mode "Cameras" dashboard** into the HA sidebar over the
WebSocket API — no `configuration.yaml` edit, no restart, no placeholder editing:
```bash
HA_URL=http://homeassistant.local:8123 HA_TOKEN=<admin-token> \
  .venv/bin/python deploy/build_cameras_dashboard.py
```
It creates a **named** dashboard (`url_path` `wyze-cameras`, its own sidebar entry),
so it never touches your main/overview dashboard. It is idempotent — re-run it
whenever your cameras change and it **overwrites** that dashboard's config.
Requires an **admin** long-lived token (creating a dashboard is admin-only) and the
`websockets` package (`pip install websockets`, already pinned in `requirements.txt`).
Online/offline is read from each live `camera.<key>` at push time (a cam whose live
entity is `unavailable`/`off`/`unknown`, or absent, lands accordingly). Override the
defaults with `DASH_URL_PATH` / `DASH_TITLE` / `DASH_ICON`.

**Or hand-edit a YAML dashboard.**
`deploy/cameras-dashboard.yaml` is a **static example** — copy it to
`/config/dashboards/cameras.yaml` and reference it from `configuration.yaml` under
`lovelace: dashboards:`. The camera keys in it are **placeholders** you replace with
your own, and the Live/Offline split is frozen at the last-known state when you write
the file.

## Development / tests
The pure logic (stream-key/URL normalization, event-label matching, archive/vision
rule gating, the event-screenshot fetch and Gemini prompt assembly) has a focused
pytest suite under `tests/` that mocks `requests` and makes no network calls:
```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements-dev.txt
.venv/bin/python -m pytest
```
`requirements.txt` pins the runtime deps to the deployed versions;
`import snapshot` needs only `requests`/`Pillow`/`wyzeapy` (`paho.mqtt`/`websockets`
are imported lazily).
The `.env` is only needed to enable the optional features — the MQTT bridge
(`MQTT_PASS`), the event-driven grab (`HA_TOKEN`), and/or Gemini vision
(`GEMINI_API_KEY`, which also needs `MQTT_PASS`); see `.env.example`.

## Manage
```bash
ssh user@HA_HOST 'docker {logs,restart} wyze-vision'
```
