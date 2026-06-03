"""Wyze still-image snapshot sidecar (Hassio-8qm).

Periodically pulls a still JPEG from every ONLINE Wyze camera over the Amazon
Kinesis Video Streams (KVS) WebRTC cloud path -- the only reliable Wyze still
source here, since every local path is blocked (symmetric NAT / WebRTC-only
camera_proxy). See memories: wyze-go2rtc-still-proof, go2rtc-supports-kvs-webrtc.

Each cycle:
  1. read the Wyze tokens from HA's core.config_entries (no password needed),
  2. enumerate cameras via wyzeapy and call get_stream_info on each,
  3. build a go2rtc kinesis source line per online camera and write go2rtc.yaml,
  4. (re)start the go2rtc child process,
  5. GET http://127.0.0.1:1984/api/frame.jpeg?src=<cam> for each and atomically
     write /config/wyze_snapshots/<cam>.jpg.

Offline cameras (get_stream_info raises / returns nothing) are skipped without
aborting the cycle. Fresh KVS creds are minted every cycle, so the loop period
MUST stay under the KVS X-Amz-Expires=1800s window.
"""
import asyncio
import io
import json
import os
import re
import signal
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone

import requests
from PIL import Image, ImageDraw, ImageFont
from wyzeapy.services.camera_service import CameraService
from wyzeapy.wyze_auth_lib import Token, WyzeAuthLib


# --- config (env-overridable) ------------------------------------------------
CONFIG_ENTRIES = os.environ.get(
    "CONFIG_ENTRIES", "/config/.storage/core.config_entries"
)
OUT_DIR = os.environ.get("OUT_DIR", "/config/wyze_snapshots")
REFRESH_SECONDS = int(os.environ.get("REFRESH_SECONDS", "600"))
GO2RTC_BIN = os.environ.get("GO2RTC_BIN", "/usr/local/bin/go2rtc")
GO2RTC_YAML = os.environ.get("GO2RTC_YAML", "/tmp/go2rtc.yaml")
GO2RTC_API = os.environ.get("GO2RTC_API", "http://127.0.0.1:1984")
# Seconds to let go2rtc settle after (re)start before pulling frames.
GO2RTC_SETTLE = int(os.environ.get("GO2RTC_SETTLE", "5"))
# Per-camera frame.jpeg fetch: KVS connect is on-demand and takes a few seconds.
FRAME_TIMEOUT = int(os.environ.get("FRAME_TIMEOUT", "30"))
FRAME_ATTEMPTS = int(os.environ.get("FRAME_ATTEMPTS", "3"))
# go2rtc passes this verbatim as the signaling recipientClientId. It does NOT
# need to match the X-Amz-ClientId in the signaling URL (proven, Hassio-b5l).
CLIENT_ID = os.environ.get("CLIENT_ID", "ada06f08-87f4-4e13-b699-e82db8517ae5")
# Persisted per-camera online history, used to date the "offline since" label.
STATE_FILE = os.environ.get("STATE_FILE", os.path.join(OUT_DIR, ".offline_state.json"))

# Optional MQTT publish of each camera's Wyze cloud connection state, consumed
# by the device-inventory sidecar to date its Wyze tab "Last Seen" from the
# durable cloud conn_state_ts instead of HA's restart-pinned last_updated
# (Hassio-708). Opt-in: the publisher only activates when all of MQTT_HOST,
# MQTT_USER and MQTT_PASS are set, so this sidecar stays secretless by default.
MQTT_HOST = os.environ.get("MQTT_HOST", "")
MQTT_PORT = int(os.environ.get("MQTT_PORT", "1883"))
MQTT_USER = os.environ.get("MQTT_USER", "")
MQTT_PASS = os.environ.get("MQTT_PASS", "")

# Optional event-driven fast path (Hassio-i0w): subscribe to HA's
# `wyze_camera_event` bus event and grab a fresh still for that single camera
# the moment it fires, instead of waiting up to REFRESH_SECONDS for the next
# periodic cycle. Opt-in: the listener thread starts only when HA_TOKEN is set,
# so this sidecar stays secretless by default (same pattern as the MQTT bridge).
HA_URL = os.environ.get("HA_URL", "ws://127.0.0.1:8123/api/websocket")
HA_TOKEN = os.environ.get("HA_TOKEN", "")
EVENT_TYPE = os.environ.get("EVENT_TYPE", "wyze_camera_event")
# Minimum seconds between event grabs for the SAME camera (debounce a motion
# burst). The periodic cycle is unaffected.
EVENT_MIN_INTERVAL = int(os.environ.get("EVENT_MIN_INTERVAL", "15"))

# Optional front-porch person archive (Hassio-5sa): on a PERSON event
# (tag_list contains 101) for the front porch (stream_key PORCH_KEY), copy the
# current on-disk still to a timestamped file under ARCHIVE_DIR/<key>/, pruned
# to ARCHIVE_RETENTION_DAYS. Opt-in: active only when ARCHIVE_DIR is a mounted
# dir (a bind mount of the NFS ZFS share); absent -> the sidecar runs unchanged.
ARCHIVE_DIR = os.environ.get("ARCHIVE_DIR", "/archive")
PORCH_KEY = os.environ.get("PORCH_KEY", "front_door")
PERSON_TAG = os.environ.get("PERSON_TAG", "101")
ARCHIVE_RETENTION_DAYS = int(os.environ.get("ARCHIVE_RETENTION_DAYS", "100"))

# --- shared state between the periodic cycle and the event listener ----------
# Serialises go2rtc.restart() (in cycle()) against an event_grab()'s fetch_frame
# so an on-event grab never races a config swap / process restart.
_go2rtc_lock = threading.Lock()
# Stream keys with a live go2rtc source as of the last periodic cycle. Reassigned
# (atomic ref swap) each cycle right after write_yaml(); read by event_grab() to
# skip cams that have no stream (offline at the last cycle -> next cycle picks up).
current_streams = set()
# key -> monotonic time of the last successful/attempted event grab, for debounce.
_last_grab = {}
# Set once we've logged that the porch archive is disabled (no /archive mount),
# so an event storm doesn't repeat the line every event.
_archive_warned = False


def log(msg):
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)


def stream_key(nickname):
    """Camera nickname -> safe go2rtc stream key / filename stem.

    'Front Door' -> 'front_door', '3D Printer' -> '3d_printer'. Collapses any
    run of non-alphanumeric chars to a single underscore and lowercases, so the
    key is safe in YAML keys, URLs and filenames.
    """
    key = re.sub(r"[^a-z0-9]+", "_", (nickname or "").strip().lower())
    return key.strip("_")


def load_tokens():
    """Pull the wyzeapi access/refresh tokens out of HA's config entries."""
    with open(CONFIG_ENTRIES) as f:
        data = json.load(f)
    for entry in data["data"]["entries"]:
        if entry.get("domain") == "wyzeapi":
            d = entry["data"]
            return d["access_token"], d["refresh_token"]
    raise RuntimeError("no wyzeapi config entry found in core.config_entries")


def undouble(url):
    """KVS signaling URLs come percent-double-encoded (%25 -> %). Undo it."""
    for _ in range(3):
        if "%25" not in url:
            break
        url = url.replace("%25", "%")
    return url


def build_source(cfg):
    """Assemble a go2rtc kinesis 'webrtc:' source line from get_stream_info."""
    sig = undouble(cfg["signaling_url"])
    ice = json.dumps(cfg["ice_servers"], separators=(",", ":"))
    src = f"webrtc:{sig}#format=kinesis#client_id={CLIENT_ID}#ice_servers={ice}"
    if "'" in src:
        # Single-quoting in YAML would break; KVS URLs never contain one, but
        # guard rather than emit invalid YAML.
        raise ValueError("single-quote in source line would break YAML")
    return src


def wyze_offline_since(cam):
    """Cloud-authoritative epoch (seconds) a camera went offline, or None.

    Wyze's device list carries conn_state (1=online, 0=offline) and
    conn_state_ts (epoch MILLIseconds of the last connection-state change).
    For an offline cam (conn_state==0) that transition time IS when it dropped
    offline -- a real "offline since". Only trust it when the cloud also reports
    the cam offline; if conn_state==1 the timestamp is the came-online time and
    is meaningless as an offline date. (There is no `last_seen` field on the
    camera device list -- it is always null; conn_state_ts is the equivalent.)
    """
    rd = getattr(cam, "raw_dict", None) or {}
    if rd.get("conn_state") == 0:
        cts = rd.get("conn_state_ts")
        if cts:
            return int(cts // 1000)
    return None


async def collect_streams():
    """Enumerate cameras into online streams and offline placeholders.

    Returns (streams, offline, conn):
      streams  = {stream_key: go2rtc_source_line} for reachable (online) cams,
      offline  = {stream_key: (nickname, since_epoch|None)} for offline cams,
                 where since_epoch is the Wyze cloud offline-transition time,
      conn     = {mac(lower,no-colon): (conn_state, conn_state_ts_ms)} for every
                 camera that reports a conn_state, for the optional MQTT bridge.
    """
    access, refresh = load_tokens()
    auth = await WyzeAuthLib.create(token=Token(access, refresh, time.time() + 1e5))
    service = CameraService(auth)
    await service._auth_lib.refresh_if_should()

    cameras = await service.get_cameras()
    log(f"discovered {len(cameras)} cameras")

    streams = {}
    offline = {}
    conn = {}
    for cam in cameras:
        # Cloud connection state for the MQTT bridge (Hassio-708) -- collected
        # for every camera regardless of whether it yields a usable stream.
        rd = getattr(cam, "raw_dict", None) or {}
        mac = getattr(cam, "mac", "") or ""
        if mac and rd.get("conn_state") is not None:
            conn[mac.lower().replace(":", "")] = (
                int(rd["conn_state"]), rd.get("conn_state_ts")
            )
        key = stream_key(cam.nickname)
        if not key:
            log(f"  skip camera with empty nickname (mac={getattr(cam, 'mac', '?')})")
            continue
        try:
            cfg = await service.get_stream_info(cam)
        except Exception as exc:
            # NB: the wyzeapy exception text embeds the full get_stream_info
            # payload (transient KVS auth_token / AWS security token); never log
            # it. Only inspect it to classify offline vs other errors.
            if "offline" in str(exc).lower():
                offline[key] = (cam.nickname or key, wyze_offline_since(cam))
                log(f"  offline {key}")
            else:
                log(f"  skip {key}: get_stream_info error ({type(exc).__name__})")
            continue
        if not cfg or "signaling_url" not in cfg:
            offline[key] = (cam.nickname or key, wyze_offline_since(cam))
            log(f"  offline {key} (no signaling_url)")
            continue
        try:
            streams[key] = build_source(cfg)
        except Exception as exc:
            log(f"  skip {key}: bad stream info ({exc})")
            continue
        log(f"  online {key}")
    return streams, offline, conn


def write_yaml(streams):
    lines = [
        "api:",
        '  listen: ":1984"',
        "log:",
        "  level: info",
        "streams:",
    ]
    for key, src in streams.items():
        lines.append(f"  {key}: '{src}'")
    with open(GO2RTC_YAML, "w") as f:
        f.write("\n".join(lines) + "\n")


class Go2rtc:
    """Manage the go2rtc child process: (re)start and stop."""

    def __init__(self):
        self.proc = None

    def restart(self):
        self.stop()
        log("starting go2rtc child")
        self.proc = subprocess.Popen([GO2RTC_BIN, "-config", GO2RTC_YAML])

    def stop(self):
        if self.proc and self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait()
        self.proc = None


class StatusPublisher:
    """Optional retained MQTT publish of each camera's Wyze cloud conn-state.

    When MQTT_HOST/USER/PASS are all set, publishes a retained
    `wyze/<mac>/status` message ({"conn_state", "conn_state_ts"}) per camera,
    consumed by the device-inventory sidecar to date its Wyze tab "Last Seen"
    from the durable cloud `conn_state_ts` rather than HA's restart-pinned
    `last_updated` (Hassio-708). Disabled (no-op) when unconfigured so this
    sidecar stays secretless by default.
    """

    def __init__(self):
        self.client = None
        if not (MQTT_HOST and MQTT_USER and MQTT_PASS):
            log("mqtt publish disabled (MQTT_HOST/USER/PASS not all set)")
            return
        try:
            import paho.mqtt.client as mqtt
        except ImportError:
            log("mqtt publish disabled (paho-mqtt not installed)")
            return
        c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="wyze-snapshot")
        c.username_pw_set(MQTT_USER, MQTT_PASS)
        c.reconnect_delay_set(min_delay=1, max_delay=60)
        c.connect_async(MQTT_HOST, MQTT_PORT, keepalive=60)
        c.loop_start()
        self.client = c
        log(f"mqtt publish enabled -> {MQTT_HOST}:{MQTT_PORT} as {MQTT_USER}")

    def publish(self, conn):
        """Publish retained status for each {mac: (conn_state, conn_state_ts)}."""
        if not self.client:
            return
        for mac, (state, ts) in conn.items():
            payload = json.dumps({"conn_state": state, "conn_state_ts": ts})
            self.client.publish(f"wyze/{mac}/status", payload, qos=1, retain=True)
        if conn:
            log(f"  mqtt published status for {len(conn)} cams")

    def stop(self):
        if self.client:
            self.client.loop_stop()
            self.client.disconnect()
            self.client = None


def fetch_frame(key):
    """Pull a JPEG for one stream, retrying while go2rtc establishes KVS."""
    url = f"{GO2RTC_API}/api/frame.jpeg?src={key}"
    last_err = None
    for attempt in range(1, FRAME_ATTEMPTS + 1):
        try:
            resp = requests.get(url, timeout=FRAME_TIMEOUT)
            if resp.status_code == 200 and resp.content[:2] == b"\xff\xd8":
                return resp.content
            last_err = f"http={resp.status_code} len={len(resp.content)}"
        except Exception as exc:
            last_err = str(exc)
        if attempt < FRAME_ATTEMPTS:
            time.sleep(3)
    log(f"  frame {key} failed after {FRAME_ATTEMPTS} attempts: {last_err}")
    return None


def write_frame(key, jpeg):
    """Atomically write the JPEG so HA never reads a half-written file."""
    dst = os.path.join(OUT_DIR, f"{key}.jpg")
    tmp = f"{dst}.tmp"
    with open(tmp, "wb") as f:
        f.write(jpeg)
    os.replace(tmp, dst)


def load_state():
    try:
        with open(STATE_FILE) as f:
            return json.load(f)
    except (FileNotFoundError, ValueError):
        return {}


def save_state(state):
    tmp = f"{STATE_FILE}.tmp"
    with open(tmp, "w") as f:
        json.dump(state, f)
    os.replace(tmp, STATE_FILE)


def _font(size):
    """Scalable default font (Pillow >=10.1); fall back to the bitmap default."""
    try:
        return ImageFont.load_default(size=size)
    except TypeError:
        return ImageFont.load_default()


def render_offline(nickname, since_epoch):
    """Render an 'offline since <date>' placeholder JPEG (bytes)."""
    width, height = 1280, 720
    img = Image.new("RGB", (width, height), (28, 28, 30))
    draw = ImageDraw.Draw(img)

    title = nickname or "Camera"
    when = time.strftime("%Y-%m-%d %H:%M UTC", time.gmtime(since_epoch))
    sub = f"offline since {when}"

    lines = [(title, _font(72), (235, 235, 235)), (sub, _font(40), (200, 120, 120))]
    heights = []
    for text, font, _ in lines:
        bbox = draw.textbbox((0, 0), text, font=font)
        heights.append(bbox[3] - bbox[1])
    gap = 24
    total = sum(heights) + gap
    y = (height - total) // 2
    for (text, font, color), th in zip(lines, heights):
        bbox = draw.textbbox((0, 0), text, font=font)
        tw = bbox[2] - bbox[0]
        draw.text(((width - tw) // 2 - bbox[0], y - bbox[1]), text, font=font, fill=color)
        y += th + gap

    buf = io.BytesIO()
    img.save(buf, format="JPEG", quality=85)
    return buf.getvalue()


def cycle(go2rtc, publisher):
    streams, offline, conn = asyncio.run(collect_streams())
    publisher.publish(conn)
    now = int(time.time())
    state = load_state()

    # Online cameras: pull a live frame, and record that we saw them online.
    ok = 0
    if streams:
        write_yaml(streams)
        # Publish the live stream set for the event listener (atomic ref swap),
        # then restart go2rtc under the lock so an event grab can't race it.
        global current_streams
        current_streams = set(streams)
        with _go2rtc_lock:
            go2rtc.restart()
        time.sleep(GO2RTC_SETTLE)
        for key in streams:
            jpeg = fetch_frame(key)
            if jpeg:
                write_frame(key, jpeg)
                state[key] = {"last_online": now, "offline_since": None}
                ok += 1
                log(f"  wrote {key}.jpg ({len(jpeg)} bytes)")
    else:
        log("no online cameras this cycle")

    # Offline cameras: render an "offline since <date>" placeholder. The "since"
    # date prefers Wyze's cloud offline-transition time (conn_state_ts), which is
    # accurate for cams that were offline long before this sidecar started. When
    # the cloud timestamp is absent it falls back to the last time we saw the cam
    # online, then to the first time we observed it offline.
    placeholders = 0
    for key, (nickname, wyze_since) in offline.items():
        prev = state.get(key, {})
        last_online = prev.get("last_online")
        offline_since = prev.get("offline_since") or now
        since = wyze_since or last_online or offline_since
        state[key] = {"last_online": last_online, "offline_since": offline_since}
        try:
            write_frame(key, render_offline(nickname, since))
            placeholders += 1
        except Exception as exc:
            log(f"  placeholder {key} failed: {exc!r}")

    save_state(state)
    log(
        f"cycle done: {ok}/{len(streams)} snapshots, "
        f"{placeholders}/{len(offline)} offline placeholders"
    )


def event_grab(key):
    """Grab a single fresh frame for `key` in response to a camera event.

    Runs on the event listener's executor thread (blocking). Steps:
      1. debounce -- skip if we grabbed this key < EVENT_MIN_INTERVAL ago;
      2. membership -- skip+log if the cam had no live stream at the last cycle
         (offline then -> no go2rtc source; the next periodic cycle picks it up);
      3. fetch one frame under _go2rtc_lock (serialised against go2rtc.restart()).
    Writes ONLY the JPEG, not .offline_state.json -- the periodic cycle owns that
    bookkeeping and the offline date prefers the cloud-authoritative conn_state_ts.
    """
    now = time.monotonic()
    last = _last_grab.get(key, 0)
    if now - last < EVENT_MIN_INTERVAL:
        log(f"event grab {key}: debounced ({now - last:.0f}s < {EVENT_MIN_INTERVAL}s)")
        return
    if key not in current_streams:
        log(f"event grab {key}: skipped (no live stream this cycle)")
        return
    _last_grab[key] = now
    with _go2rtc_lock:
        jpeg = fetch_frame(key)
    if jpeg:
        write_frame(key, jpeg)
        log(f"event grab {key}: wrote {key}.jpg ({len(jpeg)} bytes)")
    else:
        log(f"event grab {key}: no frame")


def is_person(data):
    """True if a wyze_camera_event payload classifies as a person detection.

    The Cam Plus AI object class rides in tag_list as integer codes (101=Person;
    memory wyze-event-tag-list-mapping); a named ai_tag_list ('person') may also
    appear on Cam-Plus accounts. Compare as strings so an int 101 or a str "101"
    both match.
    """
    tags = [str(t) for t in (data.get("tag_list") or [])]
    ai = [str(x).lower() for x in (data.get("ai_tag_list") or [])]
    return (PERSON_TAG in tags) or ("person" in ai)


def prune_archive(dstdir):
    """Remove archived *.jpg older than ARCHIVE_RETENTION_DAYS by mtime.

    Best-effort: any error is logged and swallowed so a prune failure can never
    kill the event listener.
    """
    try:
        cutoff = time.time() - ARCHIVE_RETENTION_DAYS * 86400
        removed = 0
        for name in os.listdir(dstdir):
            if not name.endswith(".jpg"):
                continue
            path = os.path.join(dstdir, name)
            try:
                if os.path.getmtime(path) < cutoff:
                    os.remove(path)
                    removed += 1
            except FileNotFoundError:
                pass
        if removed:
            log(f"porch archive: pruned {removed} still(s) older than "
                f"{ARCHIVE_RETENTION_DAYS}d from {os.path.basename(dstdir)}/")
    except Exception as exc:
        log(f"porch archive prune error ({exc!r})")


def archive_porch_still(key):
    """Copy the current on-disk still for `key` into the dated archive.

    Opt-in: when ARCHIVE_DIR is not a mounted dir the archive is disabled and we
    log once then return, so the sidecar runs unchanged with no /archive bind.
    The current OUT_DIR/<key>.jpg is copied (read bytes -> write .tmp ->
    os.replace) to ARCHIVE_DIR/<key>/<UTC-timestamp>.jpg with microseconds in the
    name to avoid same-second collisions, then the dir is pruned to retention.
    """
    global _archive_warned
    if not os.path.isdir(ARCHIVE_DIR):
        if not _archive_warned:
            log(f"porch archive disabled (ARCHIVE_DIR {ARCHIVE_DIR} not a mounted dir)")
            _archive_warned = True
        return
    src = os.path.join(OUT_DIR, f"{key}.jpg")
    if not os.path.exists(src):
        log(f"porch archive {key}: skip (no {src} yet)")
        return
    dstdir = os.path.join(ARCHIVE_DIR, key)
    os.makedirs(dstdir, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S_%fZ")
    dst = os.path.join(dstdir, f"{ts}.jpg")
    tmp = f"{dst}.tmp"
    try:
        with open(src, "rb") as f:
            jpeg = f.read()
        with open(tmp, "wb") as f:
            f.write(jpeg)
        os.replace(tmp, dst)
        log(f"porch archive wrote wyze/{key}/{ts}.jpg ({len(jpeg)} bytes)")
    except Exception as exc:
        log(f"porch archive {key} failed: {exc!r}")
        return
    prune_archive(dstdir)


async def run_event_listener(stop):
    """Subscribe to HA's `wyze_camera_event` and grab a still per event.

    Mirrors wyze-event-catalog/watcher.py's websocket handshake. On each event
    it maps device_name -> stream_key and runs the blocking event_grab on the
    default executor so the websocket stays responsive to ping/pong and stop.
    Reconnects with exponential backoff 1->60s. Started only when HA_TOKEN is set.
    """
    import websockets

    loop = asyncio.get_running_loop()
    backoff = 1
    while not stop["flag"]:
        try:
            async with websockets.connect(
                HA_URL, max_size=None, ping_interval=20
            ) as ws:
                msg = json.loads(await ws.recv())
                if msg.get("type") != "auth_required":
                    raise RuntimeError(f"unexpected first frame: {msg}")
                await ws.send(json.dumps({"type": "auth", "access_token": HA_TOKEN}))
                msg = json.loads(await ws.recv())
                if msg.get("type") != "auth_ok":
                    raise RuntimeError(f"auth failed: {msg}")
                log("event listener: authenticated")

                await ws.send(json.dumps({
                    "id": 1, "type": "subscribe_events", "event_type": EVENT_TYPE,
                }))
                msg = json.loads(await ws.recv())
                if not msg.get("success"):
                    raise RuntimeError(f"subscribe failed: {msg}")
                log(f"event listener: subscribed to '{EVENT_TYPE}'")
                backoff = 1

                while not stop["flag"]:
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=30)
                    except asyncio.TimeoutError:
                        continue  # stay responsive to stop["flag"]
                    msg = json.loads(raw)
                    if msg.get("type") != "event":
                        continue
                    data = msg.get("event", {}).get("data", {})
                    key = stream_key(data.get("device_name"))
                    if not key:
                        continue
                    await loop.run_in_executor(None, event_grab, key)
                    # Front-porch person archive (Hassio-5sa): on a PERSON event
                    # for the porch cam, snapshot the current still to the ZFS
                    # archive. Runs after the grab so it copies the freshest file.
                    if key == PORCH_KEY and is_person(data):
                        await loop.run_in_executor(None, archive_porch_still, key)
        except Exception as exc:
            if stop["flag"]:
                break
            log(f"event listener: connection error ({exc}); retry in {backoff}s")
            for _ in range(backoff):
                if stop["flag"]:
                    break
                await asyncio.sleep(1)
            backoff = min(backoff * 2, 60)
    log("event listener: stopped")


def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    go2rtc = Go2rtc()
    publisher = StatusPublisher()

    stop = {"flag": False}

    def handle(_signum, _frame):
        stop["flag"] = True

    signal.signal(signal.SIGTERM, handle)
    signal.signal(signal.SIGINT, handle)

    # Event-driven fast path (Hassio-i0w): opt-in via HA_TOKEN. The daemon thread
    # shares `stop` and dies on process exit (SIGTERM/SIGINT set stop["flag"]).
    if HA_TOKEN:
        threading.Thread(
            target=lambda: asyncio.run(run_event_listener(stop)), daemon=True
        ).start()
    else:
        log("event listener disabled (HA_TOKEN not set)")

    log(f"wyze-snapshot starting (refresh={REFRESH_SECONDS}s, out={OUT_DIR})")
    try:
        while not stop["flag"]:
            start = time.time()
            try:
                cycle(go2rtc, publisher)
            except Exception as exc:
                log(f"cycle error (continuing): {exc!r}")
            elapsed = time.time() - start
            sleep_for = max(1, REFRESH_SECONDS - int(elapsed))
            for _ in range(sleep_for):
                if stop["flag"]:
                    break
                time.sleep(1)
    finally:
        go2rtc.stop()
        publisher.stop()
        log("wyze-snapshot stopped")


if __name__ == "__main__":
    sys.exit(main())
