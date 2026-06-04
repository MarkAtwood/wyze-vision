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
import base64
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
# Resilience to a "sick" camera (Hassio-3hd): a cam whose go2rtc/KVS stream is
# timing out must not stall the whole event pipeline. Two layers:
#  (A) the listener hands each event to a bounded queue drained by EVENT_WORKERS
#      tasks, so ws.recv() never blocks on one cam's grab+burst (which would drop
#      events for healthy cams). EVENT_QUEUE_MAX caps backlog.
#  (B) the event path uses a TIGHT frame budget (the periodic cycle can wait out a
#      KVS cold-start, but a live event is latency-sensitive -- a dead stream
#      should fail in seconds, not minutes), bounds its wait for the go2rtc lock,
#      and skips cams that failed their last periodic grab (_frame_unhealthy).
EVENT_FRAME_TIMEOUT = int(os.environ.get("EVENT_FRAME_TIMEOUT", "8"))
EVENT_FRAME_ATTEMPTS = int(os.environ.get("EVENT_FRAME_ATTEMPTS", "1"))
EVENT_LOCK_TIMEOUT = int(os.environ.get("EVENT_LOCK_TIMEOUT", "20"))
EVENT_WORKERS = int(os.environ.get("EVENT_WORKERS", "3"))
EVENT_QUEUE_MAX = int(os.environ.get("EVENT_QUEUE_MAX", "64"))

# Wyze detection-media fast path (Hassio-zcm): the wyze_camera_event payload carries
# Wyze's OWN cloud-AI detection screenshot (event_screenshot), captured AT detection
# time -- so it contains the subject that triggered the event, unlike a live go2rtc
# grab taken seconds-to-tens-of-seconds later (ha-wyzeapi polls Wyze every 30s, then
# KVS connect adds more, so the subject has usually left frame). When this is on and
# the payload has a screenshot we use THAT as the event still (dashboard tile +
# archive + Gemini vision) and skip the live grab; we fall back to the live go2rtc
# grab/burst when it's absent or unfetchable. The URL is self-authenticating via its
# signed `st` token (NO Wyze access token needed), but Wyze's gateway only honours a
# recognised client User-Agent and returns a misleading 401 'Access token is invalid.'
# for unknown/bot UAs (Hassio-2ph) -- so we send EVENT_MEDIA_UA. (event_video is also
# in the payload but is typically a 404 for these cams -- no Cam Plus cloud clip --
# so only the still is used.) Set EVENT_USE_SCREENSHOT=0 to force the old live grab.
EVENT_USE_SCREENSHOT = os.environ.get("EVENT_USE_SCREENSHOT", "1") not in (
    "0", "false", "no", ""
)
EVENT_MEDIA_UA = os.environ.get("EVENT_MEDIA_UA", "okhttp/4.9.3")
EVENT_MEDIA_TIMEOUT = int(os.environ.get("EVENT_MEDIA_TIMEOUT", "10"))

# Optional event-still archive (Hassio-5sa / -bp8 / -ud0): on a matching
# wyze_camera_event, copy the current on-disk still to a timestamped file under
# ARCHIVE_DIR/<key>/, pruned to ARCHIVE_RETENTION_DAYS. Which cameras + event
# types to archive is data-driven via ARCHIVE_RULES (edit the docker-compose env,
# no code change). Per-cam subdirs are created on first write, so adding or
# renaming a camera needs no manual mkdir.
ARCHIVE_DIR = os.environ.get("ARCHIVE_DIR", "/archive")
# Sentinel proving ARCHIVE_DIR is the real mounted ZFS share, not a docker-made
# empty LOCAL bind dir from an NFS outage. Create it once on the share itself:
#   touch /mnt/tank/shared/wyze/.archive_root
# Missing marker -> archive disables (logs once) instead of writing to local disk.
ARCHIVE_MARKER = os.environ.get("ARCHIVE_MARKER", ".archive_root")
ARCHIVE_RETENTION_DAYS = int(os.environ.get("ARCHIVE_RETENTION_DAYS", "100"))

# Named AI event labels -> the integer tag_list codes Wyze emits (memory
# wyze-event-tag-list-mapping); ai_tag_list carries the same names. Used to match
# an event against the labels listed for a camera in ARCHIVE_RULES.
ARCHIVE_TAG_CODES = {
    "person": "101",
    "pet": "102",
    "vehicle": "103",
    "package": "104",
}


def _load_archive_rules():
    """Parse ARCHIVE_RULES JSON {stream_key: [label, ...]} from the env.

    Labels are lowercased. A bad/missing value disables the archive (empty dict)
    rather than crashing the sidecar. Default preserves the original behavior:
    archive front_door person events.
    """
    raw = os.environ.get("ARCHIVE_RULES", '{"front_door": ["person"]}')
    try:
        # json.loads raises ValueError on bad JSON; .items() raises AttributeError
        # if the top level parsed to a non-dict (e.g. a JSON list/number). Both are
        # caught below so a fat-fingered env can never crash the sidecar at import.
        parsed = json.loads(raw)
        # Normalise every label to lowercase so rule-matching (event_labels) is
        # case-insensitive; `labels or []` tolerates a null value for a key.
        return {
            key: [str(lbl).lower() for lbl in (labels or [])]
            for key, labels in parsed.items()
        }
    except (ValueError, AttributeError) as exc:
        # Empty dict => should_archive() always returns False => archive off.
        # print() not log(): log() is defined later in the file, but this runs at
        # import time (ARCHIVE_RULES = _load_archive_rules() below).
        print(f"[archive] bad ARCHIVE_RULES ({exc!r}); archive disabled", flush=True)
        return {}


ARCHIVE_RULES = _load_archive_rules()

# Optional Gemini vision analysis (Hassio-5sk): on a matching wyze_camera_event,
# grab a short multi-frame burst from the (already warm) go2rtc stream, send it to
# Gemini for a structured description, and publish the result to an MQTT-discovery
# HA sensor (sensor.wyze_<key>_vision). Opt-in: active only when GEMINI_API_KEY is
# set AND the MQTT publisher is configured (the sensor rides the same dokr broker
# HA already consumes), so the sidecar stays secretless by default.
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "")
VISION_MODEL = os.environ.get("VISION_MODEL", "gemini-2.5-flash")
# Gemini generateContent base. The API key rides in the query string, so the full
# request URL must NEVER be logged (analyze_with_gemini logs status + message only).
GEMINI_ENDPOINT = os.environ.get(
    "GEMINI_ENDPOINT", "https://generativelanguage.googleapis.com/v1beta/models"
)
GEMINI_TIMEOUT = int(os.environ.get("GEMINI_TIMEOUT", "30"))
# Burst geometry: VISION_FRAMES frames spaced VISION_FRAME_INTERVAL seconds apart,
# pulled from the live stream so the model sees motion ACROSS the burst (the whole
# point -- a single still often misses the moving subject). 4 x 1.5s ~= 4.5s span.
VISION_FRAMES = int(os.environ.get("VISION_FRAMES", "4"))
VISION_FRAME_INTERVAL = float(os.environ.get("VISION_FRAME_INTERVAL", "1.5"))
# Per-camera debounce: minimum seconds between Gemini analyses for the SAME cam, so
# a motion burst (many events in seconds) costs one analysis, not dozens.
VISION_MIN_INTERVAL = int(os.environ.get("VISION_MIN_INTERVAL", "30"))
# MQTT discovery prefix HA listens on (default 'homeassistant'); the per-cam sensor
# config is published retained under <prefix>/sensor/wyze_vision_<key>/config.
VISION_DISCOVERY_PREFIX = os.environ.get("VISION_DISCOVERY_PREFIX", "homeassistant")
# Recurring-background suppression (Hassio-i02): when on, the periodic cycle caches
# each online camera's ambient (timer-driven, usually-empty) still under BASELINE_DIR
# and the vision burst sends that as a labeled REFERENCE frame, so Gemini reports
# only what differs from the recurring background instead of re-describing the fixed
# scene every event. Self-maintaining (refreshed every cycle, so it tracks lighting/
# season). Default on; set VISION_BASELINE=0 to send the raw burst with no reference.
VISION_BASELINE = os.environ.get("VISION_BASELINE", "1") not in ("0", "false", "no", "")
BASELINE_DIR = os.environ.get("BASELINE_DIR", os.path.join(OUT_DIR, "baselines"))


def _load_vision_rules():
    """Parse VISION_RULES JSON {stream_key: [label, ...]} (same shape as
    ARCHIVE_RULES). A top-level '*' key matches ANY camera; a rule list of ['any']
    (or '*') matches any event for that cam. Default analyses every camera on every
    event ({"*": ["any"]}). Bad/missing JSON disables vision (empty dict) rather
    than crashing the sidecar at import.
    """
    raw = os.environ.get("VISION_RULES", '{"*": ["any"]}')
    try:
        parsed = json.loads(raw)
        return {
            key: [str(lbl).lower() for lbl in (labels or [])]
            for key, labels in parsed.items()
        }
    except (ValueError, AttributeError) as exc:
        print(f"[vision] bad VISION_RULES ({exc!r}); vision disabled", flush=True)
        return {}


VISION_RULES = _load_vision_rules()

# JSON object Gemini must fill (structured output via responseSchema). Keeps the
# sensor attributes stable + machine-usable: `summary` is short (sensor-friendly),
# `motion` is the cross-frame narrative (what CHANGED between the burst frames), and
# the *_present booleans drive automations. `notable` is optional (not required).
VISION_SCHEMA = {
    "type": "object",
    "properties": {
        "change_detected": {"type": "boolean"},
        "summary": {"type": "string"},
        "description": {"type": "string"},
        "motion": {"type": "string"},
        "people": {"type": "integer"},
        "person_present": {"type": "boolean"},
        "package_present": {"type": "boolean"},
        "vehicle_present": {"type": "boolean"},
        "pet_present": {"type": "boolean"},
        "notable": {"type": "string"},
    },
    "required": [
        "change_detected", "summary", "description", "motion", "people",
        "person_present", "package_present", "vehicle_present", "pet_present",
    ],
    "propertyOrdering": [
        "change_detected", "summary", "description", "motion", "people",
        "person_present", "package_present", "vehicle_present", "pet_present",
        "notable",
    ],
}

# No-reference prompt (VISION_BASELINE off, or no baseline cached yet): describe the
# burst on its own. change_detected is always true here (no baseline to compare to).
VISION_PROMPT = (
    "These image(s) are from a single home security camera named '{title}', captured "
    "during a motion event{label_hint}.{box_hint} If there are several, treat them as "
    "one time-ordered burst ~{interval}s apart, not separate scenes. Report: "
    "change_detected (always true here, there is no reference); a one-line summary; "
    "a fuller description of the scene; what is happening (and, across the frames if "
    "there are several, who/what moves, in which direction, what they are doing); "
    "counts and presence of people, packages, vehicles and pets; and anything notable "
    "or concerning. If nothing of interest is present, say so plainly."
)

# Reference-frame prompt (VISION_BASELINE on, baseline available): the recurring
# background is sent as image 1; the model reports only what DIFFERS from it. This
# is the answer to 'just tell us what it sees beyond the recurring background'.
VISION_PROMPT_BASELINE = (
    "You are analyzing a home security camera named '{title}'. The FIRST image is "
    "this camera's normal, EMPTY background with no event happening. The remaining "
    "image(s) were captured during a motion event{label_hint}; if there are several "
    "they are a time-ordered burst ~{interval}s apart.{box_hint} IGNORE everything that also "
    "appears in the background image: the building, fixed furniture, parked vehicles, "
    "plants, signage, and any lighting or day/night IR differences. Describe ONLY "
    "what is NEW, moving, or changed relative to the background -- who or what "
    "entered, where it is (and how it moves across the frames if there are several), "
    "and what it is doing. Counts and *_present flags must cover only things that are "
    "NOT part of the background. If the event image(s) are essentially identical to "
    "the background, set change_detected false, summary to 'no change', and every "
    "*_present flag false. Otherwise set change_detected true. Note anything "
    "concerning."
)

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
# Stream keys whose frame grab FAILED in the last periodic cycle (Hassio-3hd).
# Maintained by cycle(); read by the event path to skip a known-sick cam instead
# of discovering it the slow way (a dead KVS stream costs a full fetch budget).
# Mutated in place (add/discard) so no global declaration is needed.
_frame_unhealthy = set()
# key -> monotonic time of the last Gemini analysis, for VISION_MIN_INTERVAL.
_vision_last = {}
# Latch: True once we've logged that the archive is disabled (sentinel marker
# missing), so an event storm doesn't repeat the line every event. Reset to False
# by archive_event_still() the moment the marker reappears, so a later outage logs
# again. See archive_event_still() for the marker-guard logic.
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
        # Stream keys whose Gemini-vision discovery config we've already published
        # this run, so we announce each sensor to HA exactly once (lazy, on first
        # result) instead of every event.
        self._vision_announced = set()
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

    def announce_vision(self, key, title):
        """Publish (once per run) the MQTT-discovery config for a cam's vision sensor.

        HA auto-creates sensor.wyze_<key>_vision from this retained message. The
        sensor's state is the event timestamp (device_class timestamp); the
        descriptive Gemini fields live in its json_attributes. Idempotent: skipped
        if already announced this run.
        """
        if not self.client or key in self._vision_announced:
            return
        object_id = f"wyze_{key}_vision"
        disco_topic = f"{VISION_DISCOVERY_PREFIX}/sensor/wyze_vision_{key}/config"
        config = {
            "name": f"{title or key} Vision",
            "unique_id": object_id,
            "object_id": object_id,
            "state_topic": f"wyze/vision/{key}/state",
            "json_attributes_topic": f"wyze/vision/{key}/attributes",
            "device_class": "timestamp",
            "icon": "mdi:eye-check",
            # Group all vision sensors under one HA device alongside nothing else;
            # keeps the entities tidy without colliding with the conn-state topics.
            "device": {
                "identifiers": ["wyze_vision"],
                "name": "Wyze Vision",
                "manufacturer": "wyze-snapshot",
            },
        }
        self.client.publish(disco_topic, json.dumps(config), qos=1, retain=True)
        self._vision_announced.add(key)
        log(f"  vision: announced sensor {object_id} to HA discovery")

    def publish_vision(self, key, title, result, frame_count):
        """Publish a Gemini vision result to the cam's discovery sensor.

        State = ISO8601 'now' (the event time); attributes = the full structured
        result plus meta (model, frame count). Both retained so HA shows the last
        result across restarts. Announces the discovery config first (lazy).
        """
        if not self.client:
            return
        self.announce_vision(key, title)
        now_iso = datetime.now(timezone.utc).isoformat()
        attributes = dict(result)
        attributes.update({
            "camera": title or key,
            "model": VISION_MODEL,
            "frames": frame_count,
            "analyzed_at": now_iso,
        })
        self.client.publish(f"wyze/vision/{key}/state", now_iso, qos=1, retain=True)
        self.client.publish(
            f"wyze/vision/{key}/attributes", json.dumps(attributes),
            qos=1, retain=True,
        )

    def stop(self):
        if self.client:
            self.client.loop_stop()
            self.client.disconnect()
            self.client = None


def fetch_frame(key, timeout=None, attempts=None):
    """Pull a JPEG for one stream, retrying while go2rtc establishes KVS.

    timeout/attempts default to the patient periodic-cycle budget (FRAME_TIMEOUT x
    FRAME_ATTEMPTS); the event path passes the tighter EVENT_FRAME_* values so a
    dead stream fails in seconds (Hassio-3hd).
    """
    timeout = FRAME_TIMEOUT if timeout is None else timeout
    attempts = FRAME_ATTEMPTS if attempts is None else attempts
    url = f"{GO2RTC_API}/api/frame.jpeg?src={key}"
    last_err = None
    for attempt in range(1, attempts + 1):
        try:
            resp = requests.get(url, timeout=timeout)
            if resp.status_code == 200 and resp.content[:2] == b"\xff\xd8":
                return resp.content
            last_err = f"http={resp.status_code} len={len(resp.content)}"
        except Exception as exc:
            last_err = str(exc)
        if attempt < attempts:
            time.sleep(3)
    log(f"  frame {key} failed after {attempts} attempts: {last_err}")
    return None


def _fetch_frame_locked(key):
    """Event-path frame fetch (Hassio-3hd): grab one JPEG under the go2rtc lock
    using the TIGHT event budget, and bound the wait for the lock itself.

    The periodic cycle holds _go2rtc_lock briefly around go2rtc.restart(); a sick
    cam mid-fetch could otherwise hold it for the full fetch budget and pin the
    event workers. acquire(timeout) bails instead of blocking indefinitely.
    Returns the JPEG bytes or None.
    """
    if not _go2rtc_lock.acquire(timeout=EVENT_LOCK_TIMEOUT):
        log(f"  frame {key} skipped: go2rtc lock busy > {EVENT_LOCK_TIMEOUT}s")
        return None
    try:
        return fetch_frame(
            key, timeout=EVENT_FRAME_TIMEOUT, attempts=EVENT_FRAME_ATTEMPTS
        )
    finally:
        _go2rtc_lock.release()


def write_frame(key, jpeg):
    """Atomically write the JPEG so HA never reads a half-written file."""
    dst = os.path.join(OUT_DIR, f"{key}.jpg")
    tmp = f"{dst}.tmp"
    with open(tmp, "wb") as f:
        f.write(jpeg)
    os.replace(tmp, dst)


def write_baseline(key, jpeg):
    """Cache a camera's ambient frame as its vision background reference.

    Written by the periodic cycle (timer-driven, so usually the empty scene), NOT
    by event_grab -- so it stays a clean baseline even as the live <key>.jpg is
    overwritten on each event. Atomic, and best-effort: a failure just means this
    cycle keeps the prior baseline. No-op when VISION_BASELINE is off.
    """
    if not VISION_BASELINE:
        return
    dst = os.path.join(BASELINE_DIR, f"{key}.jpg")
    tmp = f"{dst}.tmp"
    try:
        with open(tmp, "wb") as f:
            f.write(jpeg)
        os.replace(tmp, dst)
    except Exception as exc:
        log(f"  baseline {key} write failed: {exc!r}")


def load_baseline(key):
    """Return the cached background-reference JPEG for `key`, or None.

    None when baselines are off, the cam has no cached ambient frame yet (e.g. it
    just came online), or the read fails -- callers then analyse with no reference.
    """
    if not VISION_BASELINE:
        return None
    path = os.path.join(BASELINE_DIR, f"{key}.jpg")
    try:
        with open(path, "rb") as f:
            return f.read()
    except (FileNotFoundError, OSError):
        return None


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
                # Refresh this cam's vision background reference (Hassio-i02). The
                # periodic still is timer-driven (no event), so it's our cleanest
                # ambient frame; updating every cycle tracks lighting/season.
                write_baseline(key, jpeg)
                state[key] = {"last_online": now, "offline_since": None}
                ok += 1
                # This cam's stream is healthy this cycle -> let the event path
                # use it again (Hassio-3hd).
                _frame_unhealthy.discard(key)
                log(f"  wrote {key}.jpg ({len(jpeg)} bytes)")
            else:
                # Frame grab failed (KVS/go2rtc trouble): mark the cam sick so the
                # event path skips it until a later cycle recovers it, instead of
                # spending the full fetch budget on a dead stream (Hassio-3hd).
                _frame_unhealthy.add(key)
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


def fetch_event_screenshot(data):
    """Fetch Wyze's own detection screenshot from a wyze_camera_event payload.

    Returns the JPEG bytes, or None if the payload has no screenshot URL or the
    fetch fails. The URL (host prod-sight-safe-auth.wyze.com) is self-authenticating
    via its signed `st` query token -- NO Wyze access token is sent. Wyze's gateway
    runs a User-Agent allowlist and answers a misleading 401 'Access token is
    invalid.' for unknown/bot UAs, so we send EVENT_MEDIA_UA (a recognised client
    UA). We send NO Authorization header (the Azure-blob backend 400s on one). See
    Hassio-2ph for the reverse-engineering of this auth path.
    """
    url = (data or {}).get("event_screenshot")
    if not url:
        return None
    try:
        r = requests.get(
            url,
            headers={"User-Agent": EVENT_MEDIA_UA},
            timeout=EVENT_MEDIA_TIMEOUT,
        )
    except Exception as exc:
        log(f"event screenshot fetch failed: {exc!r}")
        return None
    if r.status_code != 200:
        log(f"event screenshot fetch: HTTP {r.status_code}")
        return None
    jpeg = r.content
    if not jpeg or jpeg[:2] != b"\xff\xd8":
        log(f"event screenshot fetch: not a JPEG ({len(jpeg)} bytes)")
        return None
    return jpeg


def use_event_screenshot(key, jpeg):
    """Publish Wyze's detection screenshot as the event still for `key`.

    Runs on the event listener's executor thread. Writes ONLY the JPEG (same as
    event_grab) and shares its EVENT_MIN_INTERVAL debounce + _last_grab clock, so a
    screenshot and a live grab never double-write within the window. Does NOT touch
    go2rtc and does NOT write a baseline (the baseline is the recurring EMPTY scene,
    captured by the periodic cycle -- an event screenshot is the opposite of that).
    """
    now = time.monotonic()
    last = _last_grab.get(key, 0)
    if now - last < EVENT_MIN_INTERVAL:
        log(f"event screenshot {key}: debounced ({now - last:.0f}s < {EVENT_MIN_INTERVAL}s)")
        return
    _last_grab[key] = now
    write_frame(key, jpeg)
    log(f"event screenshot {key}: wrote {key}.jpg ({len(jpeg)} bytes)")


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
    if key in _frame_unhealthy:
        log(f"event grab {key}: skipped (stream unhealthy at last cycle)")
        return
    _last_grab[key] = now
    jpeg = _fetch_frame_locked(key)
    if jpeg:
        write_frame(key, jpeg)
        log(f"event grab {key}: wrote {key}.jpg ({len(jpeg)} bytes)")
    else:
        log(f"event grab {key}: no frame")


def event_labels(data):
    """Set of archive labels a wyze_camera_event payload matches.

    The Cam Plus AI object class rides in tag_list as integer codes (101=Person,
    memory wyze-event-tag-list-mapping) and/or as named strings in ai_tag_list.
    Returns the lowercased label names (person/pet/vehicle/package) present, by
    matching both the named ai_tag_list and the ARCHIVE_TAG_CODES-mapped tag_list.
    """
    # Two parallel signals from the payload, normalised to string sets so we can do
    # set membership below regardless of how Wyze typed them:
    #   tag_list   -> integer codes, stringified ("101")     -> compare vs code
    #   ai_tag_list-> names, lowercased ("person")           -> compare vs label
    # `or []` guards a missing/null field (not every event carries both).
    tags = {str(t) for t in (data.get("tag_list") or [])}
    ai = {str(x).lower() for x in (data.get("ai_tag_list") or [])}
    # A label is "present" if EITHER signal names it: its mapped code is in tag_list
    # OR its name is in ai_tag_list. Belt-and-suspenders since cams differ in which
    # of the two they populate.
    return {
        label
        for label, code in ARCHIVE_TAG_CODES.items()
        if code in tags or label in ai
    }


def should_archive(key, data):
    """True if `key` is configured in ARCHIVE_RULES and the event matches.

    A rule list of ['any'] (or '*') archives every event for that camera;
    otherwise the event must carry one of the configured labels.
    """
    # wanted = the label list configured for this cam, or None if the cam isn't in
    # ARCHIVE_RULES at all. `not wanted` covers both the absent key and an empty
    # list -> nothing to archive for this cam, bail before touching the payload.
    wanted = ARCHIVE_RULES.get(key)
    if not wanted:
        return False
    # Wildcard: archive EVERY event for this cam regardless of class. Lets you
    # configure a cam with ["any"] in the env without enumerating labels.
    if "any" in wanted or "*" in wanted:
        return True
    # Otherwise the event must carry at least one of the configured labels:
    # non-empty set intersection between what the event matched and what we want.
    return bool(event_labels(data) & set(wanted))


def should_analyze(key, data):
    """True if `key` should be sent to Gemini for this event.

    Gated on GEMINI_API_KEY being set, then matched against VISION_RULES (same
    shape as ARCHIVE_RULES) with an extra '*' wildcard KEY that matches ANY camera
    (the default {"*": ["any"]} analyses every cam on every event). A rule list of
    ['any'] (or '*') matches any event for that cam; otherwise the event must carry
    one of the configured labels.
    """
    if not GEMINI_API_KEY:
        return False
    # Per-cam rule first, then the '*' wildcard cam as a fallback. `wanted is None`
    # (cam absent AND no wildcard) -> not configured -> skip.
    wanted = VISION_RULES.get(key)
    if wanted is None:
        wanted = VISION_RULES.get("*")
    if not wanted:
        return False
    if "any" in wanted or "*" in wanted:
        return True
    return bool(event_labels(data) & set(wanted))


def prune_archive(dstdir):
    """Remove archived *.jpg older than ARCHIVE_RETENTION_DAYS by mtime.

    Best-effort: any error is logged and swallowed so a prune failure can never
    kill the event listener.
    """
    try:
        # Everything with mtime before this wall-clock instant is too old. mtime
        # (not the filename timestamp) is the source of truth so a clock skew or a
        # manually-copied file is still pruned sanely.
        cutoff = time.time() - ARCHIVE_RETENTION_DAYS * 86400
        removed = 0
        for name in os.listdir(dstdir):
            # Only our own JPEGs; never touch the .archive_root marker (it lives one
            # level up in ARCHIVE_DIR, not here) or any stray non-jpg.
            if not name.endswith(".jpg"):
                continue
            path = os.path.join(dstdir, name)
            try:
                if os.path.getmtime(path) < cutoff:
                    os.remove(path)
                    removed += 1
            except FileNotFoundError:
                # Raced with another writer/pruner; the file is already gone, which
                # is the outcome we wanted anyway -> ignore.
                pass
        if removed:
            log(f"archive: pruned {removed} still(s) older than "
                f"{ARCHIVE_RETENTION_DAYS}d from {os.path.basename(dstdir)}/")
    except Exception as exc:
        # Broad catch by design: a prune failure (NFS hiccup, perms) must never
        # propagate and kill the event listener. Log and move on.
        log(f"archive prune error ({exc!r})")


def archive_event_still(key):
    """Copy the current on-disk still for `key` into the dated archive.

    Opt-in + NFS-safe: the archive is active only when the sentinel ARCHIVE_MARKER
    file exists inside ARCHIVE_DIR. That marker lives on the real ZFS share, so a
    docker-auto-created empty LOCAL bind dir (NFS down) lacks it and the archive
    disables (logs once) rather than silently writing to local disk; the marker
    reappearing re-arms the warning. The per-cam subdir is created on first write,
    so a newly added/renamed camera needs no manual mkdir. The current
    OUT_DIR/<key>.jpg is copied (read bytes -> write .tmp -> os.replace) to
    ARCHIVE_DIR/<key>/<UTC-timestamp>.jpg with microseconds in the name to avoid
    same-second collisions, then the dir is pruned to retention.
    """
    global _archive_warned
    # --- NFS-safe sentinel guard (req #1) -----------------------------------
    # The marker is a file we created ON the ZFS share. If the NFS mount is down,
    # docker has auto-created an empty LOCAL bind dir that does NOT contain it, so
    # isfile() is False and we refuse to write (which would silently land on local
    # disk). Latch the warning so an event storm logs it once; clear the latch the
    # instant the marker is back so a future outage warns again.
    marker = os.path.join(ARCHIVE_DIR, ARCHIVE_MARKER)
    if not os.path.isfile(marker):
        if not _archive_warned:
            log(f"archive disabled (marker {marker} missing; NFS down or unmounted?)")
            _archive_warned = True
        return
    _archive_warned = False
    # Source is the still the periodic cycle / event_grab just wrote. If it doesn't
    # exist yet (cam never produced a frame this run) there's nothing to copy.
    src = os.path.join(OUT_DIR, f"{key}.jpg")
    if not os.path.exists(src):
        log(f"archive {key}: skip (no {src} yet)")
        return
    # --- per-cam subdir, auto-created (req #2) ------------------------------
    # No pre-created dirs: makedirs(exist_ok=True) is a no-op once it exists and
    # creates it on first write, so a newly added/renamed cam (new stream_key)
    # just starts archiving with no manual mkdir.
    dstdir = os.path.join(ARCHIVE_DIR, key)
    os.makedirs(dstdir, exist_ok=True)
    # UTC + microseconds: sortable, tz-unambiguous, and collision-proof if two
    # events for one cam land in the same second.
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S_%fZ")
    dst = os.path.join(dstdir, f"{ts}.jpg")
    tmp = f"{dst}.tmp"
    try:
        # Read whole JPEG, write to a .tmp, then os.replace -> atomic rename. A
        # reader (or a backup walking the tree) never sees a half-written .jpg.
        with open(src, "rb") as f:
            jpeg = f.read()
        with open(tmp, "wb") as f:
            f.write(jpeg)
        os.replace(tmp, dst)
        log(f"archive wrote wyze/{key}/{ts}.jpg ({len(jpeg)} bytes)")
    except Exception as exc:
        # Don't let an I/O error (NFS stall mid-write, full disk) kill the listener.
        # The orphaned .tmp, if any, is harmless and overwritten next time.
        log(f"archive {key} failed: {exc!r}")
        return
    # Enforce retention on this cam's dir after every successful write.
    prune_archive(dstdir)


def event_burst(key):
    """Pull a short multi-frame burst for `key` from the warm go2rtc stream.

    Runs on the listener's executor thread (blocking). Grabs up to VISION_FRAMES
    frames VISION_FRAME_INTERVAL seconds apart so Gemini sees motion ACROSS the
    burst (a single still often misses the moving subject). Each fetch is taken
    under _go2rtc_lock (serialised against go2rtc.restart()), the same guard
    event_grab uses. Byte-identical consecutive frames are dropped (a stalled
    stream returns the same JPEG) so we don't pay to send duplicates. Returns a
    list of JPEG byte strings (possibly empty if the cam went offline).
    """
    if key not in current_streams:
        log(f"vision burst {key}: skipped (no live stream this cycle)")
        return []
    if key in _frame_unhealthy:
        log(f"vision burst {key}: skipped (stream unhealthy at last cycle)")
        return []
    frames = []
    seen = set()
    for i in range(VISION_FRAMES):
        jpeg = _fetch_frame_locked(key)
        if not jpeg:
            # Tight event budget already spent on this frame -> the stream is cold.
            # Stop the burst rather than waiting it out on a dead feed (Hassio-3hd);
            # whatever we collected so far still gets analysed.
            log(f"vision burst {key}: stream cold, stopping after {len(frames)} frame(s)")
            break
        # Dedupe identical frames (stalled stream) by content hash so a frozen
        # feed costs one image, not VISION_FRAMES copies of the same picture.
        h = hash(jpeg)
        if h not in seen:
            seen.add(h)
            frames.append(jpeg)
        # Sleep BETWEEN frames only (not after the last) so the burst spans
        # (VISION_FRAMES-1)*interval seconds, not one interval longer.
        if i < VISION_FRAMES - 1:
            time.sleep(VISION_FRAME_INTERVAL)
    return frames


def _image_part(jpeg):
    """Wrap a JPEG byte string as a Gemini inline_data image part."""
    return {
        "inline_data": {
            "mime_type": "image/jpeg",
            "data": base64.b64encode(jpeg).decode("ascii"),
        }
    }


def analyze_with_gemini(frames, title, labels, baseline=None, boxed=False):
    """Send a burst of JPEGs to Gemini and return the parsed structured result.

    Builds a single multimodal request. When `baseline` (a reference JPEG of the
    empty scene) is supplied, it is sent FIRST -- labeled as the background -- and
    the prompt tells the model to report only what differs from it (Hassio-i02);
    otherwise the raw burst is described on its own. generationConfig pins
    thinkingBudget=0 (REQUIRED -- otherwise the model spends the whole output
    budget "thinking" and returns an empty/truncated answer) and asks for JSON
    matching VISION_SCHEMA. Returns the parsed dict, or None on any failure
    (network, non-200, unparseable). NEVER logs the request URL or API key (the
    key rides in the query string): only the HTTP status and a short message tail.
    """
    if not frames:
        return None
    # label_hint folds the Wyze-reported AI labels into the prompt so the model
    # has a steer ("the camera's AI flagged: person, package") without us asserting
    # they're correct -- it still reports what it actually sees.
    label_hint = f" (the camera's AI flagged: {', '.join(sorted(labels))})" if labels else ""
    # box_hint (Hassio-zcm): Wyze's event_screenshot has a GREEN bounding box drawn
    # over the region its detector flagged as moving. Point the model at it so it
    # focuses on the actual trigger, but tell it the box is a software overlay (not a
    # real object) so it isn't described as part of the scene. Live go2rtc fallback
    # frames have no box, so this is empty for them (boxed=False).
    box_hint = (
        " Wyze has drawn a GREEN BOUNDING BOX on the image around the region its "
        "motion detector flagged -- look there first to find what triggered the "
        "event. The box is a software overlay, not a real object: do not describe "
        "the box itself."
    ) if boxed else ""
    template = VISION_PROMPT_BASELINE if baseline else VISION_PROMPT
    prompt = template.format(
        title=title or "camera",
        interval=VISION_FRAME_INTERVAL,
        label_hint=label_hint,
        box_hint=box_hint,
    )
    parts = [{"text": prompt}]
    if baseline:
        # Interleave labels so the model knows which image is the background vs the
        # live event (Gemini honours text parts placed between images).
        parts.append({"text": "Background reference (normal empty scene):"})
        parts.append(_image_part(baseline))
        parts.append({"text": "Live event burst frames:"})
    for jpeg in frames:
        parts.append(_image_part(jpeg))
    body = {
        "contents": [{"role": "user", "parts": parts}],
        "generationConfig": {
            "temperature": 0.2,
            "maxOutputTokens": 600,
            # Mandatory: without thinkingBudget=0 the 2.5 models burn the entire
            # output allowance on hidden "thoughts" and return nothing usable.
            "thinkingConfig": {"thinkingBudget": 0},
            "responseMimeType": "application/json",
            "responseSchema": VISION_SCHEMA,
        },
    }
    url = f"{GEMINI_ENDPOINT}/{VISION_MODEL}:generateContent?key={GEMINI_API_KEY}"
    try:
        resp = requests.post(url, json=body, timeout=GEMINI_TIMEOUT)
    except Exception as exc:
        log(f"vision: gemini request failed ({exc})")
        return None
    if resp.status_code != 200:
        # text[:200] only -- never the URL/key. Gemini errors are short JSON.
        log(f"vision: gemini http={resp.status_code} {resp.text[:200]}")
        return None
    try:
        payload = resp.json()
        text = payload["candidates"][0]["content"]["parts"][0]["text"]
        return json.loads(text)
    except (KeyError, IndexError, ValueError, TypeError) as exc:
        log(f"vision: gemini parse error ({exc!r})")
        return None


def vision_task(publisher, key, title, data, shot=None):
    """Burst -> Gemini -> MQTT for one event. Best-effort, runs on the executor.

    Steps (any failure logs and returns -- never kills the listener):
      1. require the MQTT publisher (the sensor rides the same broker);
      2. debounce -- skip if we analysed this key < VISION_MIN_INTERVAL ago;
      3. get frames, send them to Gemini, publish the structured result to the
         per-cam discovery sensor.
    When `shot` is given (Wyze's own detection screenshot, Hassio-zcm) it is used as
    the single vision frame -- it was captured AT detection time so it actually
    contains the subject, unlike a live burst grabbed seconds-to-tens-of-seconds
    later. Otherwise we fall back to a live go2rtc burst (event_burst).
    """
    # The result lands on an MQTT-discovery sensor, so without the publisher there's
    # nowhere to put it -- skip rather than pay Gemini for an unpublishable answer.
    if not getattr(publisher, "client", None):
        return
    try:
        now = time.monotonic()
        last = _vision_last.get(key, 0)
        if now - last < VISION_MIN_INTERVAL:
            log(f"vision {key}: debounced ({now - last:.0f}s < {VISION_MIN_INTERVAL}s)")
            return
        _vision_last[key] = now
        frames = [shot] if shot is not None else event_burst(key)
        if not frames:
            log(f"vision {key}: no frames")
            return
        # Background reference (Hassio-i02): the cached ambient still, sent so the
        # model reports only what differs. None until the first periodic cycle has
        # cached one for this cam -> falls back to describing the raw burst.
        baseline = load_baseline(key)
        # boxed: only the Wyze event_screenshot (shot) carries the green detection
        # box; the live event_burst fallback does not.
        result = analyze_with_gemini(
            frames, title, event_labels(data), baseline, boxed=shot is not None
        )
        if result is None:
            log(f"vision {key}: no result")
            return
        publisher.publish_vision(key, title, result, len(frames))
        log(f"vision {key}: published ({len(frames)} frame(s)): "
            f"{str(result.get('summary'))[:80]}")
    except Exception as exc:
        # Broad catch by design: a vision failure must never propagate and kill the
        # event listener. Log and move on.
        log(f"vision {key} failed: {exc!r}")


async def run_event_listener(stop, publisher):
    """Subscribe to HA's `wyze_camera_event` and grab a still per event.

    Mirrors wyze-event-catalog/watcher.py's websocket handshake. On each event it
    maps device_name -> stream_key and ENQUEUES the work; a pool of EVENT_WORKERS
    tasks drains the queue, each running the blocking grab/archive/vision pipeline
    on the default executor. The recv loop itself never blocks, so a sick camera
    can't stall the listener or drop other cams' events (Hassio-3hd). The per-event
    pipeline: a detection still -- Wyze's own event_screenshot when present
    (Hassio-zcm), else a live grab (event_grab) -- then optional archive
    (should_archive) and/or a Gemini vision read (should_analyze -> vision_task,
    publishing to MQTT via `publisher`). Reconnects with exponential backoff 1->60s;
    workers persist across reconnects. Started only when HA_TOKEN is set.
    """
    import websockets

    loop = asyncio.get_running_loop()

    # Layer A (Hassio-3hd): the recv loop must NOT block on a camera's grab+burst,
    # or a single sick cam (KVS timing out for minutes) stalls ws.recv() and events
    # for healthy cams that fire in that window are lost. So each event is handed to
    # a bounded queue drained by a small worker pool; the recv loop only enqueues.
    queue = asyncio.Queue(maxsize=EVENT_QUEUE_MAX)

    async def handle(key, name, data):
        """Per-event pipeline: detection still, then optional archive + vision.

        Hassio-zcm: prefer Wyze's OWN event_screenshot (captured at detection time,
        so it contains the subject) as the still and the single vision frame; fall
        back to a live go2rtc grab/burst only when the screenshot is absent. The
        screenshot is fetched ONCE here and reused for the still + the vision read.
        """
        shot = None
        if EVENT_USE_SCREENSHOT:
            shot = await loop.run_in_executor(None, fetch_event_screenshot, data)
        if shot is not None:
            await loop.run_in_executor(None, use_event_screenshot, key, shot)
        else:
            await loop.run_in_executor(None, event_grab, key)
        # Event-still archive (Hassio-5sa / -bp8): if this cam + event match
        # ARCHIVE_RULES, snapshot the current still to the ZFS archive. Runs after
        # the still is written so it copies the freshest file (screenshot or grab).
        if should_archive(key, data):
            await loop.run_in_executor(None, archive_event_still, key)
        # Gemini vision (Hassio-5sk): if this cam + event match VISION_RULES (and a
        # key+MQTT are configured), analyse the detection still (or a live burst when
        # there was none) and publish to the discovery sensor. vision_task is
        # best-effort (its own try/except).
        if should_analyze(key, data):
            await loop.run_in_executor(
                None, vision_task, publisher, key, name, data, shot,
            )

    async def worker():
        while True:
            key, name, data = await queue.get()
            try:
                await handle(key, name, data)
            except Exception as exc:
                log(f"event worker: {key} failed ({exc!r})")
            finally:
                queue.task_done()

    # Workers live for the whole listener lifetime (across reconnects), so an
    # in-flight grab is never orphaned by a websocket blip.
    workers = [asyncio.create_task(worker()) for _ in range(EVENT_WORKERS)]
    try:
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
                        # Non-blocking handoff: never await the work here. If the
                        # queue is full (workers all stuck on slow cams), drop the
                        # event rather than block recv -- a dropped still is cheaper
                        # than a stalled listener; the next periodic cycle recovers.
                        try:
                            queue.put_nowait((key, data.get("device_name"), data))
                        except asyncio.QueueFull:
                            log(f"event listener: queue full, dropping {key}")
            except Exception as exc:
                if stop["flag"]:
                    break
                log(f"event listener: connection error ({exc}); retry in {backoff}s")
                for _ in range(backoff):
                    if stop["flag"]:
                        break
                    await asyncio.sleep(1)
                backoff = min(backoff * 2, 60)
    finally:
        for w in workers:
            w.cancel()
    log("event listener: stopped")


def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    if VISION_BASELINE:
        os.makedirs(BASELINE_DIR, exist_ok=True)
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
            target=lambda: asyncio.run(run_event_listener(stop, publisher)),
            daemon=True,
        ).start()
        # Vision is a sub-feature of the event listener: it needs a key AND the
        # MQTT sensor to land on. Log which of the three states we're in so the
        # startup line is diagnostic.
        if not GEMINI_API_KEY:
            log("vision disabled (GEMINI_API_KEY not set)")
        elif not publisher.client:
            log("vision disabled (needs MQTT_HOST/USER/PASS for the result sensor)")
        else:
            bg = "background-subtract on" if VISION_BASELINE else "no background ref"
            log(f"vision enabled ({VISION_MODEL}, {VISION_FRAMES} frames, {bg}, "
                f"rules={json.dumps(VISION_RULES)})")
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
