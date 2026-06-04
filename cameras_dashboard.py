"""Pure, network-free logic for the wyze-vision "Cameras" dashboard.

Shared by two entrypoints:
  * deploy/build_cameras_dashboard.py -- the one-shot CLI that pushes the
    dashboard once over the WebSocket API, and
  * snapshot.py -- the in-sidecar live dashboard-sync task that re-pushes it
    whenever the camera roster or online/offline split changes.

Everything here is a pure transform of its arguments (no I/O), so it is the
single source of truth for what the dashboard contains and is trivially
unit-tested (tests/test_dashboard.py).
"""
import re

# Snapshot tiles created by provision_local_file_cameras.sh are named
# camera.wyze_<key>_snapshot; the live (tap-through) camera is camera.<key>.
SNAP_RE = re.compile(r"^camera\.wyze_(.+)_snapshot$")

# Live-camera states that mean the camera is not reachable right now.
OFFLINE_STATES = {"unavailable", "unknown", "off", "offline"}

# Rolling Wyze event log (left half of band 1). Static -- no per-cam edits.
EVENT_LOG_TEMPLATE = (
    "{% set rows = state_attr('sensor.wyze_event_log', 'entries') or [] %}\n"
    "{% set ns = namespace(lines=[]) %}\n"
    "{% for e in rows %}\n"
    "{% set ns.lines = ns.lines + ['**' ~ e.cam ~ '** &middot; ' ~ e.label ~ "
    "' &middot; ' ~ relative_time(as_datetime(e.ts)) ~ ' ago'] %}\n"
    "{% endfor %}\n"
    "{{ ns.lines | join('\\n\\n') if ns.lines else "
    "'_No Wyze events recorded yet._' }}\n"
)

# Latest per-cam Gemini vision read (right half of band 1). Auto-lists every
# sensor.wyze_<key>_vision, newest first -- no per-cam edits.
VISION_TEMPLATE = (
    "{% set ns = namespace(items=[]) %}\n"
    "{% for s in states.sensor %}\n"
    "{% if s.entity_id.startswith('sensor.wyze_') and "
    "s.entity_id.endswith('_vision') and s.state not in "
    "['unknown', 'unavailable'] %}\n"
    "{% set ns.items = ns.items + [s] %}\n"
    "{% endif %}\n"
    "{% endfor %}\n"
    "{% set rows = ns.items | sort(attribute='state', reverse=true) %}\n"
    "{% set out = namespace(lines=[]) %}\n"
    "{% for s in rows %}\n"
    "{% set cam = state_attr(s.entity_id, 'camera') or s.entity_id %}\n"
    "{% set summary = state_attr(s.entity_id, 'summary') or "
    "state_attr(s.entity_id, 'description') or '(no description)' %}\n"
    "{% set changed = state_attr(s.entity_id, 'change_detected') %}\n"
    "{% set ago = relative_time(as_datetime(s.state)) ~ ' ago' %}\n"
    "{% if changed %}\n"
    "{% set out.lines = out.lines + ['**' ~ cam ~ '** &middot; ' ~ summary ~ "
    "' &middot; ' ~ ago] %}\n"
    "{% else %}\n"
    "{% set out.lines = out.lines + [cam ~ ' &middot; _no change_ &middot; ' ~ "
    "ago] %}\n"
    "{% endif %}\n"
    "{% endfor %}\n"
    "{{ out.lines | join('\\n\\n') if out.lines else '_No vision reads yet._' }}\n"
)


def _title_from(state, key):
    """Friendly tile title, e.g. "Wyze Front Door Snapshot" -> "Front Door"."""
    fn = (state.get("attributes") or {}).get("friendly_name") or ""
    fn = re.sub(r"^Wyze\s+", "", fn)
    fn = re.sub(r"\s+Snapshot$", "", fn)
    return fn or key.replace("_", " ").title()


def _is_online(live_state):
    """Online unless the live camera reports a known-offline state.

    A missing live entity (None) defaults to online: the snapshot tile still
    renders, and we have no reachability signal to demote it.
    """
    if live_state is None:
        return True
    return live_state.lower() not in OFFLINE_STATES


def discover_cameras(states):
    """Build the camera list from a HA /api/states (or WS get_states) payload.

    Returns a list of dicts {key, title, snapshot, live, online} sorted by key,
    one per camera.wyze_<key>_snapshot entity found.
    """
    by_id = {s["entity_id"]: s for s in states}
    cams = []
    for s in states:
        m = SNAP_RE.match(s["entity_id"])
        if not m:
            continue
        key = m.group(1)
        live_id = f"camera.{key}"
        live_present = live_id in by_id
        cams.append({
            "key": key,
            "title": _title_from(s, key),
            "snapshot": s["entity_id"],
            # Tap-through target: the live cam if it exists, else the still.
            "live": live_id if live_present else s["entity_id"],
            "online": _is_online(by_id.get(live_id, {}).get("state")),
        })
    cams.sort(key=lambda c: c["key"])
    return cams


def signature(cameras):
    """Stable change-detection key for a discovered camera roster.

    Two rosters that would render an identical dashboard share a signature; any
    change to membership, the online/offline split, a title, or a tap-through
    target changes it. The live-sync task pushes a new config ONLY when this
    differs from the last push, so unchanged state never reloads viewers.
    `discover_cameras` already returns cameras sorted by key, so the tuple order
    is deterministic.
    """
    return tuple(
        (c["key"], c["online"], c["title"], c["snapshot"], c["live"])
        for c in cameras
    )


def is_roster_event(msg):
    """True if a HA WS event message could change the dashboard's camera roster.

    Roster-affecting = any `entity_registry_updated` (a tile added / removed /
    renamed), or a `state_changed` on a `camera.*` entity (an online/offline
    flip, or a snapshot tile appearing/disappearing). Everything else in HA's
    `state_changed` firehose is ignored by the live-sync watcher.
    """
    if msg.get("type") != "event":
        return False
    ev = msg.get("event") or {}
    etype = ev.get("event_type")
    if etype == "entity_registry_updated":
        return True
    if etype == "state_changed":
        entity_id = (ev.get("data") or {}).get("entity_id") or ""
        return entity_id.startswith("camera.")
    return False


def _glance(cam):
    return {
        "type": "picture-glance",
        "title": cam["title"],
        "entity": cam["live"],
        "camera_image": cam["snapshot"],
        "entities": [],
    }


def _grid(cams):
    return {
        "type": "grid",
        "columns": 3,
        "square": False,
        "cards": [_glance(c) for c in cams],
    }


def build_dashboard_config(cameras, title="Cameras"):
    """Assemble the Lovelace config dict for the Cameras dashboard.

    Layout mirrors deploy/cameras-dashboard.yaml: a panel view whose single
    root vertical-stack holds (1) an event-log + vision band, (2) a Live camera
    grid, (3) an Offline camera grid. Empty bands are omitted.
    """
    live = [c for c in cameras if c["online"]]
    offline = [c for c in cameras if not c["online"]]

    band1 = {
        "type": "horizontal-stack",
        "cards": [
            {"type": "vertical-stack", "cards": [
                {"type": "markdown", "content": "## Wyze Event Log"},
                {"type": "markdown", "title": "Wyze Event Log",
                 "content": EVENT_LOG_TEMPLATE},
            ]},
            {"type": "vertical-stack", "cards": [
                {"type": "markdown", "content": "## Wyze Vision"},
                {"type": "markdown", "title": "Wyze Vision",
                 "content": VISION_TEMPLATE},
            ]},
        ],
    }

    root_cards = [band1]
    if live:
        root_cards.append({"type": "markdown", "content": "## Live cameras"})
        root_cards.append(_grid(live))
    if offline:
        root_cards.append({"type": "markdown", "content": "## Offline cameras"})
        root_cards.append(_grid(offline))

    return {
        "title": title,
        "views": [{
            "title": "Wyze",
            "path": "wyze",
            "icon": "mdi:cctv",
            "panel": True,
            "cards": [{"type": "vertical-stack", "cards": root_cards}],
        }],
    }
