#!/usr/bin/env python3
"""Create/update a storage-mode "Cameras" dashboard in Home Assistant.

Auto-discovers the wyze-vision still cameras (camera.wyze_<key>_snapshot) from
HA's REST /api/states, splits them Live/Offline by the matching live
camera.<key> state, and pushes a "Cameras" dashboard to the HA sidebar over the
WebSocket API (Lovelace config is WebSocket-only -- it is not in the REST API).

The dashboard is storage-mode with its own url_path/sidebar entry
(default "wyze-cameras"), so it NEVER touches your main/overview dashboard.
Idempotent: it creates the dashboard if absent, then saves (overwrites) its
config -- re-run it whenever your cameras change.

Requires an ADMIN long-lived access token (creating a Lovelace dashboard is
admin-only), supplied via HA_TOKEN; it is never logged.

Usage:
  HA_URL=http://homeassistant.local:8123 HA_TOKEN=<admin-token> \
    python3 deploy/build_cameras_dashboard.py
"""
import asyncio
import json
import os
import re
import sys
import urllib.request

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
    """Build the camera list from a HA /api/states payload.

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


def ws_url_from(http_url):
    """Derive the ws(s)://host/api/websocket URL from an HA base URL."""
    url = http_url.rstrip("/")
    if url.startswith("https://"):
        url = "wss://" + url[len("https://"):]
    elif url.startswith("http://"):
        url = "ws://" + url[len("http://"):]
    if not url.endswith("/api/websocket"):
        url += "/api/websocket"
    return url


def fetch_states(http_url, token):
    """GET /api/states (REST) and return the parsed list."""
    req = urllib.request.Request(
        http_url.rstrip("/") + "/api/states",
        headers={"Authorization": f"Bearer {token}"},
    )
    with urllib.request.urlopen(req, timeout=20) as resp:
        return json.loads(resp.read())


async def _cmd(ws, next_id, payload):
    """Send a WS command (auto-id) and return its result message."""
    cid = next_id[0]
    next_id[0] += 1
    payload = {"id": cid, **payload}
    await ws.send(json.dumps(payload))
    while True:
        msg = json.loads(await ws.recv())
        if msg.get("id") == cid and msg.get("type") == "result":
            return msg


async def push_dashboard(http_url, token, url_path, title, icon, config):
    """Create the storage-mode dashboard if absent, then save its config."""
    import websockets  # lazy: keep module import dependency-free for tests

    ws_url = ws_url_from(http_url)
    async with websockets.connect(ws_url, max_size=None, ping_interval=20) as ws:
        msg = json.loads(await ws.recv())
        if msg.get("type") != "auth_required":
            raise RuntimeError(f"unexpected first frame: {msg}")
        await ws.send(json.dumps({"type": "auth", "access_token": token}))
        msg = json.loads(await ws.recv())
        if msg.get("type") != "auth_ok":
            raise RuntimeError(f"auth failed: {msg.get('message', msg)}")

        next_id = [1]
        listed = await _cmd(ws, next_id, {"type": "lovelace/dashboards/list"})
        if not listed.get("success"):
            raise RuntimeError(f"dashboards/list failed: {listed.get('error')}")
        existing = {d.get("url_path") for d in listed.get("result", [])}

        if url_path in existing:
            print(f"dashboard '{url_path}' exists -> updating config")
        else:
            created = await _cmd(ws, next_id, {
                "type": "lovelace/dashboards/create",
                "url_path": url_path,
                "mode": "storage",
                "title": title,
                "icon": icon,
                "show_in_sidebar": True,
                "require_admin": False,
            })
            if not created.get("success"):
                raise RuntimeError(
                    f"dashboards/create failed: {created.get('error')}")
            print(f"dashboard '{url_path}' created")

        saved = await _cmd(ws, next_id, {
            "type": "lovelace/config/save",
            "url_path": url_path,
            "config": config,
        })
        if not saved.get("success"):
            raise RuntimeError(f"config/save failed: {saved.get('error')}")


def main():
    http_url = os.environ.get("HA_URL", "http://homeassistant.local:8123")
    token = os.environ.get("HA_TOKEN")
    if not token:
        sys.exit("set HA_TOKEN to a Home Assistant admin long-lived access token")
    url_path = os.environ.get("DASH_URL_PATH", "wyze-cameras")
    title = os.environ.get("DASH_TITLE", "Cameras")
    icon = os.environ.get("DASH_ICON", "mdi:cctv")

    cams = discover_cameras(fetch_states(http_url, token))
    if not cams:
        sys.exit("no camera.wyze_<key>_snapshot entities found "
                 "(run provision_local_file_cameras.sh first)")
    live = sum(1 for c in cams if c["online"])
    config = build_dashboard_config(cams, title=title)
    asyncio.run(push_dashboard(http_url, token, url_path, title, icon, config))
    print(f"done: {len(cams)} cameras ({live} live, {len(cams) - live} offline) "
          f"-> dashboard '{url_path}'")


if __name__ == "__main__":
    main()
