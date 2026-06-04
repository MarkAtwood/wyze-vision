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

The dashboard's contents (camera discovery + Lovelace config assembly) live in
the shared, network-free cameras_dashboard module, so this CLI and the in-sidecar
live-sync task (snapshot.py) build the exact same dashboard. Run from the repo
root as a module so that import resolves:

Usage:
  HA_URL=http://homeassistant.local:8123 HA_TOKEN=<admin-token> \
    python3 -m deploy.build_cameras_dashboard
"""
import asyncio
import json
import os
import sys
import urllib.request

from cameras_dashboard import build_dashboard_config, discover_cameras


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
