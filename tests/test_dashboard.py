"""Unit tests for the pure dashboard-building logic in
deploy/build_cameras_dashboard.py.

No network: these exercise camera discovery from a /api/states payload, the
online/offline split, title derivation, the URL helper, and Lovelace config
assembly -- all pure transforms of their arguments.
"""
import cameras_dashboard as cd
from deploy import build_cameras_dashboard as bcd


def _state(entity_id, state="idle", friendly_name=None):
    attrs = {}
    if friendly_name is not None:
        attrs["friendly_name"] = friendly_name
    return {"entity_id": entity_id, "state": state, "attributes": attrs}


# --- ws_url_from -------------------------------------------------------------

def test_ws_url_from_http():
    assert bcd.ws_url_from("http://homeassistant.local:8123") == \
        "ws://homeassistant.local:8123/api/websocket"


def test_ws_url_from_https_and_trailing_slash():
    assert bcd.ws_url_from("https://ha.example.com/") == \
        "wss://ha.example.com/api/websocket"


def test_ws_url_from_already_ws_path():
    assert bcd.ws_url_from("ws://h:8123/api/websocket") == \
        "ws://h:8123/api/websocket"


# --- title derivation --------------------------------------------------------

def test_title_strips_wyze_and_snapshot():
    s = _state("camera.wyze_front_door_snapshot",
               friendly_name="Wyze Front Door Snapshot")
    assert cd._title_from(s, "front_door") == "Front Door"


def test_title_falls_back_to_key():
    s = _state("camera.wyze_side_gate_snapshot")
    assert cd._title_from(s, "side_gate") == "Side Gate"


# --- online heuristic --------------------------------------------------------

def test_is_online_true_for_active_states():
    assert cd._is_online("idle")
    assert cd._is_online("streaming")


def test_is_online_false_for_offline_states():
    assert not cd._is_online("unavailable")
    assert not cd._is_online("off")
    assert not cd._is_online("UNKNOWN")  # case-insensitive


def test_is_online_missing_live_defaults_true():
    assert cd._is_online(None)


# --- discover_cameras --------------------------------------------------------

def test_discover_cameras_splits_and_maps():
    states = [
        _state("camera.wyze_front_door_snapshot",
               friendly_name="Wyze Front Door Snapshot"),
        _state("camera.front_door", state="idle"),
        _state("camera.wyze_garage_snapshot",
               friendly_name="Wyze Garage Snapshot"),
        _state("camera.garage", state="unavailable"),
        _state("sensor.something_else"),  # ignored
    ]
    cams = cd.discover_cameras(states)
    assert [c["key"] for c in cams] == ["front_door", "garage"]

    fd = cams[0]
    assert fd["title"] == "Front Door"
    assert fd["snapshot"] == "camera.wyze_front_door_snapshot"
    assert fd["live"] == "camera.front_door"  # live entity present
    assert fd["online"] is True

    gar = cams[1]
    assert gar["online"] is False  # live camera.garage is unavailable


def test_discover_cameras_missing_live_taps_snapshot():
    states = [
        _state("camera.wyze_attic_snapshot",
               friendly_name="Wyze Attic Snapshot"),
        # no camera.attic
    ]
    cams = cd.discover_cameras(states)
    assert len(cams) == 1
    # Tap-through falls back to the snapshot entity when no live cam exists.
    assert cams[0]["live"] == "camera.wyze_attic_snapshot"
    assert cams[0]["online"] is True


# --- build_dashboard_config --------------------------------------------------

def _cam(key, online, live=None):
    return {
        "key": key,
        "title": key.replace("_", " ").title(),
        "snapshot": f"camera.wyze_{key}_snapshot",
        "live": live or f"camera.{key}",
        "online": online,
    }


def test_build_config_top_level_shape():
    cfg = cd.build_dashboard_config([_cam("front_door", True)], title="Cameras")
    assert cfg["title"] == "Cameras"
    assert len(cfg["views"]) == 1
    view = cfg["views"][0]
    assert view["panel"] is True
    # Panel mode renders only the first card, so everything lives under one root.
    assert len(view["cards"]) == 1
    assert view["cards"][0]["type"] == "vertical-stack"


def test_build_config_live_and_offline_bands():
    cams = [_cam("front_door", True), _cam("garage", False)]
    root = cd.build_dashboard_config(cams)["views"][0]["cards"][0]["cards"]
    headings = [c["content"] for c in root if c.get("type") == "markdown"]
    assert "## Live cameras" in headings
    assert "## Offline cameras" in headings

    grids = [c for c in root if c.get("type") == "grid"]
    assert len(grids) == 2
    live_grid, offline_grid = grids
    assert len(live_grid["cards"]) == 1
    assert live_grid["cards"][0]["camera_image"] == \
        "camera.wyze_front_door_snapshot"
    assert live_grid["cards"][0]["entity"] == "camera.front_door"
    assert len(offline_grid["cards"]) == 1


def test_build_config_omits_empty_offline_band():
    cams = [_cam("front_door", True)]
    root = cd.build_dashboard_config(cams)["views"][0]["cards"][0]["cards"]
    headings = [c["content"] for c in root if c.get("type") == "markdown"]
    assert "## Live cameras" in headings
    assert "## Offline cameras" not in headings


def test_build_config_picture_glance_shape():
    cams = [_cam("front_door", True)]
    root = cd.build_dashboard_config(cams)["views"][0]["cards"][0]["cards"]
    grid = [c for c in root if c.get("type") == "grid"][0]
    card = grid["cards"][0]
    assert card["type"] == "picture-glance"
    assert card["entities"] == []  # required by the picture-glance schema


# --- signature (live-sync change detection) ---------------------------------

def test_signature_stable_for_identical_rosters():
    a = [_cam("front_door", True), _cam("garage", False)]
    b = [_cam("front_door", True), _cam("garage", False)]
    assert cd.signature(a) == cd.signature(b)


def test_signature_changes_on_online_flip():
    before = cd.signature([_cam("front_door", True)])
    after = cd.signature([_cam("front_door", False)])
    assert before != after


def test_signature_changes_on_roster_change():
    one = cd.signature([_cam("front_door", True)])
    two = cd.signature([_cam("front_door", True), _cam("garage", True)])
    assert one != two


def test_signature_changes_on_title_change():
    base = _cam("front_door", True)
    renamed = dict(base, title="Porch")
    assert cd.signature([base]) != cd.signature([renamed])


# --- is_roster_event (live-sync event filter) -------------------------------

def _event(event_type, **data):
    return {"type": "event",
            "event": {"event_type": event_type, "data": data}}


def test_roster_event_true_for_camera_state_change():
    assert cd.is_roster_event(_event("state_changed", entity_id="camera.garage"))


def test_roster_event_false_for_noncamera_state_change():
    assert not cd.is_roster_event(
        _event("state_changed", entity_id="sensor.kitchen_temp"))


def test_roster_event_true_for_registry_update():
    assert cd.is_roster_event(
        _event("entity_registry_updated", action="create",
               entity_id="camera.wyze_porch_snapshot"))


def test_roster_event_false_for_non_event_message():
    assert not cd.is_roster_event({"type": "result", "id": 7, "success": True})


def test_roster_event_false_for_unrelated_event():
    assert not cd.is_roster_event(_event("call_service"))
