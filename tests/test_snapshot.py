"""Unit tests for the pure decision/normalization logic in snapshot.py.

No network, no go2rtc, no MQTT -- these exercise functions that only transform
their arguments (or read module-level rule dicts that we monkeypatch).
"""
import snapshot


# --- stream_key: nickname -> safe go2rtc stream key / filename stem ----------

def test_stream_key_basic():
    assert snapshot.stream_key("Front Door") == "front_door"


def test_stream_key_leading_digit_and_space():
    assert snapshot.stream_key("3D Printer") == "3d_printer"


def test_stream_key_collapses_runs_and_strips():
    # A run of non-alphanumerics collapses to ONE underscore; leading/trailing
    # separators are stripped.
    assert snapshot.stream_key("  Back-Yard / Cam!! ") == "back_yard_cam"


def test_stream_key_none_is_empty():
    assert snapshot.stream_key(None) == ""


# --- undouble: KVS signaling URLs come percent-double-encoded -----------------

def test_undouble_single_pass():
    assert snapshot.undouble("a%25b") == "a%b"


def test_undouble_double_encoded():
    # %2525 -> %25 -> % over the (max 3) passes.
    assert snapshot.undouble("a%2525b") == "a%b"


def test_undouble_no_encoding_unchanged():
    assert snapshot.undouble("https://example/x?y=1") == "https://example/x?y=1"


# --- event_labels: tag codes / ai names -> {person,pet,vehicle,package} -------

def test_event_labels_from_tag_codes():
    # 101=person, 103=vehicle per ARCHIVE_TAG_CODES.
    assert snapshot.event_labels({"tag_list": [101, 103]}) == {"person", "vehicle"}


def test_event_labels_from_ai_names_case_insensitive():
    assert snapshot.event_labels({"ai_tag_list": ["Person", "PET"]}) == {"person", "pet"}


def test_event_labels_union_of_both_signals():
    out = snapshot.event_labels({"tag_list": [104], "ai_tag_list": ["person"]})
    assert out == {"package", "person"}


def test_event_labels_empty_payload():
    assert snapshot.event_labels({}) == set()


# --- should_archive: ARCHIVE_RULES matching -----------------------------------

def test_should_archive_label_match(monkeypatch):
    monkeypatch.setattr(snapshot, "ARCHIVE_RULES", {"front_door": ["person"]})
    assert snapshot.should_archive("front_door", {"tag_list": [101]}) is True


def test_should_archive_label_miss(monkeypatch):
    monkeypatch.setattr(snapshot, "ARCHIVE_RULES", {"front_door": ["person"]})
    # vehicle event, but only person configured.
    assert snapshot.should_archive("front_door", {"tag_list": [103]}) is False


def test_should_archive_cam_not_configured(monkeypatch):
    monkeypatch.setattr(snapshot, "ARCHIVE_RULES", {"front_door": ["person"]})
    assert snapshot.should_archive("garage", {"tag_list": [101]}) is False


def test_should_archive_any_wildcard_matches_empty_event(monkeypatch):
    monkeypatch.setattr(snapshot, "ARCHIVE_RULES", {"front_door": ["any"]})
    assert snapshot.should_archive("front_door", {}) is True


def test_should_archive_star_wildcard_label(monkeypatch):
    monkeypatch.setattr(snapshot, "ARCHIVE_RULES", {"front_door": ["*"]})
    assert snapshot.should_archive("front_door", {}) is True


# --- should_analyze: GEMINI_API_KEY gate + VISION_RULES matching --------------

def test_should_analyze_no_api_key_is_false(monkeypatch):
    monkeypatch.setattr(snapshot, "GEMINI_API_KEY", "")
    monkeypatch.setattr(snapshot, "VISION_RULES", {"*": ["any"]})
    assert snapshot.should_analyze("front_door", {}) is False


def test_should_analyze_star_cam_any_label(monkeypatch):
    monkeypatch.setattr(snapshot, "GEMINI_API_KEY", "k")
    monkeypatch.setattr(snapshot, "VISION_RULES", {"*": ["any"]})
    # '*' cam key matches ANY camera; 'any' label matches any event.
    assert snapshot.should_analyze("whatever", {}) is True


def test_should_analyze_per_cam_label_match(monkeypatch):
    monkeypatch.setattr(snapshot, "GEMINI_API_KEY", "k")
    monkeypatch.setattr(snapshot, "VISION_RULES", {"porch": ["package"]})
    assert snapshot.should_analyze("porch", {"ai_tag_list": ["package"]}) is True
    assert snapshot.should_analyze("porch", {"ai_tag_list": ["person"]}) is False


def test_should_analyze_cam_absent_and_no_wildcard(monkeypatch):
    monkeypatch.setattr(snapshot, "GEMINI_API_KEY", "k")
    monkeypatch.setattr(snapshot, "VISION_RULES", {"porch": ["person"]})
    assert snapshot.should_analyze("garage", {"tag_list": [101]}) is False
