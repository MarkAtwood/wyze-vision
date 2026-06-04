"""Unit tests for the HTTP-touching vision helpers in snapshot.py.

`requests.get` / `requests.post` are monkeypatched so nothing leaves the box;
we assert on what the functions would have sent and how they handle responses.
"""
import snapshot


JPEG_MAGIC = b"\xff\xd8"
FAKE_JPEG = JPEG_MAGIC + b"\x00\x10fakejpegbody"


class FakeResponse:
    def __init__(self, status_code=200, content=b"", json_data=None, text=""):
        self.status_code = status_code
        self.content = content
        self._json = json_data
        self.text = text

    def json(self):
        return self._json


# --- fetch_event_screenshot --------------------------------------------------

def _patch_get(monkeypatch, resp, capture):
    def fake_get(url, headers=None, timeout=None):
        capture["url"] = url
        capture["headers"] = headers or {}
        capture["timeout"] = timeout
        return resp
    monkeypatch.setattr(snapshot.requests, "get", fake_get)


def test_fetch_event_screenshot_ok_returns_bytes(monkeypatch):
    capture = {}
    _patch_get(monkeypatch, FakeResponse(200, FAKE_JPEG), capture)
    out = snapshot.fetch_event_screenshot({"event_screenshot": "https://wyze/x.jpg"})
    assert out == FAKE_JPEG
    # The Wyze media gateway runs a UA allowlist; we must send EVENT_MEDIA_UA...
    assert capture["headers"].get("User-Agent") == snapshot.EVENT_MEDIA_UA
    # ...and NEVER an Authorization header (the blob backend 400s on one).
    assert "Authorization" not in capture["headers"]


def test_fetch_event_screenshot_401_returns_none(monkeypatch):
    capture = {}
    _patch_get(monkeypatch, FakeResponse(401, b"Access token is invalid."), capture)
    assert snapshot.fetch_event_screenshot({"event_screenshot": "https://wyze/x.jpg"}) is None


def test_fetch_event_screenshot_non_jpeg_returns_none(monkeypatch):
    capture = {}
    _patch_get(monkeypatch, FakeResponse(200, b"not a jpeg"), capture)
    assert snapshot.fetch_event_screenshot({"event_screenshot": "https://wyze/x.jpg"}) is None


def test_fetch_event_screenshot_missing_url_returns_none(monkeypatch):
    # No request should be made at all when there's no URL.
    def boom(*a, **k):
        raise AssertionError("requests.get should not be called without a URL")
    monkeypatch.setattr(snapshot.requests, "get", boom)
    assert snapshot.fetch_event_screenshot({}) is None
    assert snapshot.fetch_event_screenshot(None) is None


# --- analyze_with_gemini: prompt box/label hints -----------------------------

GREEN_BOX_MARK = "GREEN BOUNDING BOX"
LABEL_HINT_MARK = "the camera's AI flagged"


def _patch_post(monkeypatch, capture):
    """Capture the request body; return a well-formed 200 so parsing succeeds."""
    valid = {
        "candidates": [
            {"content": {"parts": [{"text": "{\"summary\": \"ok\"}"}]}}
        ]
    }

    def fake_post(url, json=None, timeout=None):
        capture["url"] = url
        capture["json"] = json
        capture["timeout"] = timeout
        return FakeResponse(200, json_data=valid)
    monkeypatch.setattr(snapshot.requests, "post", fake_post)


def _prompt_of(capture):
    return capture["json"]["contents"][0]["parts"][0]["text"]


def test_gemini_prompt_includes_box_hint_when_boxed(monkeypatch):
    capture = {}
    _patch_post(monkeypatch, capture)
    snapshot.analyze_with_gemini([FAKE_JPEG], "Front Door", labels=set(), boxed=True)
    assert GREEN_BOX_MARK in _prompt_of(capture)


def test_gemini_prompt_omits_box_hint_when_not_boxed(monkeypatch):
    capture = {}
    _patch_post(monkeypatch, capture)
    snapshot.analyze_with_gemini([FAKE_JPEG], "Front Door", labels=set(), boxed=False)
    assert GREEN_BOX_MARK not in _prompt_of(capture)


def test_gemini_prompt_includes_label_hint(monkeypatch):
    capture = {}
    _patch_post(monkeypatch, capture)
    snapshot.analyze_with_gemini(
        [FAKE_JPEG], "Front Door", labels={"person", "package"}, boxed=False
    )
    prompt = _prompt_of(capture)
    assert LABEL_HINT_MARK in prompt
    assert "person" in prompt and "package" in prompt


def test_gemini_prompt_omits_label_hint_when_no_labels(monkeypatch):
    capture = {}
    _patch_post(monkeypatch, capture)
    snapshot.analyze_with_gemini([FAKE_JPEG], "Front Door", labels=set(), boxed=False)
    assert LABEL_HINT_MARK not in _prompt_of(capture)


def test_gemini_empty_frames_returns_none_without_request(monkeypatch):
    def boom(*a, **k):
        raise AssertionError("no request should be made for empty frames")
    monkeypatch.setattr(snapshot.requests, "post", boom)
    assert snapshot.analyze_with_gemini([], "Front Door", labels=set()) is None
