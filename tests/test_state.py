import json

from pipeline.state import State
from tests.conftest import utc


def test_roundtrip_and_atomic_file(tmp_path):
    path = tmp_path / "data" / "state.json"
    s = State(path)
    s.set("tg_offset", 7)
    s.append("alerts", {"at": "2026-10-23T12:00:00+00:00", "status": "sent"})
    assert State(path).get("tg_offset") == 7
    assert [p.name for p in path.parent.iterdir()] == ["state.json"]  # no temp file left behind


def test_old_entries_are_dropped(tmp_path):
    s = State(tmp_path / "state.json")
    s.data["alert_memory"] = {"ES=F:day:down": {"at": "2026-10-01T12:00:00+00:00", "move": 2.0},
                              "NQ=F:day:down": {"at": "2026-10-22T12:00:00+00:00", "move": 3.0}}  # fmt: skip
    s.append("claude_calls", {"at": "2026-10-01T12:00:00+00:00", "kind": "explain"})
    s.append("claude_calls", {"at": "2026-10-23T12:00:00+00:00", "kind": "explain"})
    assert [c["at"][:10] for c in s.data["claude_calls"]] == ["2026-10-23"]
    assert list(s.data["alert_memory"]) == ["NQ=F:day:down"]
    assert len(s.items_since("claude_calls", utc("2026-10-23T00:00"))) == 1


def test_unreadable_file_starts_empty(tmp_path):
    path = tmp_path / "state.json"
    path.write_text("{not json")
    assert State(path).data == {}


def test_readonly_never_writes(tmp_path):
    path = tmp_path / "state.json"
    path.write_text(json.dumps({"heartbeat": "x"}))
    s = State(path, readonly=True)
    s.set("heartbeat", "y")
    assert s.get("heartbeat") == "y" and json.loads(path.read_text()) == {"heartbeat": "x"}
