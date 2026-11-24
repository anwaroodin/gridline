"""tec-register-19-november-2026.csv: 10 verbatim lines of the real 19 Nov 2026 release.
tec-register-01-december-2026-EDITED.csv: the same rows with hand edits (only one release is retrievable):
PRO-001651 total 275 -> 300 MW; PRO-000368 date +91 days; PRO-000950 -> Built, 0 -> 35 MW connected;
PRO-003518 gate -> 2; PRO-000026 -> Under Construction; PRO-001624-1 connected 200 -> 250 MW (no event);
PRO-000398 -> Built with 0 MW (no `connected`) and MW Increase "TBC"; PRO-003578 removed;
PRO-002010 gets a second line with the same Project ID and blank Stage; one new PV row with no Project ID."""

import json
from datetime import date
from pathlib import Path

import pytest

import gridline
from gridline import archive, build, parse_num, parse_tec, published_from_name, read_tec

FIX = Path(__file__).parent / "fixtures"


def test_events_between_fixtures():
    snaps = [
        (p.stem, published_from_name(p.name), read_tec(p))
        for p in sorted(
            FIX.glob("tec-register-*.csv"), key=lambda p: published_from_name(p.name)
        )
    ]
    events, versions = build(snaps)
    got = {
        (
            e["project_number"] or e["project_name"],
            e["event_type"],
            e["new_value"],
            e["days"],
            e["low_confidence"],
        )
        for e in events
    }
    assert got == {
        ("PRO-001651", "capacity_changed", "300.0", None, False),
        ("PRO-000368", "date_slipped", "2026-11-30", 91, False),
        ("PRO-000950", "status_changed", "Built", None, False),
        ("PRO-000950", "connected", "35.0", None, False),
        ("PRO-000398", "status_changed", "Built", None, False),
        ("PRO-003518", "gate_assigned", "2", None, False),
        (
            "PRO-000026",
            "status_changed",
            "Under Construction/Commissioning",
            None,
            False,
        ),
        ("PRO-003578", "withdrawn", None, None, False),
        (
            "PRO-002010",
            "added",
            None,
            None,
            True,
        ),  # repeated Project ID + Stage: can't match one-to-one
        (
            "Example New Solar",
            "added",
            None,
            None,
            True,
        ),  # no Project ID: normalised name key
    }
    assert len(events) == len(got)
    assert (
        next(r for r in snaps[1][2] if r["project_number"] == "PRO-000398")["mw_change"]
        is None
    )  # "TBC"
    # 9 rows kept from the first release + 2 added rows
    assert sum(v["valid_to"] is None for v in versions) == 11
    assert [v["valid_to"] for v in versions if v["project_number"] == "PRO-003578"] == [
        snaps[1][1]
    ]


def test_parsing_guards(tmp_path, monkeypatch):
    real = (FIX / "tec-register-19-november-2026.csv").read_bytes()
    # renamed date column (Ofgem proposal) still maps to effective_from
    renamed = parse_tec(real.replace(b"MW Effective From", b"Planned Connection Date"))
    assert [r["effective_from"] for r in renamed] == [
        r["effective_from"] for r in parse_tec(real)
    ]
    # both date headers during the rename: the blank one must not wipe the filled one
    lines = real.split(b"\r\n")
    both = b"\r\n".join(
        [lines[0] + b",Planned Connection Date"] + [ln + b"," for ln in lines[1:] if ln]
    )
    assert [r["effective_from"] for r in parse_tec(both)] == [
        r["effective_from"] for r in parse_tec(real)
    ]
    # a required column missing is an error, not silent nulls
    with pytest.raises(ValueError, match="status"):
        parse_tec(real.replace(b"Project Status", b"Status Of Project"))
    assert (
        parse_num("nan") is None
        and parse_num("inf") is None
        and parse_num("1,200.5") == 1200.5
    )

    # archive refuses non-TEC or empty blobs and writes nothing
    monkeypatch.chdir(tmp_path)
    for junk in [
        b"<!doctype html><title>503</title>",
        b"",
        real.splitlines(keepends=True)[0],
    ]:
        with pytest.raises(SystemExit):
            archive(junk, "junk", None)
    assert not (tmp_path / "archive").exists()

    # fetch: date comes from the URL file name, not the resource UUID; null last_modified is fine
    url = "https://x/dataset/1d2e/resource/9f8a/download/tec-register-19-november-2026.csv"
    ckan = {
        "result": {"resources": [{"format": "CSV", "url": url, "last_modified": None}]}
    }
    monkeypatch.setattr(
        gridline,
        "http",
        lambda u: json.dumps(ckan).encode() if u == gridline.CKAN else real,
    )
    gridline.fetch()
    assert gridline.manifest()["published"].to_list() == [published_from_name(url)]
    # an unrelated CSV in the dataset is skipped, not fatal
    guide = "https://x/download/tec-guidance-notes.csv"
    ckan["result"]["resources"].insert(0, {"format": "CSV", "url": guide})
    gridline.fetch()
    assert gridline.manifest().height == 1
    # no date anywhere -> clear exit instead of a crash
    ckan["result"]["resources"][-1]["url"] = "https://x/download/tec-register.csv"
    with pytest.raises(SystemExit, match="no date"):
        gridline.fetch()


def test_archive_refuses_truncated_release(tmp_path, monkeypatch):
    real = (FIX / "tec-register-19-november-2026.csv").read_bytes()
    monkeypatch.chdir(tmp_path)
    archive(real, "full", published_from_name("19-november-2026"))
    # 2 of 10 rows: a cut-off download, not 8 withdrawals
    short = b"".join(real.splitlines(keepends=True)[:3])
    with pytest.raises(SystemExit, match="under half"):
        archive(short, "short", published_from_name("1-december-2026"))
    assert gridline.manifest().height == 1
    archive(short, "short", published_from_name("1-december-2026"), force=True)
    assert gridline.manifest()["rows"].to_list() == [10, 2]
    # Project ID column renamed: refuse rather than re-key every row by name
    no_ids = real.replace(b"Project ID", b"Official Project Ref")
    with pytest.raises(SystemExit, match="no Project IDs"):
        archive(no_ids, "renamed", published_from_name("2-december-2026"))


def row(**kw):
    return {**dict.fromkeys(gridline.ROW_SCHEMA), "project_name": "P", **kw}


def kinds(snaps):
    return sorted((e["key"], e["event_type"]) for e in build(snaps)[0])


def test_stage_change_is_a_new_key():
    # pins today's behaviour (see the ponytail note in index())
    d1, d2 = date(2026, 11, 19), date(2026, 12, 1)
    a = [row(project_id="X", stage=None)]
    b = [row(project_id="X", stage="1")]
    assert kinds([("s1", d1, a), ("s2", d2, b)]) == [
        ("X|", "withdrawn"),
        ("X|1", "added"),
    ]
