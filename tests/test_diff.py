"""tec-register-19-november-2026.csv: 10 verbatim lines of the real 19 Nov 2026 release.
tec-register-01-december-2026-EDITED.csv: the same rows with hand edits (only one release is retrievable):
PRO-001651 total 275 -> 300 MW; PRO-000368 date +91 days; PRO-000950 -> Built, 0 -> 35 MW connected;
PRO-003518 gate -> 2; PRO-000026 -> Under Construction; PRO-001624-1 connected 200 -> 250 MW (no event);
PRO-000398 -> Built with 0 MW (no `connected`) and MW Increase "TBC"; PRO-003578 removed;
PRO-002010 gets a second line with the same Project ID and blank Stage; one new PV row with no Project ID."""

import json
from datetime import date
from pathlib import Path

import polars as pl
import pytest

import gridline
from gridline import (
    archive,
    build,
    parse_ecr,
    parse_num,
    parse_tec,
    published_from_name,
    read_tec,
)

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
    assert published_from_name("ecr-19-sept-2026.csv") == date(2026, 9, 19)
    assert published_from_name("ecr-19-september-2026.csv") == date(2026, 9, 19)
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
        "result": {
            "resources": [
                {"format": None, "url": "https://x/readme"},
                {"format": "CSV", "url": url, "last_modified": None},
            ]
        }
    }
    monkeypatch.setattr(
        gridline,
        "http",
        lambda u: json.dumps(ckan).encode() if u == gridline.CKAN else real,
    )
    gridline.fetch(["tec"])
    assert gridline.manifest()["published"].to_list() == [published_from_name(url)]
    # an unrelated CSV in the dataset is skipped, not fatal
    guide = "https://x/download/tec-guidance-notes.csv"
    ckan["result"]["resources"].insert(0, {"format": "CSV", "url": guide})
    gridline.fetch(["tec"])
    assert gridline.manifest().height == 1
    # no date anywhere -> clear exit instead of a crash
    ckan["result"]["resources"][-1]["url"] = "https://x/download/tec-register.csv"
    with pytest.raises(SystemExit, match="no date"):
        gridline.fetch(["tec"])


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
    monkeypatch.setattr(gridline.sys, "argv", ["gridline", "backfill"])
    with pytest.raises(SystemExit, match="usage"):
        gridline.main()
    # staleness: newest release is 1 Dec
    gridline.check(date(2026, 12, 15), ["tec"])
    with pytest.raises(SystemExit, match="stale"):
        gridline.check(date(2026, 12, 16), ["tec"])
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


def test_build_paths_across_three_releases():
    d = [date(2026, 11, 19), date(2026, 11, 22), date(2026, 11, 26)]
    snaps = [
        ("s1", d[0], [row(project_id="A", gate="2"), row(project_id="B")]),
        # A: gate removed, date set from blank (neither emits today); B gone
        ("s2", d[1], [row(project_id="A", effective_from=d[2])]),
        # B back again; A's date cleared (emits nothing)
        ("s3", d[2], [row(project_id="A"), row(project_id="B")]),
    ]
    events, versions = build(snaps)
    assert [(e["snapshot_id"], e["key"], e["event_type"]) for e in events] == [
        ("s2", "B|", "withdrawn"),
        ("s3", "B|", "added"),
    ]
    # B has two closed-off intervals, not one spanning its absence
    b = [(v["valid_from"], v["valid_to"]) for v in versions if v["key"] == "B|"]
    assert sorted(b, key=str) == [(d[0], d[1]), (d[2], None)]
    # A changed twice (gate, date), so three versions
    assert len([v for v in versions if v["key"] == "A|"]) == 3


def test_ssen_ecr(tmp_path, monkeypatch):
    """ssen-ecr-1mw-*.csv: 10 rows each of SSEN's real Oct and Nov 2026 >=1 MW CSVs, with
    MPANs, customer name and address blanked. 7 sites are in both; 3 left, 3 arrived."""
    oct_ = (FIX / "ssen-ecr-1mw-12-october-2026.csv").read_bytes()
    nov = (FIX / "ssen-ecr-1mw-13-november-2026.csv").read_bytes()
    rows = {r["project_id"]: r for r in parse_ecr(nov)}
    assert len(rows) == 10
    r = rows["SSEN0703"]  # "SSEN0703-1MW-ACCEPTED TO CONNECT"
    assert (r["status"], r["mw_total"], r["effective_from"], r["connection_site"]) == (
        "accepted to connect",
        37.0,
        date(2026, 9, 30),
        "FETTERSSO GRID",
    )
    # connected: capacity from the other column; "DATA NOT AVAILABLE" date -> None
    r = rows["SSEN0001"]
    assert (r["status"], r["mw_total"], r["effective_from"]) == (
        "connected",
        4.275,
        None,
    )
    assert rows["SSEN0714"]["plant_type"].startswith("SOLAR;STORED ENERGY")
    assert gridline.parse_date("46336") == date(2026, 11, 10)
    assert gridline.parse_date("2024-03-07T00:00:00+00:00") == date(2024, 3, 7)
    assert gridline.parse_date("17/07/2026 00:00") == date(2026, 7, 17)

    # fetch: only the >=1 MW CSVs, oldest first, dated by last_modified
    url = "https://data-api.ssen.co.uk/dataset/x/resource/{}/download/{}".format
    res = [
        {
            "format": "XLSX",
            "url": url(1, "embedded-capacity-register-4.0_november_2026.xlsx"),
        },
        {
            "format": "CSV",
            "url": url(2, "ssen_ecr_part_1_1mw_november_2026_csv.csv"),
            "last_modified": "2026-11-13T10:33:35.355884",
        },
        {"format": "CSV", "url": url(3, "ssen_ecr_part_1_50kw_november_2026_csv.csv")},
        {
            "format": "CSV",
            "url": url(4, "ssen_ecr_part_1_1mw_october_2026_csv.csv"),
            "last_modified": "2026-10-12T16:05:19.020109",
        },
        {"format": "CSV", "url": url(5, "embedded-capacity-register-attributes.csv")},
    ]
    blobs = {
        gridline.SSEN_CKAN: json.dumps({"result": {"resources": res}}).encode(),
        res[1]["url"]: nov,
        res[3]["url"]: oct_,
    }
    monkeypatch.setattr(gridline, "http", blobs.__getitem__)
    monkeypatch.chdir(tmp_path)
    gridline.fetch(["ssen"])
    assert gridline.manifest("ssen")["published"].to_list() == [
        date(2026, 10, 12),
        date(2026, 11, 13),
    ]
    gridline.diff()
    ev = pl.read_parquet("data/event.parquet")
    assert set(ev["register"]) == {"ssen"}
    assert sorted(zip(ev["event_type"], ev["project_id"])) == [
        ("added", "SSEN1514"),
        ("added", "SSEN1515"),
        ("added", "SSEN1516"),
        ("removed_from_register", "SSEN0737"),
        ("removed_from_register", "SSEN0756"),
        ("removed_from_register", "SSEN1265"),
    ]
    assert not ev["low_confidence"].any()


def test_npg_ecr(tmp_path, monkeypatch):
    """npg-ecr-1mw-13-november-2026.csv: 5 connected + 5 accepted rows of NPg's real
    Nov 2026 export (1 MW and over), with MPANs, customer name and address blanked."""
    blob = (FIX / "npg-ecr-1mw-13-november-2026.csv").read_bytes()
    rows = {r["project_id"]: r for r in parse_ecr(blob)}
    assert len(rows) == 10
    r = rows["NPg15-000863"]
    assert (r["project_name"], r["status"], r["mw_total"], r["connection_site"]) == (
        "Maiden Hill Farm BESS",
        "accepted to connect",
        99.4,
        "Stella South",
    )
    assert rows["NPg23-001124"]["mw_total"] == 18.07
    # NPg never fills Target Energisation Date ("Data Not Available")
    assert all(r["effective_from"] is None for r in rows.values())

    meta = {"metas": {"default": {"data_processed": "2026-11-13T16:54:55+00:00"}}}
    export = f"{gridline.NPG}/exports/csv?delimiter=%2C"
    blobs = {gridline.NPG: json.dumps(meta).encode(), export: blob.splitlines()[0]}
    monkeypatch.setattr(gridline, "http", blobs.__getitem__)
    monkeypatch.chdir(tmp_path)
    # a login-gated portal answers HTTP 200 with the header only
    with pytest.raises(SystemExit, match="0 data rows"):
        gridline.fetch(["npg"])
    blobs[export] = blob
    gridline.fetch(["npg"])
    # a real shrink (e.g. projects moved to Gate 1) is refused, then forced through
    blobs[export] = b"\r\n".join(blob.splitlines()[:4])
    blobs[gridline.NPG] = json.dumps(meta).replace("11-13", "12-11").encode()
    with pytest.raises(SystemExit, match="under half"):
        gridline.fetch(["npg"])
    monkeypatch.setattr(gridline.sys, "argv", ["gridline", "fetch", "--force", "npg"])
    gridline.main()
    assert gridline.manifest("npg")["rows"].to_list() == [10, 3]
    # a crash inside one source is reported, not raised past the others
    blobs[gridline.NPG] = b"[]"  # TypeError, not caught before
    with pytest.raises(SystemExit, match="npg: list indices"):
        gridline.fetch(["npg"])
