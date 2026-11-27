"""tec-register-19-november-2026.csv: 10 verbatim lines of the real 19 Nov 2026 release.
tec-register-01-december-2026-EDITED.csv: the same rows with hand edits (only one release is retrievable):
PRO-001651 total 275 -> 300 MW; PRO-000368 date +91 days; PRO-000950 -> Built, 0 -> 35 MW connected;
PRO-003518 gate -> 2; PRO-000026 -> Under Construction; PRO-001624-1 connected 200 -> 250 MW (no event);
PRO-000398 -> Built with 0 MW (no `connected`) and MW Increase "TBC"; PRO-003578 removed;
PRO-002010 gets a second line with the same Project ID and blank Stage; one new PV row with no Project ID."""

import csv
import hashlib
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
        lambda u, h=None: json.dumps(ckan).encode() if u == gridline.CKAN else real,
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


def test_stage_split_stays_one_project():
    """tec-stage-split-*.csv: the real PRO-000366 rows. On 19 Nov it is one row with no
    Stage; on 23 Nov the same Project ID has Stage 1 (1,365 MW, Built) and Stage 2
    (1,400 MW, Scoping)."""
    snaps = [
        (p.stem, published_from_name(p.name), read_tec(p))
        for p in sorted(FIX.glob("tec-stage-split-*.csv"))
    ]
    events, _ = build(snaps)
    pid = "a0l4L0000005itwQAA"
    assert [(e["event_type"], e["old_value"], e["new_value"]) for e in events] == [
        ("stage_split", f"{pid}|", f"{pid}|1;{pid}|2")
    ]
    # a lone rename (blank -> "1") is not a split: still withdrawn + added
    d1, d2 = date(2026, 11, 19), date(2026, 12, 1)
    a = [row(project_id="X", stage=None)]
    b = [row(project_id="X", stage="1")]
    assert kinds([("s1", d1, a), ("s2", d2, b)]) == [
        ("X|", "withdrawn"),
        ("X|1", "added"),
    ]
    # ECR rows (no Stage) repeated under one ID are not a split
    c = [row(project_id="S"), row(project_id="S")]
    assert kinds([("s1", d1, [row(project_id="S")]), ("s2", d2, c)]) == [
        ("S|#2", "added")
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
    monkeypatch.setattr(gridline, "http", lambda u, h=None: blobs[u])
    monkeypatch.chdir(tmp_path)
    gridline.fetch(["ssen"])
    assert gridline.manifest("ssen")["published"].to_list() == [
        date(2026, 10, 12),
        date(2026, 11, 13),
    ]
    gridline.diff()
    ev = pl.read_parquet("data/event.parquet")
    assert set(ev["register"]) == {"ssen"}
    assert sorted(zip(ev["event_type"], ev["source_id"])) == [
        ("added", "SSEN1514"),
        ("added", "SSEN1515"),
        ("added", "SSEN1516"),
        ("removed_from_register", "SSEN0737"),
        ("removed_from_register", "SSEN0756"),
        ("removed_from_register", "SSEN1265"),
    ]
    assert not ev["low_confidence"].any()
    # every row is a project of its own, under an ID that a rebuild keeps
    links = pl.read_parquet("data/project_link.parquet")
    assert links.height == pl.read_parquet("data/project.parquet").height == 13
    assert set(ev["project_id"]) <= set(links["project_id"])
    ledger = Path("archive/project_ids.csv").read_text()
    gridline.diff()
    assert Path("archive/project_ids.csv").read_text() == ledger
    assert pl.read_parquet("data/project_link.parquet").equals(links)
    assert Path("data/event.csv").exists()


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
    monkeypatch.setattr(gridline, "http", lambda u, h=None: blobs[u])
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


def test_ssen_xlsx_matches_csv():
    """ssen-ecr-1mw-13-november-2026.xlsx: the same 10 rows as the CSV fixture, taken from
    SSEN's real Nov 2026 workbook (sheet "Register Part 1 - >=1MW", title row above the
    header, typed cells), personal fields blanked. The workbook has no Unique ID column."""
    xlsx = parse_ecr((FIX / "ssen-ecr-1mw-13-november-2026.xlsx").read_bytes())
    csv_ = parse_ecr((FIX / "ssen-ecr-1mw-13-november-2026.csv").read_bytes())
    assert [{**r, "project_id": None} for r in csv_] == xlsx


def test_release_hosted_snapshot_wrong_hash_is_refused(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    blob = (FIX / "ssen-ecr-1mw-13-november-2026.xlsx").read_bytes()
    url = "https://github.com/o/r/releases/download/h/ssen-ecr-13-november-2026.xlsx"
    monkeypatch.setattr(gridline, "http", lambda u, h=None: blob)
    good = {"raw_path": url, "sha256": gridline.hashlib.sha256(blob).hexdigest()}
    assert gridline.raw(good) == blob  # downloaded, verified, cached
    with pytest.raises(ValueError, match="sha256"):
        gridline.raw({"raw_path": url, "sha256": "0" * 64})
    assert sorted(p.name for p in (tmp_path / "data" / "cache").iterdir()) == [
        good["sha256"]
    ]


def test_history_without_ids_borrows_them():
    # workbook month (no IDs) then CSV month (IDs): one project, not removed + added
    d1, d2 = date(2026, 9, 14), date(2026, 10, 12)
    site = {"project_name": "Three Lochs Hydro", "connection_site": "GRUDIE BRIDGE"}
    old = [row(**site, mw_total=2.0), row(project_name="Gone", mw_total=1.0)]
    new = [row(**site, mw_total=2.5, project_id="SSEN0711")]
    events, _ = build([("x", d1, old), ("c", d2, new)])
    assert [(e["key"], e["event_type"], e["low_confidence"]) for e in events] == [
        ("n:gone|||", "withdrawn", True),
        ("SSEN0711|", "capacity_changed", True),
    ]


def test_key_gated_sources(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    calls = []
    blob = (FIX / "npg-ecr-1mw-13-november-2026.csv").read_bytes()
    meta = {"metas": {"default": {"data_processed": "2026-11-13T11:07:15+00:00"}}}

    def http(url, headers=None):
        calls.append((url, headers))
        return json.dumps(meta).encode() if url == gridline.UKPN else blob

    monkeypatch.setattr(gridline, "http", http)
    monkeypatch.delenv("UKPN_API_KEY", raising=False)
    gridline.fetch(["ukpn"])  # no key: skipped, not failed
    gridline.check(date(2026, 11, 24), ["ukpn"])
    assert calls == [] and gridline.manifest("ukpn") is None
    monkeypatch.setenv("UKPN_API_KEY", "secret-123")
    gridline.fetch(["ukpn"])
    assert {h["Authorization"] for _, h in calls} == {"Apikey secret-123"}
    assert "secret-123" not in gridline.manifest("ukpn")["source"][0]


def test_gsp_renamed_by_the_register_keeps_the_key():
    # SSEN switched from GSP codes to names in Oct 2023
    assert gridline.gsp("MELK") == gridline.gsp("MELKSHAM GSP") == "MELK_1"
    assert gridline.gsp("BERB2") == gridline.gsp("BERRYBURN GRID")  # not in NESO's list
    # SSEN's old BRAC is Braco, a different GSP from Braco West
    assert gridline.gsp("BRAC") != gridline.gsp("BRACO WEST GRID")
    assert gridline.gsp("Somewhere New GSP") == "somewhere new"  # unknown: its name
    d1, d2 = date(2023, 9, 15), date(2023, 10, 15)
    site = {"project_name": "Rosebank Farm", "plant_type": "SOLAR", "mw_total": 5.0}
    old = [row(**site, connection_site="MELK")]
    new = [row(**site, connection_site="MELKSHAM GSP")]
    assert kinds([("s1", d1, old), ("s2", d2, new)]) == []


def test_npg_workbook_site_id_and_repeated_names():
    # NPg workbooks since May 2026 carry the CSV's ID as "Unique Site Identifier"
    blob = (FIX / "npg-ecr-1mw-13-november-2026.csv").read_bytes()
    blob = blob.replace(b"ecrid,", b"Unique Site Identifier,", 1)
    assert parse_ecr(blob)[0]["project_id"].startswith("NPg")
    # two rows, one name: the ID is borrowed by capacity, not by file order
    d1, d2 = date(2026, 4, 14), date(2026, 5, 14)
    a, b = (
        row(project_name="Pillswood Farm", mw_total=49.9),
        row(project_name="Pillswood Farm", mw_total=20.0),
    )
    old = [a, b]
    new = [{**b, "project_id": "N2"}, {**a, "project_id": "N1"}]
    assert kinds([("s1", d1, old), ("s2", d2, new)]) == []


def test_renamed_rows_continue_as_one():
    d = [date(2024, 6, 15), date(2024, 7, 10), date(2024, 8, 8)]
    site = {"connection_site": "Knaresborough", "plant_type": "Waste", "mw_total": 2.0}
    a = row(project_name="Alne Material Recycling Facility", **site)
    b = row(project_name="Alne Materials Recycling Facility", **site)
    # a respelt name continues the row, and the next release keeps its key
    events, versions = build([("s1", d[0], [a]), ("s2", d[1], [b]), ("s3", d[2], [b])])
    assert [(e["event_type"], e["new_value"], e["low_confidence"]) for e in events] == [
        ("renamed", "Alne Materials Recycling Facility", True)
    ]
    assert len({v["key"] for v in versions}) == 1
    # the same export meter links two unrelated-looking names
    old = [row(project_name="Kingspan", mpan="2380000303419", **site)]
    new = [row(project_name="Hurricane Park", mpan="2380000303419", **site)]
    assert [
        e["event_type"] for e in build([("s1", d[0], old), ("s2", d[1], new)])[0]
    ] == ["renamed"]
    # a placeholder name replaced by the site name: same GSP, technology, MW, date, status
    old = [row(project_name="49.9MW BATTERY", effective_from=d[2], **site)]
    new = [row(project_name="Drum Farm BESS", effective_from=d[2], **site)]
    assert [
        e["event_type"] for e in build([("s1", d[0], old), ("s2", d[1], new)])[0]
    ] == ["renamed"]


def test_different_projects_are_not_relinked():
    d1, d2 = date(2024, 6, 15), date(2024, 7, 10)
    site = {"connection_site": "CARRADALE", "plant_type": "WIND", "mw_total": 47.5}
    # another phase number is another project, even when everything else matches
    old = [row(project_name="Beinn an Tuirc 2", **site)]
    new = [row(project_name="Beinn an Tuirc 3", **site)]
    assert kinds([("s1", d1, old), ("s2", d2, new)]) == [
        ("n:beinn an tuirc 2|CAAD_P|wind|", "withdrawn"),
        ("n:beinn an tuirc 3|CAAD_P|wind|", "added"),
    ]
    # two rows with their own IDs: the IDs decide, however alike the rest is
    old = [row(project_name="Glen Farm", project_id="SSEN0001", **site)]
    new = [row(project_name="Glen Farm", project_id="SSEN0002", **site)]
    assert kinds([("s1", d1, old), ("s2", d2, new)]) == [
        ("SSEN0001|", "withdrawn"),
        ("SSEN0002|", "added"),
    ]


def test_project_ids_are_stable():
    d1, d2, d3 = date(2023, 8, 1), date(2024, 1, 1), date(2025, 1, 1)
    minted = {("tec", "A|"): d2, ("ssen", "S1|"): d1, ("ssen", "S2|"): d3}
    ids, _, ledger = gridline.assign_ids(minted, [], [])
    assert len(set(ids.values())) == 3
    # a second build reads the ledger and changes nothing; so does a build with no ledger
    assert gridline.assign_ids(minted, [], ledger)[0] == ids
    assert gridline.assign_ids(minted, [], [])[0] == ids
    # TEC row A links to both SSEN rows: the merged project keeps the oldest ID (S1's)
    edges = [
        (("tec", "A|"), ("ssen", s), "rule", 0.9, "same name") for s in ("S1|", "S2|")
    ]
    merged, how, ledger2 = gridline.assign_ids(minted, edges, ledger)
    assert set(merged.values()) == {ids["ssen", "S1|"]}
    assert how["ssen", "S2|"] == ("rule", 0.9, "same name")
    old = {r["key"]: r for r in ledger2}
    assert (
        old["A|"]["project_id"] == ids["tec", "A|"]
    )  # the first ID is never forgotten
    assert old["A|"]["merged_into"] == ids["ssen", "S1|"]
    # the link is sticky: a later build without the edge keeps it
    assert gridline.assign_ids(minted, [], ledger2)[0] == merged
    # unless an override separates a row: it takes back its own first ID
    apart, *_ = gridline.assign_ids(minted, [], ledger2, {("ssen", "S2|")})
    assert apart["ssen", "S2|"] == ids["ssen", "S2|"]
    assert apart["tec", "A|"] == merged["tec", "A|"]
    # a row the matcher now links to another project moves: the two projects never merge
    minted[("tec", "B|")] = d3
    moved, *_ = gridline.assign_ids(
        minted, [(("tec", "B|"), ("ssen", "S2|"), "rule", 0.9, "")], ledger2
    )
    assert moved["ssen", "S2|"] == moved["tec", "B|"] != moved["tec", "A|"]


def ver(register, key, **kw):
    """A register row's only version, as diff() hands it to match()."""
    v = {**dict.fromkeys(gridline.VERSION_SCHEMA), "register": register, "key": key}
    return {**v, "valid_from": date(2026, 11, 19), **kw}


def tec_ver(name, site, mw, status, **kw):
    return ver(
        "tec",
        f"{name}|",
        project_name=name,
        connection_site=site,
        mw_total=mw,
        status=status,
        agreement_type="Embedded",
        host_to="SHET",
        **kw,
    )


def ssen_ver(name, site, mw, status, **kw):
    connected = mw if status == "connected" else 0
    return ver(
        "ssen",
        f"n:{name}",
        project_name=name,
        connection_site=site,
        mw_total=mw,
        mw_connected=connected,
        status=status,
        **kw,
    )


MATCH_ROWS = [
    tec_ver(
        "Achanalt Battery Storage",
        "Grudie Bridge GSP",
        49.9,
        "Scoping",
        plant_type="Energy Storage System",
        customer_name="ADTEK LIMITED",
    ),
    ssen_ver(
        "ACHANALT BATTERY ENERGY STORAGE SYSTEM",
        "GRUDIE BRIDGE",
        49.9,
        "accepted to connect",
        plant_type="STORED ENERGY",
        customer_name="ADTEK LTD",
    ),
    tec_ver(
        "Beinn an Tuirc 2", "Carradale 1 GSP", 43.7, "Built", plant_type="Wind Onshore"
    ),
    tec_ver(
        "Beinn an Tuirc 3", "Carradale 1 GSP", 50.0, "Built", plant_type="Wind Onshore"
    ),
    ssen_ver("BEINN AN TUIRC 2", "CARRADALE", 43.7, "connected", plant_type="WIND"),
    ssen_ver(
        "BEINN AN TUIRC 3 WINDFARM", "CARRADALE", 47.5, "connected", plant_type="WIND"
    ),
    # the TEC row is a new scheme at Scoping; the ECR row is the farm built years ago
    tec_ver(
        "Novar 2 Wind Farm", "Alness GSP", 43.2, "Scoping", plant_type="Wind Onshore"
    ),
    ssen_ver(
        "NOVAR 2 WIND FARM",
        "ALNESS",
        36.8,
        "connected",
        plant_type="WIND",
        customer_name="JOHN SMITH",
    ),
]


def jev_reply(p_same, p_related=None):
    """A Jev answer as the API returns it."""
    p_related = 1 - p_same if p_related is None else p_related
    probs = {"0": round(1 - p_same - p_related, 2), "1": p_related, "2": p_same}
    return {
        "model": gridline.JEV_MODEL,
        "usage": {"input_tokens": 1000},
        "answers": {"link_state": {"probabilities": probs}},
    }


def linked(edges):
    return sorted((a[1], b[1], method) for a, b, method, *_ in edges)


def test_match_tec_to_ecr(tmp_path, monkeypatch):
    monkeypatch.setattr(gridline, "ANSWERS", tmp_path / "jev_answers.csv")
    monkeypatch.setattr(gridline, "OVERRIDES", tmp_path / "overrides.csv")
    # no key: rule links stand, the rest are possible, and the build still runs
    monkeypatch.setattr(gridline, "jev_key", lambda: None)
    edges, possible, _, counts = gridline.match(MATCH_ROWS)
    assert linked(edges) == [
        (
            "Achanalt Battery Storage|",
            "n:ACHANALT BATTERY ENERGY STORAGE SYSTEM",
            "rule",
        ),
        ("Beinn an Tuirc 2|", "n:BEINN AN TUIRC 2", "rule"),
        ("Beinn an Tuirc 3|", "n:BEINN AN TUIRC 3 WINDFARM", "rule"),
    ]
    assert counts["jev asked now"] == 0 and not (tmp_path / "jev_answers.csv").exists()
    novar = [x for x in possible if x[0][1] == "Novar 2 Wind Farm|"]
    assert [(x[1][1], x[2]) for x in novar] == [("n:NOVAR 2 WIND FARM", None)]
    assert "rules only" in novar[0][3] and "TEC row not started" in novar[0][3]

    # with a key: Jev is asked once per pair, and its answers are kept for the next build
    calls = []

    def fake_post(body, key):
        calls.append(json.loads(body))
        return jev_reply(0.95, 0.03)

    monkeypatch.setattr(gridline, "jev_key", lambda: "test-key")
    monkeypatch.setattr(gridline, "jev_post", fake_post)
    edges, possible, _, counts = gridline.match(MATCH_ROWS)
    asked = len(calls)
    assert asked == counts["jev asked now"] > 0
    # a person's name is never sent; a company's is
    sent = [c["state"]["ecr"].get("customer") for c in calls]
    assert "ADTEK LTD" in sent and "JOHN SMITH" not in sent
    assert "test-key" not in (tmp_path / "jev_answers.csv").read_text()
    # even at P(same) 0.95 the built Novar is not linked to the new scheme
    assert [x[2] for x in possible if x[0][1] == "Novar 2 Wind Farm|"] == [0.95]
    assert "n:NOVAR 2 WIND FARM" not in {b[1] for _, b, *_ in edges}
    # cached answers: a rebuild makes no request at all, and gives the same links
    monkeypatch.setattr(gridline, "jev_post", lambda *a: pytest.fail("network call"))
    assert linked(gridline.match(MATCH_ROWS)[0]) == linked(edges)

    # overrides beat rules and Jev; one that names no row stops the build
    (tmp_path / "overrides.csv").write_text(
        "action,a,b,why\n"
        "separate,tec:Achanalt Battery Storage|,ssen:n:ACHANALT BATTERY ENERGY STORAGE SYSTEM,test\n"
        "link,tec:Novar 2 Wind Farm|,ssen:n:NOVAR 2 WIND FARM,test\n"
        "link,tec:Beinn an Tuirc 3|,ssen:n:BEINN AN TUIRC 2,test\n"
    )
    edges, possible, apart, _ = gridline.match(MATCH_ROWS)
    # BEINN AN TUIRC 2 goes where the override says, not also to its rule match
    assert linked(edges) == [
        ("Beinn an Tuirc 3|", "n:BEINN AN TUIRC 2", "override"),
        ("Beinn an Tuirc 3|", "n:BEINN AN TUIRC 3 WINDFARM", "rule"),
        ("Novar 2 Wind Farm|", "n:NOVAR 2 WIND FARM", "override"),
    ]
    assert ("ssen", "n:ACHANALT BATTERY ENERGY STORAGE SYSTEM") in apart
    assert not any(x[1][1].startswith("n:ACHANALT") for x in possible)
    (tmp_path / "overrides.csv").write_text(
        "action,a,b,why\nlink,tec:Gone|,ssen:n:X,test\n"
    )
    with pytest.raises(SystemExit, match="no such row"):
        gridline.match(MATCH_ROWS)


def test_each_ecr_row_goes_to_its_best_match(tmp_path, monkeypatch):
    """Two batteries at one GSP: Jev is a little surer of the crossed pair, but the names decide."""
    monkeypatch.setattr(gridline, "ANSWERS", tmp_path / "jev_answers.csv")
    monkeypatch.setattr(gridline, "jev_key", lambda: "test-key")

    def fake_post(body, key):
        b = json.loads(body)["state"]
        crossed = ("west" in b["tec"]["name"].lower()) != (
            "west" in b["ecr"]["name"].lower()
        )
        return jev_reply(0.96 if crossed else 0.87)

    monkeypatch.setattr(gridline, "jev_post", fake_post)
    site = "Braco West 275/33kV GSP Substation"
    rows = [
        tec_ver(
            "Braco Battery",
            site,
            49.9,
            "Consents Approved",
            plant_type="Energy Storage System",
        ),
        tec_ver(
            "Braco West BESS",
            site,
            49.9,
            "Consents Approved",
            plant_type="Energy Storage System",
        ),
        ssen_ver(
            "BRACO BATTERY",
            "BRACO WEST GRID",
            49.9,
            "accepted to connect",
            plant_type="STORED ENERGY",
        ),
        ssen_ver(
            "BRACO WEST BESS",
            "BRACO WEST GRID",
            49.9,
            "accepted to connect",
            plant_type="STORED ENERGY",
        ),
    ]
    assert [(a, b) for a, b, _ in linked(gridline.match(rows)[0])] == [
        ("Braco Battery|", "n:BRACO BATTERY"),
        ("Braco West BESS|", "n:BRACO WEST BESS"),
    ]


def test_rows_with_the_same_fields_share_one_answer(tmp_path, monkeypatch):
    """Two ECR rows alike in every field Jev sees send one request; both pairs get its answer."""
    monkeypatch.setattr(gridline, "ANSWERS", tmp_path / "jev_answers.csv")
    monkeypatch.setattr(gridline, "jev_key", lambda: "test-key")
    posts = []

    def fake_post(body, key):
        posts.append(body)
        return jev_reply(0.6)

    monkeypatch.setattr(gridline, "jev_post", fake_post)
    tec = tec_ver(
        "Kincorth BESS",
        "Craigiebuckler GSP",
        49.9,
        "Scoping",
        plant_type="Energy Storage System",
    )
    one = ssen_ver(
        "KINCORTH",
        "CRAIGIEBUCKLER",
        45.0,
        "accepted to connect",
        plant_type="STORED ENERGY",
    )
    two = {**one, "key": "n:KINCORTH#2"}
    edges, possible, _, _ = gridline.match([tec, one, two])
    assert len(posts) == 1
    probs = [x[3] for x in edges] + [x[2] for x in possible]
    assert probs == [0.6, 0.6]


def test_answers_survive_a_failed_request(tmp_path, monkeypatch):
    """Answers cost money: those that arrived are saved even when a later request fails."""
    monkeypatch.setattr(gridline, "ANSWERS", tmp_path / "jev_answers.csv")
    monkeypatch.setattr(gridline, "OVERRIDES", tmp_path / "overrides.csv")
    monkeypatch.setattr(gridline, "jev_key", lambda: "test-key")

    def fake_post(body, key):
        if "BEINN AN TUIRC 3" in body:
            raise RuntimeError("Jev answered HTTP 500")
        return jev_reply(0.9)

    monkeypatch.setattr(gridline, "jev_post", fake_post)
    with pytest.raises(RuntimeError):
        gridline.match(MATCH_ROWS)
    assert len(gridline.read_answers()) > 0


def test_match_quality():
    """reference/labels.csv: TEC/ECR pairs labelled by hand with the evidence. Rebuilds each
    decision from the stored fields and Jev's committed answers, and fails below the bar.
    Pairs labelled unsure are left out."""
    fields = ("name", "site", "plant", "mw", "connected", "status", "gate", "date")
    fields += ("customer", "host", "register", "key")

    def side(r, prefix):
        x = {f: r[f"{prefix}_{f}"] or None for f in fields}
        for f in ("mw", "connected"):
            x[f] = x[f] and float(x[f])
        x["date"] = x["date"] and date.fromisoformat(x["date"])
        return x

    answers = gridline.read_answers()
    pairs = []
    with (gridline.REFERENCE / "labels.csv").open(encoding="utf-8") as f:
        for r in csv.DictReader(f):
            if r["unsure"]:
                continue
            t, e = side(r, "tec"), side(r, "ecr")
            body = gridline.jev_request(t, e)
            a = answers.get(hashlib.sha256(body.encode()).hexdigest())
            p = a and float(a["p_same"])
            blocked = gridline.blocked(t, e)
            pairs.append(
                {
                    "label": r["label"],
                    "stratum": r["stratum"],
                    "blocked": blocked,
                    "out": gridline.decide(t, e, p)[0] if blocked else None,
                    "ecr": e["key"],
                    "rank": gridline.rank(p, gridline.score(t, e)[0]),
                }
            )
    # as in match(): an ECR row keeps only its best link (here, among the labelled pairs)
    best = {}
    for x in pairs:
        if x["out"] == "link":
            best[x["ecr"]] = max(best.get(x["ecr"], x["rank"]), x["rank"])
    for x in pairs:
        if x["out"] == "link" and x["rank"] < best[x["ecr"]]:
            x["out"] = None
    links = [x["label"] for x in pairs if x["out"] == "link"]
    same = [x for x in pairs if x["label"] == "same"]
    hunted = [x["blocked"] for x in same if x["stratum"] == "hunted"]
    precision = links.count("same") / len(links)  # automatic links that are right
    review = sum(x["out"] is not None for x in same) / len(same)  # linked or possible
    blocking = sum(hunted) / len(hunted)  # pairs found by hand that blocking scores
    print(f"precision {precision:.1%} of {len(links)}")
    print(f"review recall {review:.1%} of {len(same)}")
    print(f"blocking recall {blocking:.1%} of {len(hunted)}")
    assert precision >= 0.97
    assert review >= 0.90
    assert blocking >= 0.85


def test_embedded_register(tmp_path, monkeypatch):
    """embedded-register-23-november-2026.csv: the first 5 rows of NESO's Embedded Register
    (embedded projects in Scotland with a transmission agreement), verbatim."""
    blob = (FIX / "embedded-register-23-november-2026.csv").read_bytes()
    rows = parse_tec(blob)
    assert len(rows) == 5
    r = rows[0]
    assert (r["project_name"], r["project_number"], r["host_to"], r["gate"]) == (
        "180/181 Clynelish Muir BESS",
        "PRO-003597",
        "SHET",
        "1",
    )
    assert r["mw_total"] == 50 and r["effective_from"] == date(2030, 10, 31)
    url = "https://api.neso.energy/dataset/x/resource/y/download/embedded-register-23-november-2026.csv"
    res = [{"format": "CSV", "url": url}, {"format": "PDF", "url": "guidance.pdf"}]
    blobs = {
        gridline.EMBEDDED_CKAN: json.dumps({"result": {"resources": res}}).encode(),
        url: blob,
    }
    monkeypatch.setattr(gridline, "http", lambda u, h=None: blobs[u])
    monkeypatch.chdir(tmp_path)
    gridline.fetch(["embedded"])
    assert gridline.manifest("embedded")["published"].to_list() == [date(2026, 11, 23)]
    # its rows are matched to ECR rows like TEC rows: Clynelish Muir is in SSEN's north
    monkeypatch.setattr(gridline, "ANSWERS", tmp_path / "jev_answers.csv")
    monkeypatch.setattr(gridline, "jev_key", lambda: None)
    clynelish = ver("embedded", f"{r['project_id']}|", **r)
    ssen = ssen_ver(
        "CLYNELISH MUIR BESS",
        "BRORA",
        50.0,
        "accepted to connect",
        plant_type="STORED ENERGY",
    )
    assert linked(gridline.match([clynelish, ssen])[0]) == [
        (clynelish["key"], "n:CLYNELISH MUIR BESS", "rule")
    ]
