"""Gridline Phase 0: archive NESO TEC register releases and diff them into typed events."""

import csv
import hashlib
import io
import json
import re
import sys
import urllib.request
from collections import Counter
from datetime import date, datetime, timezone
from pathlib import Path

import polars as pl

DATA = Path("data")  # derived Parquet, gitignored
RAW = Path("archive") / "tec"  # raw releases + manifest, committed: NESO keeps no past releases
MANIFEST = RAW / "snapshots.csv"
CKAN = "https://api.neso.energy/api/3/action/package_show?id=transmission-entry-capacity-tec-register"

# lower-cased CSV header -> our column name
COLS = {
    "project name": "project_name", "customer name": "customer_name", "connection site": "connection_site",
    "stage": "stage", "mw connected": "mw_connected", "mw increase / decrease": "mw_change",
    "cumulative total capacity (mw)": "mw_total", "mw effective from": "effective_from",
    "project status": "status", "agreement type": "agreement_type", "host to": "host_to",
    "plant type": "plant_type", "project id": "project_id", "project number": "project_number", "gate": "gate",
}
NUM = ("mw_connected", "mw_change", "mw_total")
ROW_SCHEMA = {c: pl.Float64 if c in NUM else pl.Date if c == "effective_from" else pl.String for c in COLS.values()}
SNAP_SCHEMA = {"snapshot_id": pl.String, "source": pl.String, "fetched_at": pl.Datetime("us"),  # naive UTC: Windows lacks tzdata
               "published": pl.Date, "sha256": pl.String, "raw_path": pl.String, "rows": pl.Int64}
EVENT_SCHEMA = {"snapshot_id": pl.String, "prev_snapshot_id": pl.String, "published": pl.Date, "key": pl.String,
                "project_id": pl.String, "project_number": pl.String, "project_name": pl.String,
                "connection_site": pl.String, "event_type": pl.String, "field": pl.String, "old_value": pl.String,
                "new_value": pl.String, "days": pl.Int64, "low_confidence": pl.Boolean}
VERSION_SCHEMA = {"key": pl.String, **ROW_SCHEMA, "low_confidence": pl.Boolean, "snapshot_id": pl.String,
                  "valid_from": pl.Date, "valid_to": pl.Date}


def http(url: str) -> bytes:
    req = urllib.request.Request(url, headers={"User-Agent": "gridline/0.1"})
    with urllib.request.urlopen(req, timeout=120) as r:
        return r.read()


def published_from_name(name: str) -> date | None:
    """'tec-register-19-november-2026.csv' -> 2026-11-19."""
    m = re.search(r"(\d{1,2})[-_ ]+([a-z]+)[-_ ]+(\d{4})", name.lower())
    for fmt in ("%d %B %Y", "%d %b %Y"):
        try:
            return datetime.strptime(" ".join(m.groups()), fmt).date() if m else None
        except ValueError:
            pass
    return None


def parse_date(s: str | None) -> date | None:
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%b-%Y", "%d/%m/%y"):
        try:
            return datetime.strptime(s, fmt).date() if s else None
        except ValueError:
            pass
    return None  # ponytail: unparseable dates become null; the raw file keeps the original text


def parse_num(s: str | None) -> float | None:
    try:
        return float(s.replace(",", "")) if s else None
    except ValueError:
        return None  # e.g. "TBC"; the raw file keeps the original text


def read_tec(path) -> list[dict]:
    b = Path(path).read_bytes()
    try:
        text = b.decode("utf-8-sig")
    except UnicodeDecodeError:
        text = b.decode("cp1252")
    rows = []
    for raw in csv.DictReader(io.StringIO(text)):
        r = dict.fromkeys(COLS.values())
        for k, v in raw.items():
            if k and k.strip().lower() in COLS:
                r[COLS[k.strip().lower()]] = (v or "").strip() or None
        if not any(r.values()):
            continue
        for c in NUM:
            r[c] = parse_num(r[c])
        r["effective_from"] = parse_date(r["effective_from"])
        rows.append(r)
    return rows


def manifest() -> pl.DataFrame | None:
    return pl.read_csv(MANIFEST, schema=SNAP_SCHEMA) if MANIFEST.exists() else None


def save(name: str, rows, schema) -> None:
    DATA.mkdir(parents=True, exist_ok=True)
    df = rows if isinstance(rows, pl.DataFrame) else pl.DataFrame(rows, schema=schema, orient="row")
    df.write_parquet(DATA / f"{name}.parquet")
    print(f"wrote {DATA / name}.parquet ({df.height} rows)")


def archive(blob: bytes, source: str, published: date | None) -> None:
    sha = hashlib.sha256(blob).hexdigest()
    snaps = manifest()
    if snaps is not None and sha in snaps["sha256"].to_list():
        print(f"skip {source}: already archived as {sha[:12]}")
        return
    RAW.mkdir(parents=True, exist_ok=True)
    path = RAW / f"{sha}.csv"
    path.write_bytes(blob)
    row = {"snapshot_id": sha[:12], "source": source, "fetched_at": datetime.now(timezone.utc).replace(tzinfo=None),
           "published": published, "sha256": sha, "raw_path": path.as_posix(), "rows": len(read_tec(path))}
    new = pl.DataFrame([row], schema=SNAP_SCHEMA)
    (new if snaps is None else pl.concat([snaps, new])).write_csv(MANIFEST)
    print(f"archived {source} -> {path} ({row['rows']} rows, published {published})")


def fetch() -> None:
    """Archive every CSV resource the CKAN dataset currently lists (NESO only exposes the latest release)."""
    for res in json.loads(http(CKAN))["result"]["resources"]:
        if res["format"].upper() == "CSV":
            published = published_from_name(res["url"]) or date.fromisoformat(res["last_modified"][:10])
            archive(http(res["url"]), res["url"], published)


def backfill(sources: list[str]) -> None:
    """Archive past releases from local files or URLs; the release date must be in the file name."""
    for src in sources:
        published = published_from_name(Path(src).name)
        if not published:
            sys.exit(f"{src}: no date in file name, rename it like tec-register-28-february-2025.csv")
        archive(http(src) if src.startswith("http") else Path(src).read_bytes(), src, published)


def norm(s: str | None) -> str:
    return re.sub(r"[^a-z0-9]+", " ", (s or "").lower()).strip()


def index(rows: list[dict]) -> dict[str, dict]:
    """Key each row. Project ID is case-sensitive (per NESO), so it is not lower-cased.
    Rows without a Project ID, or whose key repeats inside one release, are low_confidence."""
    base = [f"{r['project_id']}|{r['stage'] or ''}" if r["project_id"]
            else "n:" + "|".join(norm(r[c]) for c in ("project_name", "connection_site", "plant_type", "stage"))
            for r in rows]
    total, seen, out = Counter(base), Counter(), {}
    for k, r in zip(base, rows):
        seen[k] += 1
        # ponytail: repeated keys are paired across releases by file order; Splink-style matching is a later phase
        key = k if seen[k] == 1 else f"{k}#{seen[k]}"
        out[key] = {**r, "key": key, "low_confidence": total[k] > 1 or not r["project_id"]}
    return out


def changes(a: dict, b: dict):
    """Typed (event_type, field, old, new, days) changes between two versions of one row."""
    if a["mw_total"] != b["mw_total"]:
        yield "capacity_changed", "mw_total", a["mw_total"], b["mw_total"], None
    if a["effective_from"] and b["effective_from"] and a["effective_from"] != b["effective_from"]:
        yield "date_slipped", "effective_from", a["effective_from"], b["effective_from"], \
            (b["effective_from"] - a["effective_from"]).days
    if a["status"] != b["status"]:
        yield "stage_changed", "status", a["status"], b["status"], None
    if b["gate"] and a["gate"] != b["gate"]:
        yield "gate_assigned", "gate", a["gate"], b["gate"], None
    # first energisation only: status Built alone is not enough (live rows are Built with 0 MW), later rises aren't new
    # ponytail: a row that drops back to 0 MW and rises again emits a second `connected`; track history if it happens
    if (a["mw_connected"] or 0) <= 0 < (b["mw_connected"] or 0):
        yield "connected", "mw_connected", a["mw_connected"], b["mw_connected"], None


def build(snapshots: list[tuple[str, date, list[dict]]]):
    """snapshots: (snapshot_id, published, rows) in publication order -> (events, versions)."""
    events, versions, open_ = [], [], {}
    prev, prev_id = None, None
    for sid, published, rows in snapshots:
        cur = index(rows)

        def ev(r, kind, field=None, old=None, new=None, days=None, lc=False):
            events.append({"snapshot_id": sid, "prev_snapshot_id": prev_id, "published": published,
                           "key": r["key"], "project_id": r["project_id"], "project_number": r["project_number"],
                           "project_name": r["project_name"], "connection_site": r["connection_site"],
                           "event_type": kind, "field": field, "old_value": None if old is None else str(old),
                           "new_value": None if new is None else str(new), "days": days,
                           "low_confidence": lc or r["low_confidence"]})

        if prev is not None:
            for k in sorted(cur.keys() - prev.keys()):
                ev(cur[k], "added")
            for k in sorted(prev.keys() - cur.keys()):
                ev(prev[k], "withdrawn")
            for k in sorted(cur.keys() & prev.keys()):
                for c in changes(prev[k], cur[k]):
                    ev(cur[k], *c, lc=prev[k]["low_confidence"])
        for k, r in cur.items():
            v = open_.get(k)
            if v and all(v[c] == r[c] for c in COLS.values()) and v["low_confidence"] == r["low_confidence"]:
                continue
            if v:
                versions.append({**v, "valid_to": published})
            open_[k] = {**r, "snapshot_id": sid, "valid_from": published, "valid_to": None}
        for k in open_.keys() - cur.keys():
            versions.append({**open_.pop(k), "valid_to": published})
        prev, prev_id = cur, sid
    versions += open_.values()
    return events, versions


def diff() -> None:
    snaps = manifest()
    if snaps is None:
        sys.exit("no snapshots yet, run `gridline fetch` first")
    snaps = snaps.sort("published", "fetched_at")
    save("source_snapshot", snaps, SNAP_SCHEMA)
    events, versions = build([(s["snapshot_id"], s["published"], read_tec(s["raw_path"]))
                              for s in snaps.iter_rows(named=True)])
    save("event", [[e[c] for c in EVENT_SCHEMA] for e in events], EVENT_SCHEMA)
    save("project_version", [[v[c] for c in VERSION_SCHEMA] for v in versions], VERSION_SCHEMA)


def main() -> None:
    cmd, *args = sys.argv[1:] or ["help"]
    if cmd == "fetch":
        fetch()
    elif cmd == "backfill":
        backfill(args) if args else fetch()
    elif cmd == "diff":
        diff()
    else:
        sys.exit("usage: gridline fetch | gridline backfill [FILE_OR_URL ...] | gridline diff")
