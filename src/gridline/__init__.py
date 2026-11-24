"""Gridline: track GB grid-connection registers over time.

Flow: `fetch` downloads each source's current release (SOURCES), and `archive` stores its raw
bytes as archive/<source>/<sha256>.csv plus a row in archive/<source>/snapshots.csv (both
committed). `diff` replays every archived release in publication order and writes
data/project_version.parquet (each row with valid_from / valid_to) and data/event.parquet (typed
changes between releases). Rows are matched on Project ID + Stage. `low_confidence` marks rows that key cannot match
one-to-one: no Project ID (name-based fallback key) or a key repeated within one release.
"""

import csv
import hashlib
import io
import json
import math
import re
import sys
import urllib.parse
import urllib.request
from collections import Counter
from collections.abc import Iterator
from datetime import UTC, date, datetime
from pathlib import Path

import polars as pl

DATA = Path("data")  # derived Parquet, gitignored
# archive/<source>/<sha256>.csv + snapshots.csv, committed: the registers keep no history
ARCHIVE = Path("archive")
CKAN = "https://api.neso.energy/api/3/action/package_show?id=transmission-entry-capacity-tec-register"

# lower-cased CSV header -> our column name
COLS = {
    "project name": "project_name",
    "customer name": "customer_name",
    "connection site": "connection_site",
    "stage": "stage",
    "mw connected": "mw_connected",
    "mw increase / decrease": "mw_change",
    "cumulative total capacity (mw)": "mw_total",
    "mw effective from": "effective_from",
    # proposed rename (Ofgem consultation, Sept 2026)
    "planned connection date": "effective_from",
    "project status": "status",
    "agreement type": "agreement_type",
    "host to": "host_to",
    "plant type": "plant_type",
    "project id": "project_id",
    "project number": "project_number",
    "gate": "gate",
}
# Older releases may lack project_id, project_number or gate; these must always be present.
REQUIRED = {"project_name", "connection_site", "mw_total", "status", "effective_from"}
NUM = ("mw_connected", "mw_change", "mw_total")
ROW_SCHEMA = {
    "project_name": pl.String,
    "customer_name": pl.String,
    "connection_site": pl.String,
    "stage": pl.String,
    "mw_connected": pl.Float64,
    "mw_change": pl.Float64,
    "mw_total": pl.Float64,
    "effective_from": pl.Date,
    "status": pl.String,
    "agreement_type": pl.String,
    "host_to": pl.String,
    "plant_type": pl.String,
    "project_id": pl.String,
    "project_number": pl.String,
    "gate": pl.String,
}
SNAP_SCHEMA = {
    "snapshot_id": pl.String,
    "source": pl.String,
    "fetched_at": pl.Datetime("us"),  # naive UTC: Windows lacks tzdata
    "published": pl.Date,
    "sha256": pl.String,
    "raw_path": pl.String,
    "rows": pl.Int64,
}
EVENT_SCHEMA = {
    "register": pl.String,
    "snapshot_id": pl.String,
    "prev_snapshot_id": pl.String,
    "published": pl.Date,
    "key": pl.String,
    "project_id": pl.String,
    "project_number": pl.String,
    "project_name": pl.String,
    "connection_site": pl.String,
    "event_type": pl.String,
    "field": pl.String,
    "old_value": pl.String,
    "new_value": pl.String,
    "days": pl.Int64,
    "low_confidence": pl.Boolean,
}
VERSION_SCHEMA = {
    "register": pl.String,
    "key": pl.String,
    **ROW_SCHEMA,
    "low_confidence": pl.Boolean,
    "snapshot_id": pl.String,
    "valid_from": pl.Date,
    "valid_to": pl.Date,
}


def http(url: str) -> bytes:
    req = urllib.request.Request(url, headers={"User-Agent": "gridline/0.1"})
    with urllib.request.urlopen(req, timeout=120) as r:
        return r.read()


def published_from_name(name: str) -> date | None:
    """'tec-register-19-november-2026.csv' -> 2026-11-19."""
    name = re.sub(r"sept(?!ember)", "sep", name.lower())  # strptime has no "sept"
    m = re.search(r"(\d{1,2})[-_ ]+([a-z]+)[-_ ]+(\d{4})", name)
    if not m:
        return None
    for fmt in ("%d %B %Y", "%d %b %Y"):
        try:
            return datetime.strptime(" ".join(m.groups()), fmt).date()
        except ValueError:
            pass
    return None


def parse_date(s: str | None) -> date | None:
    if not s:
        return None
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%b-%Y", "%d/%m/%y"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            pass
    # ponytail: unparseable dates become null; the raw file keeps the original text
    return None


def parse_num(s: str | None) -> float | None:
    if not s:
        return None
    try:
        value = float(s.replace(",", ""))
    except ValueError:
        # e.g. "TBC"; the raw file keeps the original text
        return None
    # NaN != NaN would fake a change on every diff
    return value if math.isfinite(value) else None


def parse_tec(blob: bytes) -> list[dict]:
    """Parse one TEC register CSV. Raises ValueError if a required column is missing."""
    try:
        text = blob.decode("utf-8-sig")
    except UnicodeDecodeError:
        text = blob.decode("cp1252")
    reader = csv.DictReader(io.StringIO(text))
    present = {COLS.get((h or "").strip().lower()) for h in reader.fieldnames or []}
    missing = REQUIRED - present
    if missing:
        raise ValueError(f"not a TEC register: missing columns {sorted(missing)}")
    rows = []
    for raw in reader:
        r = dict.fromkeys(ROW_SCHEMA)
        for k, v in raw.items():
            if k and k.strip().lower() in COLS:
                # two headers can map to one column (old + renamed date):
                # a blank one must not wipe the filled one
                col = COLS[k.strip().lower()]
                r[col] = r[col] or (v or "").strip() or None
        if not any(r.values()):
            continue
        for c in NUM:
            r[c] = parse_num(r[c])
        r["effective_from"] = parse_date(r["effective_from"])
        rows.append(r)
    return rows


def read_tec(path: str | Path) -> list[dict]:
    return parse_tec(Path(path).read_bytes())


def manifest(source: str = "tec") -> pl.DataFrame | None:
    path = ARCHIVE / source / "snapshots.csv"
    return pl.read_csv(path, schema=SNAP_SCHEMA) if path.exists() else None


def save(name: str, rows: list | pl.DataFrame, schema: dict) -> None:
    DATA.mkdir(parents=True, exist_ok=True)
    df = (
        rows
        if isinstance(rows, pl.DataFrame)
        else pl.DataFrame(rows, schema=schema, orient="row")
    )
    df.write_parquet(DATA / f"{name}.parquet")
    print(f"wrote {DATA / name}.parquet ({df.height} rows)")


def archive(
    blob: bytes,
    url: str,
    published: date | None,
    source: str = "tec",
    force: bool = False,
) -> None:
    """Store a release of `source` unless its hash is known.

    Refuses anything its parser rejects or that has 0 rows, and (unless `force`) a release
    with under half the rows of the latest one: a truncated download would otherwise be
    committed forever and replayed as mass `withdrawn` events, or one that lost the
    Project IDs the latest one had."""
    parse = SOURCES[source]["parse"]
    sha = hashlib.sha256(blob).hexdigest()
    snaps = manifest(source)
    if snaps is not None and sha in snaps["sha256"].to_list():
        print(f"skip {url}: already archived as {sha[:12]}")
        return
    try:
        rows = parse(blob)
    except ValueError as e:
        sys.exit(f"refusing {url}: {e}")
    if not rows:
        sys.exit(f"refusing {url}: 0 data rows")
    if snaps is not None and not force:
        last = snaps.sort("published", "fetched_at").row(-1, named=True)
        if len(rows) < last["rows"] / 2:
            sys.exit(
                f"refusing {url}: {len(rows)} rows, under half of the latest release"
                f" ({last['rows']}); use `backfill --force` if it is real"
            )
        # a renamed or dropped Project ID column would re-key every row by name:
        # every project `withdrawn` and `added` again
        last_rows = parse(Path(last["raw_path"]).read_bytes())
        had_ids = any(r["project_id"] for r in last_rows)
        if had_ids and not any(r["project_id"] for r in rows):
            sys.exit(
                f"refusing {url}: no Project IDs, but the latest release had them;"
                " use `backfill --force` if it is real"
            )
    raw = ARCHIVE / source
    raw.mkdir(parents=True, exist_ok=True)
    path = raw / f"{sha}.csv"
    path.write_bytes(blob)
    row = {
        "snapshot_id": sha[:12],
        "source": url,
        "fetched_at": datetime.now(UTC).replace(tzinfo=None),
        "published": published,
        "sha256": sha,
        "raw_path": path.as_posix(),
        "rows": len(rows),
    }
    new = pl.DataFrame([row], schema=SNAP_SCHEMA)
    (new if snaps is None else pl.concat([snaps, new])).write_csv(raw / "snapshots.csv")
    print(f"archived {url} -> {path} ({len(rows)} rows, published {published})")


def ckan(api: str, pattern: str) -> list[tuple[str, date]]:
    """(url, published) of each CSV resource whose file name contains `pattern`, oldest
    first. Other files in the dataset are skipped, so a new guidance CSV can't stop the
    archiver; a matching file that fails the checks still does."""
    found, skipped = [], 0
    for res in json.loads(http(api))["result"]["resources"]:
        url = res["url"]
        name = Path(urllib.parse.urlparse(url).path).name
        is_csv = (res.get("format") or "").upper() == "CSV"
        if not is_csv or pattern not in name.lower():
            skipped += 1
            continue
        published = published_from_name(name)
        if not published and res.get("last_modified"):
            # ponytail: an old resource re-uploaded later would be dated by the upload;
            # order by the month in its name if that ever happens
            published = date.fromisoformat(res["last_modified"][:10])
        if not published:
            sys.exit(
                f"{url}: no date in the file name and no last_modified, cannot order this release"
            )
        found.append((url, published))
    print(f"{len(found)} files match {pattern!r}, {skipped} other resources skipped")
    return sorted(found, key=lambda f: f[1])


# name -> how to find its releases (oldest first), parse them, and when it is stale
SOURCES = {
    "tec": {
        "find": lambda: ckan(CKAN, "tec-register"),
        "parse": lambda blob: parse_tec(blob),
        "stale_days": 14,  # NESO publishes about twice a week
        "gone": "withdrawn",
    },
}


def fetch(sources: list[str] | None = None) -> None:
    """Archive new releases of each source. One failing source doesn't stop the others;
    the run exits non-zero at the end, after the rest are archived."""
    failed = []
    for name in sources or SOURCES:
        try:
            for url, published in SOURCES[name]["find"]():
                archive(http(url), url, published, name)
        # archive() refuses with sys.exit; that must not skip the next source
        except (OSError, ValueError, KeyError, SystemExit) as e:
            print(f"FAILED {name}: {e}", file=sys.stderr)
            failed.append(f"{name}: {e}")
    if failed:
        sys.exit("\n".join(failed))


def backfill(sources: list[str], force: bool = False) -> None:
    """Archive past releases from local files or URLs; the release date must be in the file name."""
    for src in sources:
        published = published_from_name(Path(src).name)
        if not published:
            sys.exit(
                f"{src}: no date in file name, rename it like tec-register-28-february-2025.csv"
            )
        archive(
            http(src) if src.startswith("http") else Path(src).read_bytes(),
            src,
            published,
            force=force,
        )


def check(today: date | None = None, sources: list[str] | None = None) -> None:
    """Exit non-zero when a source's newest release is older than its stale_days."""
    stale = []
    for name in sources or SOURCES:
        snaps = manifest(name)
        newest = None if snaps is None else snaps["published"].max()
        age = None if newest is None else ((today or date.today()) - newest).days
        msg = f"{name}: newest release {newest} ({age} days old)"
        print(msg)
        if age is None or age > SOURCES[name]["stale_days"]:
            stale.append(msg)
    if stale:
        sys.exit("stale: " + "; ".join(stale))


def norm(s: str | None) -> str:
    return re.sub(r"[^a-z0-9]+", " ", (s or "").lower()).strip()


def index(rows: list[dict]) -> dict[str, dict]:
    """Key each row. Project ID is case-sensitive (per NESO), so it is not lower-cased.
    Rows without a Project ID, or whose key repeats inside one release, are low_confidence."""
    base = []
    for r in rows:
        if r["project_id"]:
            # ponytail: Stage is in the key, so a Stage change (blank -> "1") shows as
            # `withdrawn` + `added`; drop it from the key once two real releases show
            # whether one Project ID can carry several stages at once
            base.append(f"{r['project_id']}|{r['stage'] or ''}")
        else:
            fields = ("project_name", "connection_site", "plant_type", "stage")
            base.append("n:" + "|".join(norm(r[c]) for c in fields))
    total, seen, out = Counter(base), Counter(), {}
    for k, r in zip(base, rows):
        seen[k] += 1
        # ponytail: repeated keys are paired across releases by file order;
        # Splink-style matching is a later phase
        key = k if seen[k] == 1 else f"{k}#{seen[k]}"
        out[key] = {
            **r,
            "key": key,
            "low_confidence": total[k] > 1 or not r["project_id"],
        }
    return out


def changes(a: dict, b: dict) -> Iterator[tuple]:
    """Typed (event_type, field, old, new, days) changes between two versions of one row."""
    if a["mw_total"] != b["mw_total"]:
        yield "capacity_changed", "mw_total", a["mw_total"], b["mw_total"], None
    if (
        a["effective_from"]
        and b["effective_from"]
        and a["effective_from"] != b["effective_from"]
    ):
        days = (b["effective_from"] - a["effective_from"]).days
        yield (
            "date_slipped",
            "effective_from",
            a["effective_from"],
            b["effective_from"],
            days,
        )
    if a["status"] != b["status"]:
        yield "status_changed", "status", a["status"], b["status"], None
    if b["gate"] and a["gate"] != b["gate"]:
        yield "gate_assigned", "gate", a["gate"], b["gate"], None
    # first energisation only: status Built alone is not enough (live rows are Built
    # with 0 MW), and later rises are not new.
    # ponytail: a row that drops back to 0 MW and rises again emits a second
    # `connected`; track history if it happens
    if (a["mw_connected"] or 0) <= 0 < (b["mw_connected"] or 0):
        yield "connected", "mw_connected", a["mw_connected"], b["mw_connected"], None


def add_event(
    events: list[dict],
    where: dict,
    r: dict,
    kind: str,
    field: str | None = None,
    old: object = None,
    new: object = None,
    days: int | None = None,
    low_confidence: bool = False,
) -> None:
    """Append one event row; `where` holds snapshot_id, prev_snapshot_id, published."""
    events.append(
        {
            **where,
            "key": r["key"],
            "project_id": r["project_id"],
            "project_number": r["project_number"],
            "project_name": r["project_name"],
            "connection_site": r["connection_site"],
            "event_type": kind,
            "field": field,
            "old_value": None if old is None else str(old),
            "new_value": None if new is None else str(new),
            "days": days,
            "low_confidence": low_confidence or r["low_confidence"],
        }
    )


def build(
    snapshots: list[tuple[str, date, list[dict]]], gone: str = "withdrawn"
) -> tuple[list[dict], list[dict]]:
    """snapshots: (snapshot_id, published, rows) in publication order -> (events, versions).
    `gone` names the event for a row that disappears."""
    events, versions, open_versions = [], [], {}
    prev, prev_id = None, None
    for sid, published, rows in snapshots:
        cur = index(rows)
        where = {
            "snapshot_id": sid,
            "prev_snapshot_id": prev_id,
            "published": published,
        }
        if prev is not None:
            for k in sorted(cur.keys() - prev.keys()):
                add_event(events, where, cur[k], "added")
            for k in sorted(prev.keys() - cur.keys()):
                add_event(events, where, prev[k], gone)
            for k in sorted(cur.keys() & prev.keys()):
                for c in changes(prev[k], cur[k]):
                    lc = prev[k]["low_confidence"]
                    add_event(events, where, cur[k], *c, low_confidence=lc)
        for k, r in cur.items():
            v = open_versions.get(k)
            unchanged = (
                v
                and all(v[c] == r[c] for c in ROW_SCHEMA)
                and v["low_confidence"] == r["low_confidence"]
            )
            if unchanged:
                continue
            if v:
                versions.append({**v, "valid_to": published})
            open_versions[k] = {
                **r,
                "snapshot_id": sid,
                "valid_from": published,
                "valid_to": None,
            }
        for k in open_versions.keys() - cur.keys():
            versions.append({**open_versions.pop(k), "valid_to": published})
        prev, prev_id = cur, sid
    versions += open_versions.values()
    return events, versions


def diff() -> None:
    """Rebuild data/*.parquet from every source's archive; `register` names the source."""
    all_snaps, events, versions = [], [], []
    for name, src in SOURCES.items():
        snaps = manifest(name)
        if snaps is None:
            continue
        snaps = snaps.sort("published", "fetched_at")
        all_snaps.append(snaps.with_columns(register=pl.lit(name)))
        snapshots = [
            (
                s["snapshot_id"],
                s["published"],
                src["parse"](Path(s["raw_path"]).read_bytes()),
            )
            for s in snaps.iter_rows(named=True)
        ]
        e, v = build(snapshots, src["gone"])
        events += [{**x, "register": name} for x in e]
        versions += [{**x, "register": name} for x in v]
    if not all_snaps:
        sys.exit("no snapshots yet, run `gridline fetch` first")
    save("source_snapshot", pl.concat(all_snaps), SNAP_SCHEMA)
    save("event", [[e[c] for c in EVENT_SCHEMA] for e in events], EVENT_SCHEMA)
    save(
        "project_version",
        [[v[c] for c in VERSION_SCHEMA] for v in versions],
        VERSION_SCHEMA,
    )


def main() -> None:
    cmd, *args = sys.argv[1:] or ["help"]
    if cmd == "fetch":
        fetch(args)
    elif cmd == "backfill" and set(args) - {"--force"}:
        backfill([a for a in args if a != "--force"], "--force" in args)
    elif cmd == "diff":
        diff()
    elif cmd == "check":
        check(sources=args)
    else:
        sys.exit(
            "usage: gridline fetch [SOURCE ...] | gridline backfill [--force] FILE_OR_URL ... | gridline diff | gridline check [SOURCE ...]"
        )
