"""Gridline: track GB grid-connection registers over time.

Flow: `fetch` downloads each source's current release (SOURCES), and `archive` stores its raw
bytes as archive/<source>/<sha256>.csv plus a row in archive/<source>/snapshots.csv (both
committed). `diff` replays every archived release in publication order and writes
data/project_version.parquet (each row with valid_from / valid_to) and data/event.parquet (typed
changes between releases). TEC rows are matched on Project ID + Stage, ECR rows on the DNO's
row ID. `low_confidence` marks rows that key cannot match one-to-one: no ID (name-based fallback
key) or a key repeated within one release.
"""

import csv
import hashlib
import io
import json
import math
import os
import re
import sys
import urllib.parse
import urllib.request
from collections import Counter, defaultdict
from collections.abc import Iterator
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import fastexcel
import polars as pl

DATA = Path("data")  # derived Parquet, gitignored
# archive/<source>/<sha256>.csv + snapshots.csv, committed: the registers keep no history
ARCHIVE = Path("archive")
CKAN = "https://api.neso.energy/api/3/action/package_show?id=transmission-entry-capacity-tec-register"
SSEN_CKAN = "https://data-api.ssen.co.uk/api/3/action/package_show?id=embedded_capacity_register"
NPG = "https://northernpowergrid.opendatasoft.com/api/explore/v2.1/catalog/datasets/embedded-capacity-register"
# 1 MW+ registers behind a free portal account; the key goes in a header, never the URL
# (the URL lands in the committed manifest)
UKPN = "https://ukpowernetworks.opendatasoft.com/api/explore/v2.1/catalog/datasets/ukpn-embedded-capacity-register"
SPEN = "https://spenergynetworks.opendatasoft.com/api/explore/v2.1/catalog/datasets/embedded-capacity-register"
ENWL = "https://electricitynorthwest.opendatasoft.com/api/explore/v2.1/catalog/datasets/enwl-embedded-capacity-register-2-1mw-and-above"

# norm(CSV header) -> our column name
COLS = {
    "project name": "project_name",
    "customer name": "customer_name",
    "connection site": "connection_site",
    "stage": "stage",
    "mw connected": "mw_connected",
    "mw increase decrease": "mw_change",
    "cumulative total capacity mw": "mw_total",
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

# Embedded Capacity Registers (DNOs) share the ENA/DCUSA template; norm() lines up the
# spellings (SSEN "Customer Site ", NPg "customer_site"). Mapped onto ROW_SCHEMA so build()
# works unchanged: site -> project_name, Grid Supply Point -> connection_site, Target
# Energisation Date -> effective_from, Energy Source 1-3 -> plant_type. The raw file keeps
# every other column.
ECR_COLS = {
    "customer site": "project_name",
    "customer name": "customer_name",
    "grid supply point": "connection_site",
    "connection status": "status",
    "already connected registered capacity mw": "mw_connected",
    "accepted to connect registered capacity mw": "mw_accepted",
    "target energisation date": "effective_from",
    "energy source 1": "plant_type",
    "energy source 2": "source_2",
    "energy source 3": "source_3",
    "unique id": "project_id",  # SSEN
    "ecrid": "project_id",  # NPg
}
# no ID column in the XLSX history; archive() still refuses a release that loses IDs
ECR_REQUIRED = set(ECR_COLS.values()) - {"customer_name", "project_id"}
# the template's many spellings of "no value", after norm()
MISSING = {
    "",
    "data not available",
    "data not applicable",
    "not applicable",
    "n a",
    "na",
    "tbc",
    "to be confirmed",
    "redacted",
}
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


def http(url: str, headers: dict | None = None) -> bytes:
    req = urllib.request.Request(
        url, headers={"User-Agent": "gridline/0.1", **(headers or {})}
    )
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
    if s.isdigit() and 20000 <= int(s) <= 80000:
        # Excel serial day that leaked into a CSV (SSEN "46336")
        return date(1899, 12, 30) + timedelta(days=int(s))
    # drop a time part: "2024-03-07T00:00:00+00:00", "17/07/2026 00:00"
    s = re.split(r"[T ]", s.strip())[0]
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


def read_rows(blob: bytes, cols: dict, required: set, what: str) -> Iterator[dict]:
    """CSV rows as {our column: stripped text or None}, blank rows dropped.
    Raises ValueError if a required column is missing."""
    try:
        text = blob.decode("utf-8-sig")
    except UnicodeDecodeError:
        text = blob.decode("cp1252")
    reader = csv.DictReader(io.StringIO(text))
    present = {cols.get(norm(h)) for h in reader.fieldnames or []}
    missing = required - present
    if missing:
        raise ValueError(f"not a {what}: missing columns {sorted(missing)}")
    for raw in reader:
        r = dict.fromkeys(cols.values())
        for k, v in raw.items():
            col = cols.get(norm(k))
            if col:
                # two headers can map to one column (old + renamed date):
                # a blank one must not wipe the filled one
                r[col] = r[col] or (v or "").strip() or None
        if any(r.values()):
            yield r


def parse_tec(blob: bytes) -> list[dict]:
    """Parse one TEC register CSV. Raises ValueError if a required column is missing."""
    rows = []
    for r in read_rows(blob, COLS, REQUIRED, "TEC register"):
        r = {**dict.fromkeys(ROW_SCHEMA), **r}
        for c in NUM:
            r[c] = parse_num(r[c])
        r["effective_from"] = parse_date(r["effective_from"])
        rows.append(r)
    return rows


def xlsx_csv(blob: bytes) -> bytes:
    """An ECR workbook's 1 MW+ register sheet as CSV, so one parser reads both formats.
    Older templates have one "Register" sheet; the header row is the one holding
    "Customer Site" (rows above it are section titles)."""
    book = fastexcel.read_excel(blob)
    sheets = [s for s in book.sheet_names if "≥1MW" in s] or [
        s for s in book.sheet_names if norm(s).startswith("register part 1")
    ]
    sheets = sheets or [s for s in book.sheet_names if norm(s) == "register"]
    if not sheets:
        raise ValueError(f"no register sheet in {book.sheet_names}")
    df = book.load_sheet(sheets[0], header_row=None, dtypes="string").to_polars()
    rows = [list(r) for r in df.iter_rows()]
    top = next((i for i, r in enumerate(rows) if "customer site" in map(norm, r)), None)
    if top is None:
        raise ValueError(f"no Customer Site header in sheet {sheets[0]!r}")
    out = io.StringIO()
    csv.writer(out).writerows(rows[top:])
    return out.getvalue().encode()


def parse_ecr(blob: bytes) -> list[dict]:
    """Parse one Embedded Capacity Register (CSV, or XLSX workbook) into ROW_SCHEMA rows."""
    if blob[:4] == b"PK\x03\x04":
        blob = xlsx_csv(blob)
    rows = []
    for r in read_rows(blob, ECR_COLS, ECR_REQUIRED, "Embedded Capacity Register"):
        r = {k: None if norm(v) in MISSING else v for k, v in r.items()}
        sources = [r.pop("plant_type"), r.pop("source_2"), r.pop("source_3")]
        accepted = parse_num(r.pop("mw_accepted"))
        row = {**dict.fromkeys(ROW_SCHEMA), **r}
        row["plant_type"] = ";".join(filter(None, sources)) or None
        # spellings differ by DNO and by row ("ACCEPTED TO CONNECT", "Accepted To Connect")
        row["status"] = r["status"] and r["status"].lower()
        # SSEN's CSV rounds MW to 3 dp and its workbook doesn't: round both, or the
        # switch from XLSX history to CSV would look like a capacity change
        row["mw_connected"] = parse_num(r["mw_connected"])
        if row["mw_connected"] is not None:
            row["mw_connected"] = round(row["mw_connected"], 3)
        accepted = None if accepted is None else round(accepted, 3)
        # the register fills one capacity column per row, chosen by status
        connected = row["status"] == "connected"
        row["mw_total"] = row["mw_connected"] if connected else accepted
        row["effective_from"] = parse_date(r["effective_from"])
        # SSEN's "SSEN0001-1MW-CONNECTED" carries the status: keep the stable number.
        # ponytail: Oct->Nov 2026 kept every number, but no row changed status that
        # month; check the number survives connection once one does
        if row["project_id"]:
            row["project_id"] = re.sub(
                r"-(1MW|50KW)-.*", "", row["project_id"], flags=re.IGNORECASE
            )
        rows.append(row)
    return rows


def read_tec(path: str | Path) -> list[dict]:
    return parse_tec(Path(path).read_bytes())


def raw(snap: dict) -> bytes:
    """A snapshot's bytes: from archive/, or for Release-hosted history downloaded once and
    cached in data/cache/ (gitignored). Refuses bytes whose sha256 isn't the manifest's."""
    path, cached = snap["raw_path"], DATA / "cache" / snap["sha256"]
    if not path.startswith("http"):
        blob = Path(path).read_bytes()
    elif cached.exists():
        blob = cached.read_bytes()
    else:
        blob = http(path)
    if hashlib.sha256(blob).hexdigest() != snap["sha256"]:
        raise ValueError(
            f"{path}: sha256 differs from the manifest, refusing to parse it"
        )
    if path.startswith("http") and not cached.exists():
        cached.parent.mkdir(parents=True, exist_ok=True)
        cached.write_bytes(blob)
    return blob


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
    hosted: str | None = None,
) -> None:
    """Store a release of `source` unless its hash is known. With `hosted` (a GitHub
    Release asset URL) only the manifest row is written and raw_path points there.

    Refuses anything its parser rejects or that has 0 rows, and (unless `force`) a release
    with under half the rows of the release before it: a truncated download would
    otherwise be committed forever and replayed as mass `withdrawn` events, or one that
    lost the Project IDs the release before it had."""
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
    # compare with the release just before this one, so backfilling old releases works
    before = None if snaps is None else snaps.filter(pl.col("published") <= published)
    if before is not None and before.height and not force:
        last = before.sort("published", "fetched_at").row(-1, named=True)
        if len(rows) < last["rows"] / 2:
            sys.exit(
                f"refusing {url}: {len(rows)} rows, under half of the release before it"
                f" ({last['rows']}); rerun with --force if it is real"
            )
        # a renamed or dropped Project ID column would re-key every row by name:
        # every project `withdrawn` and `added` again
        had_ids = any(r["project_id"] for r in parse(raw(last)))
        if had_ids and not any(r["project_id"] for r in rows):
            sys.exit(
                f"refusing {url}: no Project IDs, but the release before it had them;"
                " rerun with --force if it is real"
            )
    folder = ARCHIVE / source
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / f"{sha}.csv"
    if not hosted:
        path.write_bytes(blob)
    row = {
        "snapshot_id": sha[:12],
        "source": url,
        "fetched_at": datetime.now(UTC).replace(tzinfo=None),
        "published": published,
        "sha256": sha,
        "raw_path": hosted or path.as_posix(),
        "rows": len(rows),
    }
    new = pl.DataFrame([row], schema=SNAP_SCHEMA)
    (new if snaps is None else pl.concat([snaps, new])).write_csv(
        folder / "snapshots.csv"
    )
    print(
        f"archived {url} -> {row['raw_path']} ({len(rows)} rows, published {published})"
    )


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


def apikey(env: str | None) -> dict:
    """Opendatasoft auth header from an env var (a repository secret in the Action)."""
    return (
        {"Authorization": f"Apikey {os.environ[env]}"}
        if os.environ.get(env or "")
        else {}
    )


def opendatasoft(dataset: str, key: str | None = None) -> list[tuple[str, date]]:
    """The dataset's whole-table CSV export, dated by its last data change. A portal that
    wants a login answers with the header only, which archive() refuses as 0 rows.
    With `key` (an env var name) unset, the source is skipped with a notice."""
    if key and not os.environ.get(key):
        print(f"skip {dataset}: set {key} to fetch it")
        return []
    meta = json.loads(http(dataset, apikey(key)))["metas"]["default"]
    return [
        (
            f"{dataset}/exports/csv?delimiter=%2C",
            date.fromisoformat(meta["data_processed"][:10]),
        )
    ]


# name -> how to find its releases (oldest first), parse them, and when it is stale
SOURCES = {
    "tec": {
        "find": lambda: ckan(CKAN, "tec-register"),
        "parse": parse_tec,
        "stale_days": 14,  # NESO publishes about twice a week
        "gone": "withdrawn",
    },
    "ssen": {
        "find": lambda: ckan(SSEN_CKAN, "_1mw_"),  # ssen_ecr_part_1_1mw_<month>_csv.csv
        "parse": parse_ecr,
        "stale_days": 45,  # monthly, mid-month
        # gone from a DNO register: often a reform move to Gate 1, not a cancellation
        "gone": "removed_from_register",
    },
    "npg": {
        "find": lambda: opendatasoft(NPG),  # the 1 MW and over register
        "parse": parse_ecr,
        "stale_days": 45,  # monthly, by the 10th working day
        "gone": "removed_from_register",
    },
    "ukpn": {
        "find": lambda: opendatasoft(UKPN, "UKPN_API_KEY"),
        "key": "UKPN_API_KEY",
        "parse": parse_ecr,
        "stale_days": 45,
        "gone": "removed_from_register",
    },
    "spen": {
        "find": lambda: opendatasoft(SPEN, "SPEN_API_KEY"),  # rows carry `unique_id`
        "key": "SPEN_API_KEY",
        "parse": parse_ecr,
        "stale_days": 45,
        "gone": "removed_from_register",
    },
    "enwl": {  # SP Electricity North West
        "find": lambda: opendatasoft(ENWL, "ENWL_API_KEY"),
        "key": "ENWL_API_KEY",
        "parse": parse_ecr,
        "stale_days": 45,
        "gone": "removed_from_register",
    },
}


def fetch(sources: list[str] | None = None, force: bool = False) -> None:
    """Archive new releases of each source. One failing source doesn't stop the others;
    the run exits non-zero at the end, after the rest are archived."""
    failed = []
    for name in sources or SOURCES:
        try:
            for url, published in SOURCES[name]["find"]():
                blob = http(url, apikey(SOURCES[name].get("key")))
                archive(blob, url, published, name, force)
        # anything one source throws (refusal, HTTP error, odd JSON) must not skip the
        # next source: a missed release is lost for good
        except (Exception, SystemExit) as e:  # noqa: BLE001
            print(f"FAILED {name}: {e}", file=sys.stderr)
            failed.append(f"{name}: {e}")
    if failed:
        sys.exit("\n".join(failed))


def backfill(
    files: list[str],
    force: bool = False,
    source: str = "tec",
    release: str | None = None,
) -> None:
    """Archive past releases from local files or URLs; the release date must be in the file
    name. With `release` (a GitHub Release download URL ending in /) the files are meant to
    be uploaded there: only their manifest rows are committed, pointing at release + name."""
    for src in files:
        published = published_from_name(Path(src).name)
        if not published:
            sys.exit(
                f"{src}: no date in file name, rename it like tec-register-28-february-2025.csv"
            )
        archive(
            http(src) if src.startswith("http") else Path(src).read_bytes(),
            src,
            published,
            source,
            force,
            release and release + Path(src).name,
        )


def check(today: date | None = None, sources: list[str] | None = None) -> None:
    """Exit non-zero when a source's newest release is older than its stale_days."""
    stale = []
    for name in sources or SOURCES:
        key = SOURCES[name].get("key")
        if key and not os.environ.get(key):
            print(f"{name}: not checked, {key} is not set")
            continue
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


def name_key(r: dict) -> str:
    """Fallback key for a row without an ID."""
    fields = ("project_name", "connection_site", "plant_type", "stage")
    return "n:" + "|".join(norm(r[c]) for c in fields)


def borrow_ids(snapshots: list[tuple[str, date, list[dict]]]) -> None:
    """Give a row without an ID the ID of the row with the same name key in another
    release, when exactly one ID has that key. The SSEN and NPg workbooks (history) have
    no ID column while their CSVs do, so without this every project would look removed and
    re-added the month the format changed. Borrowed IDs are low_confidence.
    ponytail: exact-key match only; Splink-style matching is Phase 2."""
    ids = defaultdict(set)
    for _, _, rows in snapshots:
        for r in rows:
            if r["project_id"]:
                ids[name_key(r)].add(r["project_id"])
    for _, _, rows in snapshots:
        for r in rows:
            found = ids.get(name_key(r), ()) if not r["project_id"] else ()
            if len(found) == 1:
                r["project_id"], r["borrowed_id"] = next(iter(found)), True


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
            base.append(name_key(r))
    total, seen, out = Counter(base), Counter(), {}
    for k, r in zip(base, rows):
        seen[k] += 1
        # ponytail: repeated keys are paired across releases by file order;
        # Splink-style matching is a later phase
        key = k if seen[k] == 1 else f"{k}#{seen[k]}"
        out[key] = {
            **r,
            "key": key,
            "low_confidence": total[k] > 1
            or not r["project_id"]
            or r.get("borrowed_id", False),
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
    borrow_ids(snapshots)
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
                src["parse"](raw(s)),
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
    force, args = "--force" in args, [a for a in args if a != "--force"]
    release = None
    if "--release" in args:
        i = args.index("--release")
        release, args = args[i + 1], args[:i] + args[i + 2 :]
    if cmd == "fetch":
        fetch(args, force)
    elif cmd == "backfill" and args:
        source = args.pop(0) if args[0] in SOURCES else "tec"
        backfill(args, force, source, release)
    elif cmd == "diff":
        diff()
    elif cmd == "check":
        check(sources=args)
    else:
        sys.exit(
            "usage: gridline fetch [--force] [SOURCE ...] | gridline backfill [--force] [--release URL/] [SOURCE] FILE_OR_URL ... | gridline diff | gridline check [SOURCE ...]"
        )
