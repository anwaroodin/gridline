"""Gridline: track GB grid-connection registers over time.

Flow: `fetch` downloads each source's current release (SOURCES), and `archive` stores its raw
bytes as archive/<source>/<sha256>.csv plus a row in archive/<source>/snapshots.csv (both
committed). `diff` replays every archived release in publication order and writes
data/project_version.parquet (each row with valid_from / valid_to) and data/event.parquet (typed
changes between releases), and groups the rows into projects: data/project.parquet, one row
per project with a Gridline ID (`GL-` + 8 characters) kept stable by the committed ledger
archive/project_ids.csv, and data/project_link.parquet, one row per register row. TEC rows are matched on Project ID + Stage, ECR rows on the DNO's
row ID. `low_confidence` marks rows that key cannot match one-to-one: no ID (name-based fallback
key) or a key repeated within one release. A row that leaves a release while another arrives
with the same export meter number (MPAN) or a near-identical name at the same GSP continues as one
row (`renamed` when its name changed), also low_confidence.
"""

import base64
import csv
import difflib
import hashlib
import io
import json
import math
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import Counter, defaultdict
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, date, datetime, timedelta
from functools import cache
from pathlib import Path

import fastexcel
import polars as pl

DATA = Path("data")  # derived Parquet, gitignored
# archive/<source>/<sha256>.csv + snapshots.csv, committed: the registers keep no history
ARCHIVE = Path("archive")
# hand-kept lookup tables, committed; found from the package so tests can chdir
# ponytail: assumes a source checkout (src/gridline); ship them as package data if installed
REFERENCE = Path(__file__).resolve().parents[2] / "reference"
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
    "ecrid": "project_id",  # NPg CSV
    "unique site identifier": "project_id",  # NPg workbooks from May 2026, same IDs
    "export mpan msid": "mpan",  # the export meter: links a row renamed between releases
}
# no ID column in the XLSX history; archive() still refuses a release that loses IDs
ECR_REQUIRED = set(ECR_COLS.values()) - {"customer_name", "project_id", "mpan"}
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


# The ledger: one row per register row (`register`, `key`) ever seen. `project_id` is the ID it
# was first given and never changes; `merged_into` is the ID it carries now, when that differs.
# `method` and `confidence` say how it joined a project other than its own; links are kept
# (sticky) in later builds. `minted` is the date the row was first published.
LEDGER = ARCHIVE / "project_ids.csv"
LEDGER_COLS = (
    "project_id",
    "register",
    "key",
    "minted",
    "merged_into",
    "method",
    "confidence",
)
PROJECT_SCHEMA = {
    "project_id": pl.String,
    "name": pl.String,
    "plant_type": pl.String,
    "mw_total": pl.Float64,
    "status": pl.String,
    "gate": pl.String,
    "effective_from": pl.Date,
    "gsp_id": pl.String,
    "registers": pl.String,
    "source_ids": pl.String,
    "members": pl.Int64,
    "first_seen": pl.Date,
    "live": pl.Boolean,  # some row of it is in its register's newest release
}
LINK_SCHEMA = {
    "project_id": pl.String,
    "register": pl.String,
    "key": pl.String,
    "source_id": pl.String,
    "method": pl.String,
    "confidence": pl.Float64,
    "evidence": pl.String,
}
# transmission-side registers: a project's name and status come from these first
TRANSMISSION = ("tec",)


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
    df.write_csv(DATA / f"{name}.csv")
    print(f"wrote {DATA / name}.parquet and .csv ({df.height} rows)")


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


def site_key(s: str | None) -> str:
    """norm() without a trailing "GSP" or "Grid": "MELKSHAM GSP" -> "melksham"."""
    words = norm(s).split()
    while len(words) > 1 and words[-1] in ("gsp", "grid"):
        words.pop()
    return " ".join(words)


@cache
def gsp_ids() -> dict[str, str]:
    """site_key(name) -> GSP ID, from reference/gsp.csv: NESO's GSP lookup plus aliases for
    other spellings, such as the codes SSEN used until Sept 2023 ("MELK" -> MELK_1)."""
    path = REFERENCE / "gsp.csv"
    if not path.exists():
        return {}
    with path.open(encoding="utf-8") as f:
        return {site_key(r["name"]): r["gsp_id"] for r in csv.DictReader(f)}


def gsp(site: str | None) -> str:
    """The GSP ID of a connection site, or its site_key() if the lookup doesn't know it."""
    k = site_key(site)
    return gsp_ids().get(k, k)


def name_key(r: dict) -> str:
    """Fallback key for a row without an ID. The site is its GSP ID, so a register that
    renames its GSPs (SSEN, Oct 2023) doesn't re-key every row."""
    parts = (
        norm(r["project_name"]),
        gsp(r["connection_site"]),
        norm(r["plant_type"]),
        norm(r["stage"]),
    )
    return "n:" + "|".join(parts)


def borrow_ids(snapshots: list[tuple[str, date, list[dict]]]) -> None:
    """Give a row without an ID the ID of the row with the same name key in another
    release, when exactly one ID has that key. The SSEN and NPg workbooks (history) have
    no ID column while their CSVs do, so without this every project would look removed and
    re-added the month the format changed. Borrowed IDs are low_confidence. Two rows can
    share a name key (one site, two connections): then the capacity must match too.
    ponytail: exact-key match only"""
    ids = defaultdict(set)
    for _, _, rows in snapshots:
        for r in rows:
            if r["project_id"]:
                ids[name_key(r)].add(r["project_id"])
                ids[name_key(r), r["mw_total"]].add(r["project_id"])
    for _, _, rows in snapshots:
        for r in rows:
            if r["project_id"]:
                continue
            found = ids.get(name_key(r), ())
            if len(found) > 1:
                found = ids.get((name_key(r), r["mw_total"]), ())
            if len(found) == 1:
                r["project_id"], r["borrowed_id"] = next(iter(found)), True


def index(rows: list[dict]) -> dict[str, dict]:
    """Key each row. Project ID is case-sensitive (per NESO), so it is not lower-cased.
    Rows without a Project ID, or whose key repeats inside one release, are low_confidence."""
    base = []
    for r in rows:
        if r["project_id"]:
            # Stage stays in the key: one Project ID carries several stages at once
            # (PRO-000366-1 and -2, 23 Nov 2026). A split is linked in build().
            # ponytail: a lone rename (blank -> "1") still shows as withdrawn + added
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


def stage_splits(prev: dict, cur: dict) -> dict[str, list[str]]:
    """{old key: new keys} for a Project ID that had one Stage and now has several: the
    project split into stages and stays one project, so build() links the new stage keys
    to the old key with one `stage_split` instead of `withdrawn` + `added`."""
    stages, keys = defaultdict(lambda: ({}, {})), {}
    for side, rows in enumerate((prev, cur)):
        for k, r in rows.items():
            if r["project_id"]:
                stages[r["project_id"]][side].setdefault(r["stage"], []).append(k)
    for old, new in stages.values():
        if len(old) == 1 and len(new) > 1:
            (old_keys,) = old.values()
            added = sorted(k for ks in new.values() for k in ks if k not in prev)
            if len(old_keys) == 1 and added:
                keys[old_keys[0]] = added
    return keys


def name_numbers(name: str | None) -> set[str]:
    """The numbers in a name: "Beinn an Tuirc 2" and "Beinn an Tuirc 3" are two projects."""
    return set(re.findall(r"\d+", norm(name)))


def relink(prev: dict, cur: dict, gone: set, new: set) -> dict[str, str]:
    """{new key: old key} for rows that left (`gone`) and arrived (`new`) in one release but
    are one row under another name or GSP spelling: the same export meter number (MPAN or
    MSID, at least 4 digits) where it is unique on both sides, even if the GSP changed or was
    filled in; else the same GSP and a name at least 85% alike with the same numbers in it,
    one-to-one, best name first and closest capacity breaking ties (rows with no name: the
    same technology and capacity); else the one row on each side with the same GSP, technology,
    capacity, date and status. Two rows that both carry their own ID are never relinked: the IDs
    decide."""
    own_id = lambda r: r["project_id"] and not r.get("borrowed_id")
    pairs = {}
    mpans = [defaultdict(list), defaultdict(list)]
    for side, rows, keys in ((0, prev, gone), (1, cur, new)):
        for k in keys:
            m = rows[k].get("mpan") or ""
            if len(re.sub(r"\D", "", m)) >= 4:  # not "0" or "BP_BESS"
                mpans[side][m].append(k)
    for m, olds in mpans[0].items():
        o = olds[0]
        if (
            len(olds) == 1
            and len(mpans[1].get(m, ())) == 1
            and not (own_id(prev[o]) and own_id(cur[mpans[1][m][0]]))
        ):
            pairs[mpans[1][m][0]] = o
    by_gsp = defaultdict(list)
    for n in new - pairs.keys():
        by_gsp[gsp(cur[n]["connection_site"])].append(n)
    found = []
    # ponytail: all pairs within one GSP; fine at a few hundred rows per release
    for o in gone - set(pairs.values()):
        a = prev[o]
        for n in by_gsp.get(gsp(a["connection_site"]), ()):
            b = cur[n]
            if own_id(a) and own_id(b):
                continue
            if not (a["project_name"] and b["project_name"]):
                # a row with no site name (redacted): same technology and exact capacity
                same = not (a["project_name"] or b["project_name"]) and (
                    norm(a["plant_type"]) == norm(b["plant_type"])
                    and a["mw_total"] is not None
                    and a["mw_total"] == b["mw_total"]
                )
                ratio = 1.0 if same else 0.0
            elif name_numbers(a["project_name"]) != name_numbers(b["project_name"]):
                continue
            else:
                ratio = difflib.SequenceMatcher(
                    None, norm(a["project_name"]), norm(b["project_name"])
                ).ratio()
            if ratio >= 0.85:
                mw = abs((a["mw_total"] or 0) - (b["mw_total"] or 0))
                found.append((-ratio, mw, o, n))
    used = set(pairs.values())
    for _, _, o, n in sorted(found):
        if o not in used and n not in pairs:
            pairs[n] = o
            used.add(o)
    # last: one row each side with the same GSP, technology, capacity, date and status (SSEN
    # replaced names like "49.9MW BATTERY" with site names in Dec 2025)
    # ponytail: two different projects alike in all five, one leaving as the other arrives,
    # would be joined; add the location (eastings) to the key if that shows up
    prints = [defaultdict(list), defaultdict(list)]
    for side, rows, keys in ((0, prev, gone - used), (1, cur, new - pairs.keys())):
        for k in keys:
            r = rows[k]
            if r["mw_total"] is not None and not own_id(r):
                f = (gsp(r["connection_site"]), norm(r["plant_type"]), r["mw_total"])
                prints[side][*f, r["effective_from"], r["status"]].append(k)
    for f, olds in prints[0].items():
        if len(olds) == 1 and len(prints[1].get(f, ())) == 1:
            o, n = olds[0], prints[1][f][0]
            a, b = (name_numbers(r["project_name"]) for r in (prev[o], cur[n]))
            if not (a and b and a != b):  # "Farm 2" is not "Farm 3"
                pairs[n] = o
    return pairs


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
    moved = {}  # key in a release -> the older key its row continues
    for sid, published, rows in snapshots:
        cur = index(rows)
        for k in [k for k in cur if k in moved]:
            # ponytail: if the old key itself comes back, it keeps its key and this row
            # stays under its own
            if moved[k] not in cur:
                cur[moved[k]] = {**cur.pop(k), "key": moved[k], "low_confidence": True}
        where = {
            "snapshot_id": sid,
            "prev_snapshot_id": prev_id,
            "published": published,
        }
        if prev is not None:
            splits = stage_splits(prev, cur)
            linked = {k for ks in splits.values() for k in ks}
            left = prev.keys() - cur.keys() - splits.keys()
            arrived = cur.keys() - prev.keys() - linked
            for n, o in sorted(relink(prev, cur, left, arrived).items()):
                cur[o] = {**cur.pop(n), "key": o, "low_confidence": True}
                moved[n] = o
                a, b = prev[o]["project_name"], cur[o]["project_name"]
                if norm(a) != norm(b):
                    add_event(events, where, cur[o], "renamed", "project_name", a, b)
            for old, new in sorted(splits.items()):
                add_event(
                    events, where, prev[old], "stage_split", "key", old, ";".join(new)
                )
            for k in sorted(cur.keys() - prev.keys() - linked):
                add_event(events, where, cur[k], "added")
            for k in sorted(prev.keys() - cur.keys() - splits.keys()):
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


def mint(seed: str, taken: set) -> str:
    """A new Gridline ID from a seed (the project's first row), so a build with no ledger
    gives the same IDs too; the ledger keeps them once minted."""
    for n in range(1000):
        h = hashlib.sha256(f"{seed}#{n}".encode()).digest()
        pid = "GL-" + base64.b32encode(h).decode()[:8]
        if pid not in taken:
            return pid
    raise RuntimeError("no free project ID")  # 32^8 IDs: never in practice


def assign_ids(
    minted: dict[tuple, date],
    edges: list[tuple],
    ledger: list[dict],
    apart: set = frozenset(),
) -> tuple[dict, dict, list[dict]]:
    """Group register rows into projects and give each project an ID.

    minted: {(register, key): first published}. edges: (row a, row b, method, confidence,
    evidence), b joining a's project. ledger: rows of archive/project_ids.csv; rows that
    share an ID there stay together (links are sticky), except rows in `apart` (an override
    said separate) and rows a matched edge links now: that edge replaces the old link, so
    a row the matcher moves to another project never merges the two. Returns ({row: ID},
    {row: (method, confidence, evidence)}, new ledger).

    A project keeps the oldest ID among its rows: two projects that merge keep the older ID,
    and the other's rows record it as `merged_into`. When a project splits, the part holding
    the row that had the ID first keeps it; the rest take back their own first ID or a new one."""
    old = {(r["register"], r["key"]): r for r in ledger}
    now = lambda r: r["merged_into"] or r["project_id"]
    born = {}  # ID -> the date it was minted
    for r in old.values():
        born[r["project_id"]] = min(born.get(r["project_id"], r["minted"]), r["minted"])
    parent = {k: k for k in minted}

    def root(k):
        while parent[k] != k:
            parent[k] = parent[parent[k]]
            k = parent[k]
        return k

    relinked = {b for _, b, method, *_ in edges if method != "id"}
    how, sticky = {}, defaultdict(list)
    for k in minted.keys() & old.keys() - apart - relinked:
        sticky[now(old[k])].append(k)
    for ks in sticky.values():
        ks.sort(key=lambda k: (old[k]["minted"], k))
        for k in ks[1:]:
            if old[k]["method"]:
                c = old[k]["confidence"]
                how[k] = (
                    old[k]["method"],
                    float(c) if c else None,
                    "kept from an earlier build",
                )
            parent[root(k)] = root(ks[0])
    for a, b, method, confidence, evidence in edges:
        if method != "id":
            how[b] = (method, confidence, evidence)
        parent[root(b)] = root(a)
    groups = defaultdict(list)
    for k in sorted(minted):
        groups[root(k)].append(k)
    # each group claims the IDs its rows carry now, then the IDs they were first given;
    # oldest ID first, then the group whose row had it first
    claims = []
    for g, ks in groups.items():
        known = [k for k in ks if k in old]
        for pass_, ids in (
            (0, {now(old[k]) for k in known}),
            (1, {old[k]["project_id"] for k in known}),
        ):
            for pid in ids:
                first = min(
                    old[k]["minted"]
                    for k in known
                    if pid in (now(old[k]), old[k]["project_id"])
                )
                claims.append((pass_, born[pid], first, pid, g))
    ids = {}
    for *_, pid, g in sorted(claims):
        if g not in ids and pid not in ids.values():
            ids[g] = pid
    taken = set(born)
    for g, ks in groups.items():
        if g not in ids:
            ids[g] = mint(f"{ks[0][0]}:{ks[0][1]}", taken)
            taken.add(ids[g])
    out = {k: ids[g] for g, ks in groups.items() for k in ks}
    rows = []
    for k in sorted(old.keys() | out.keys()):
        r = old.get(k) or {
            "project_id": out[k],
            "register": k[0],
            "key": k[1],
            "minted": minted[k],
        }
        if k in out:
            method, confidence, _ = how.get(k, ("", None, None))
            r = {
                **r,
                "merged_into": "" if out[k] == r["project_id"] else out[k],
                "method": method,
                "confidence": "" if confidence is None else confidence,
            }
        rows.append(r)
    return out, how, rows


def read_ledger() -> list[dict]:
    if not LEDGER.exists():
        return []
    with LEDGER.open(encoding="utf-8", newline="") as f:
        return [
            {**r, "minted": date.fromisoformat(r["minted"])} for r in csv.DictReader(f)
        ]


def write_ledger(rows: list[dict]) -> None:
    LEDGER.parent.mkdir(parents=True, exist_ok=True)
    with LEDGER.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, LEDGER_COLS, extrasaction="ignore", lineterminator="\n")
        w.writeheader()
        w.writerows(rows)


def lineages(versions: list[dict]) -> dict[tuple, list[dict]]:
    """{(register, key): that row's versions, oldest first}."""
    out = defaultdict(list)
    for v in versions:
        out[v["register"], v["key"]].append(v)
    for vs in out.values():
        vs.sort(key=lambda v: v["valid_from"])
    return out


def projects(
    versions: list[dict], edges: list[tuple] = (), apart: set = frozenset()
) -> tuple[list, list, dict]:
    """(project rows, project_link rows, {(register, key): project ID}) for every register
    row in `versions`. Rows of one register with the same official ID (a TEC Project ID with
    several stages) are one project; `edges` link rows across registers. Reads and rewrites
    the ledger."""
    lineage = lineages(versions)
    minted = {k: vs[0]["valid_from"] for k, vs in lineage.items()}
    by_id = defaultdict(list)
    for k, vs in sorted(lineage.items()):
        if vs[-1]["project_id"]:
            by_id[k[0], vs[-1]["project_id"]].append(k)
    same = [(ks[0], k, "id", None, None) for ks in by_id.values() for k in ks[1:]]
    ids, how, ledger = assign_ids(minted, same + list(edges), read_ledger(), apart)
    write_ledger(ledger)
    links, members = [], defaultdict(list)
    for k, vs in sorted(lineage.items()):
        method, confidence, evidence = how.get(k, ("id", None, None))
        links.append(
            {
                "project_id": ids[k],
                "register": k[0],
                "key": k[1],
                "source_id": vs[-1]["project_id"],
                "method": method,
                "confidence": confidence,
                "evidence": evidence,
            }
        )
        members[ids[k]].append(k)
    out = []
    for pid, ks in sorted(members.items()):
        last = {k: lineage[k][-1] for k in ks}
        # the transmission row first, else the row still live, seen most recently
        best = max(
            ks,
            key=lambda k: (
                k[0] in TRANSMISSION,
                last[k]["valid_to"] is None,
                last[k]["valid_from"],
                k,
            ),
        )
        b = last[best]
        out.append(
            {
                "project_id": pid,
                "name": b["project_name"],
                "plant_type": b["plant_type"],
                "mw_total": b["mw_total"],
                "status": b["status"],
                "gate": b["gate"],
                "effective_from": b["effective_from"],
                "gsp_id": (site_gsp if best[0] in TRANSMISSION else gsp)(
                    b["connection_site"]
                )
                or None,
                "registers": ";".join(sorted({k[0] for k in ks})),
                "source_ids": ";".join(
                    sorted(
                        {
                            f"{k[0]}:{last[k]['project_id']}"
                            for k in ks
                            if last[k]["project_id"]
                        }
                    )
                )
                or None,
                "members": len(ks),
                "first_seen": min(minted[k] for k in ks),
                "live": any(v["valid_to"] is None for v in last.values()),
            }
        )
    return out, links, ids


# Matching: a TEC row that connects through a DNO ("Embedded") and that DNO's ECR row for the
# same project. Rules score each pair; TypeSafe's Jev model judges the unsure ones. Its answers
# are committed in reference/jev_answers.csv, so a rebuild needs no key and gives the same links.
JEV_URL = "https://api.typesafe.ai/v1/systemone"
JEV_MODEL = "jev-1.14.0"
ANSWERS = REFERENCE / "jev_answers.csv"
ANSWER_COLS = (
    "request",
    "model",
    "asked",
    "tec",
    "ecr",
    "p_different",
    "p_related",
    "p_same",
    "tokens",
)
OVERRIDES = REFERENCE / "overrides.csv"
POSSIBLE_SCHEMA = {
    "project_id": pl.String,  # the TEC row's project
    "register": pl.String,  # the ECR row
    "key": pl.String,
    "other_project_id": pl.String,  # the ECR row's own project
    "probability": pl.Float64,  # Jev's P(same project); empty when Jev wasn't asked
    "evidence": pl.String,
}
# DNO registers by transmission area: SHET is north of Scotland, NGET England and Wales.
# ponytail: SPT (south Scotland) waits for SPEN's register
AREA = {"SHET": ("ssen",), "NGET": ("ssen", "npg")}
# words that say nothing about which project a name means
NAME_STOP = set(
    "ltd limited plc llp the and of at on land farm farms solar pv park parks bess battery "  # noqa: SIM905
    "batteries storage energy energies wind windfarm windfarms turbine turbines project "
    "scheme site substation power generation renewable renewables hybrid centre center hub "
    "grid system systems co uk development developments array estate cva export import "
    "generating station facility hydro gas chp plant".split()
)
SITE_STOP = {
    "gsp",
    "grid",
    "supply",
    "point",
    "substation",
    "sub",
    "bsp",
    "primary",
    "connection",
    "node",
    "new",
    "ss",
}
# a customer name is sent to Jev only if it reads like an organisation: a private person's
# name (some ECR rows name one) never leaves the machine. Unsure means dropped.
COMPANY = re.compile(
    r"\b(ltd|limited|plc|llp|llc|lp|inc|cic|co|company|corporation|group|holdings?|energy|energies"
    r"|power|solar|wind|renewables?|green|farms?|estates?|council|trust|university|college|nhs"
    r"|partners|partnership|services|developments?|storage|bess|properties|investments?|capital"
    r"|association|society|water|utilities|networks?|distribution|generation|infrastructure|uk)\b"
)
STARTED = {"built", "under construction commissioning"}
LEVELS = [
    (
        "They are two different projects: different places, different developers, or different "
        "schemes that just share a word in their names or connect at the same substation."
    ),
    (
        "They are closely related but may not be the same project: another phase or stage of the "
        "same scheme (for example 'Farm 2' and 'Farm 3'), an extension of an existing site, a "
        "sibling site nearby, or one part of a hybrid site (the wind part and the battery part)."
    ),
    "They are one and the same project, described by two different registers.",
]
CONTEXT = (
    "Record `tec` comes from the national transmission register (TEC), which lists projects "
    "that need a transmission agreement, including large projects that connect to a local "
    "distribution network ('Embedded'). Record `ecr` comes from a distribution network "
    "operator's Embedded Capacity Register. The same project is often named differently in the "
    "two registers: a developer's scheme name in one and a site address or company name in the "
    "other, upper or lower case, with or without 'Ltd', 'Solar Farm', 'BESS'. The connection "
    "site is a substation (a grid supply point, GSP); many projects share one. Rules: a "
    "different phase or stage number ('2' vs '3', 'Phase 1' vs 'Phase 2') means a different "
    "project; an 'Extension' is a separate project from the original site; a hybrid site's "
    "wind, solar and battery parts may be listed as separate rows; 49.9 MW is a very common "
    "size, so equal capacity on its own is weak evidence. Capacities and dates can drift between "
    "registers by a few percent or a few months for the same project."
)
DNO_NAME = {
    "ssen": "SSEN (Scottish and Southern Electricity Networks)",
    "npg": "Northern Powergrid",
}


def match_name(s: str | None) -> str:
    """A name with MW mentions and generic words removed; numbers stay ("Farm 2")."""
    s = re.sub(r"\b\d+(\s\d+)?\s?mw[a-z]*\b", " ", norm(s))
    return " ".join(w for w in s.split() if w not in NAME_STOP)


def techs(plant: str | None) -> set[str]:
    s, out = (plant or "").lower(), set()
    for tech, pattern in (
        ("solar", r"solar|photo|pv"),
        ("wind", r"wind"),
        ("bess", r"storage|batter|stored|pump"),
        ("gas", r"gas|ccgt|ocgt|chp|reciprocat|diesel|oil|fossil|engine|thermal"),
        ("hydro", r"hydro(?!.*pump)"),
        ("bio", r"bio|waste|landfill|anaerob"),
        ("demand", r"demand"),
    ):
        if re.search(pattern, s):
            out.add(tech)
    return out or {"other"}


def site_gsp(site: str | None) -> str:
    """GSP ID of a TEC connection site ("Melksham B 132kV Substation" -> MELK_1): the longest
    leading run of words the GSP lookup knows, else the cleaned name."""
    s = re.sub(r"\d+(\s?/\s?\d+)*\s?kv\b", " ", (site or "").lower())
    words = [w for w in norm(s).split() if w not in SITE_STOP and not w.isdigit()]
    for n in range(len(words), 0, -1):
        if " ".join(words[:n]) in gsp_ids():
            return gsp_ids()[" ".join(words[:n])]
    return " ".join(words)


def company(name: str | None) -> str | None:
    return name if name and COMPANY.search(norm(name)) else None


def side(register: str, key: str, v: dict) -> dict:
    """The fields matching uses, from a row's newest version."""
    return {
        "register": register,
        "key": key,
        "name": v["project_name"],
        "site": v["connection_site"],
        "plant": v["plant_type"],
        "mw": v["mw_total"] or v["mw_connected"],
        "connected": v["mw_connected"],
        "status": v["status"],
        "gate": v["gate"],
        "date": v["effective_from"],
        "customer": company(v["customer_name"]),
        "host": v["host_to"],
    }


def company_alike(a: str | None, b: str | None) -> bool:
    """ADTEK LIMITED and ADTEK LTD."""
    return bool(
        a and b and difflib.SequenceMatcher(None, norm(a), norm(b)).ratio() >= 0.8
    )


def similar(a: str, b: str) -> float:
    if not a or not b:
        return 0.0
    ta, tb = set(a.split()), set(b.split())
    return max(len(ta & tb) / len(ta | tb), difflib.SequenceMatcher(None, a, b).ratio())


def mw_within(a: float | None, b: float | None, share: float) -> bool:
    return bool(a and b and abs(a - b) <= share * max(a, b))


def same_tech(t: dict, e: dict) -> bool:
    """A shared technology; a row whose technology is "other" or blank could be anything."""
    a, b = techs(t["plant"]), techs(e["plant"])
    return bool(a & b) or "other" in a | b


def blocked(t: dict, e: dict) -> bool:
    """Worth scoring: the ECR's DNO serves the TEC row's area, a shared technology, and the
    same GSP or a shared distinctive name word (a "Connection Node" site names no GSP)."""
    if e["register"] not in AREA.get(t["host"], ()) or not same_tech(t, e):
        return False
    g = gsp(e["site"])
    # SSEN serves both: north of Scotland GSP IDs end _P, southern England _1 or _6
    if e["register"] == "ssen" and g.endswith(
        ("_1", "_6") if t["host"] == "SHET" else "_P"
    ):
        return False
    if site_gsp(t["site"]) == g:
        return True
    words = {w for w in match_name(t["name"]).split() if len(w) >= 4}
    return bool(words & set(match_name(e["name"]).split()))


def score(t: dict, e: dict) -> tuple[float, bool, str]:
    """(points, phase numbers clash, evidence). Name likeness 0-1 counts 4, the same GSP 1.5,
    MW within 10% 1.5 (25%: 0.7), the same company 1; phase numbers that differ cost 3."""
    sim = similar(match_name(t["name"]), match_name(e["name"]))
    a, b = name_numbers(match_name(t["name"])), name_numbers(match_name(e["name"]))
    clash = bool(a and b and not a & b)
    pts, why = 4 * sim, [f"name {sim:.2f}"]
    if site_gsp(t["site"]) == gsp(e["site"]):
        pts += 1.5
        why.append("same GSP")
    if mw_within(t["mw"], e["mw"], 0.1):
        pts += 1.5
        why.append("MW within 10%")
    elif mw_within(t["mw"], e["mw"], 0.25):
        pts += 0.7
        why.append("MW within 25%")
    if company_alike(t["customer"], e["customer"]):
        pts += 1
        why.append("same company")
    if clash:
        pts -= 3
        why.append("phase numbers differ")
    if t["date"] and t["date"] == e["date"]:
        why.append("same date")
    return round(pts, 2), clash, "; ".join(why)


def rule(pts: float, clash: bool) -> str:
    if clash:
        return "maybe" if pts >= 4 else "no"
    return "link" if pts >= 6 else "maybe" if pts >= 4 else "no"


def jev_request(t: dict, e: dict) -> str:
    """The request body for one pair: no IDs, no keys, no person's name."""

    def clean(d):
        return {k: v for k, v in d.items() if v not in (None, "", [])}

    state = {
        "tec": clean(
            {
                "name": t["name"],
                "connection_site": t["site"],
                "technology": t["plant"],
                "mw": t["mw"],
                "status": t["status"],
                "gate": t["gate"],
                "connection_date": t["date"] and str(t["date"]),
                "customer": t["customer"],
                "transmission_owner_area": t["host"],
            }
        ),
        "ecr": clean(
            {
                "name": e["name"],
                "grid_supply_point": e["site"],
                "technology": e["plant"],
                "mw": e["mw"],
                "status": e["status"],
                "connection_date": e["date"] and str(e["date"]),
                "customer": e["customer"],
                "network_operator": DNO_NAME.get(e["register"], e["register"]),
            }
        ),
    }
    question = {
        "type": "score",
        "instructions": "How do the project described in `tec` and the project described in "
        "`ecr` relate? " + CONTEXT,
        "criteria": LEVELS,
    }
    return json.dumps(
        {"model": JEV_MODEL, "state": state, "questions": {"link_state": question}},
        sort_keys=True,
        default=str,
    )


def jev_key() -> str | None:
    """TYPESAFE_API_KEY from ./.env, else the environment. Never printed or logged."""
    env = Path(".env")
    if env.exists():
        for line in env.read_text(encoding="utf-8-sig").splitlines():
            name, sep, value = line.strip().removeprefix("export ").partition("=")
            if (
                sep
                and name.strip() == "TYPESAFE_API_KEY"
                and value.strip().strip("'\"")
            ):
                return value.strip().strip("'\"")
    return os.environ.get("TYPESAFE_API_KEY") or None


def jev_post(body: str, key: str) -> dict:
    """One request; the key goes only in the Authorization header. Retries when rate-limited."""
    req = urllib.request.Request(
        JEV_URL,
        data=body.encode(),
        method="POST",
        headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
    )
    for attempt in range(5):
        try:
            with urllib.request.urlopen(req, timeout=60) as r:
                return json.load(r)
        except urllib.error.HTTPError as err:
            if err.code != 429 or attempt == 4:
                raise RuntimeError(f"Jev answered HTTP {err.code}") from None
            time.sleep(float(err.headers.get("retry-after") or 2**attempt))


def read_answers() -> dict[str, dict]:
    if not ANSWERS.exists():
        return {}
    with ANSWERS.open(encoding="utf-8", newline="") as f:
        return {r["request"]: r for r in csv.DictReader(f)}


def jev_answers(asks: dict[str, tuple], answers: dict[str, dict]) -> int:
    """Ask Jev about each {request hash: (tec ref, ecr ref, body)} not yet answered, add the
    answers to `answers` and reference/jev_answers.csv. Returns how many were asked; 0 when
    the key is missing (those pairs are decided by the rules alone)."""
    todo = {h: a for h, a in asks.items() if h not in answers}
    if not todo:
        return 0
    key = jev_key()
    if not key:
        print(
            f"notice: TYPESAFE_API_KEY not set; {len(todo)} new pairs decided by rules only"
        )
        return 0
    try:
        with ThreadPoolExecutor(6) as pool:
            got = pool.map(lambda a: jev_post(a[2], key), todo.values())
            for (h, (tec, ecr, _)), out in zip(todo.items(), got):
                p = out["answers"]["link_state"]["probabilities"]
                answers[h] = {
                    "request": h,
                    "model": out["model"],
                    "asked": date.today().isoformat(),
                    "tec": tec,
                    "ecr": ecr,
                    "p_different": p["0"],
                    "p_related": p["1"],
                    "p_same": p["2"],
                    "tokens": out["usage"]["input_tokens"],
                }
    finally:  # a failed request still keeps the answers already paid for
        with ANSWERS.open("w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, ANSWER_COLS, lineterminator="\n")
            w.writeheader()
            w.writerows(
                sorted(
                    answers.values(), key=lambda r: (r["tec"], r["ecr"], r["request"])
                )
            )
    print(f"asked Jev about {len(todo)} pairs")
    return len(todo)


def decide(t: dict, e: dict, p_same: float | None) -> tuple[str | None, str, str]:
    """(outcome: "link", "possible" or None, method, evidence) for one scored pair.

    Rules first. With Jev's P(same): a rule link stands at 0.5 or more; a rule "maybe" becomes
    a link at 0.9 or more and is dropped under 0.3; a rule "no" is possible only at 0.9 or
    more. Without an answer, rule links stand and "maybe" is possible. Never a link: a built
    ECR row with a TEC row that hasn't started (Novar 2 is a new farm beside the built one)."""
    pts, clash, why = score(t, e)
    r = rule(pts, clash)
    why = f"rules {pts} ({why})"
    if p_same is None:
        out, method = {"link": "link", "maybe": "possible"}.get(r), "rule"
        why += "; rules only, Jev not asked"
    else:
        why += f"; Jev P(same) {p_same:.2f}"
        out = {
            "link": "link" if p_same >= 0.5 else "possible",
            "maybe": "link" if p_same >= 0.9 else None if p_same < 0.3 else "possible",
            "no": "possible" if p_same >= 0.9 else None,
        }[r]
        method = "rule" if r == "link" else "jev"
    built = e["status"] == "connected" or (e["connected"] or 0) > 0
    started = norm(t["status"]) in STARTED or (t["connected"] or 0) > 0
    if out == "link" and built and not started:
        out, why = "possible", why + "; ECR row built, TEC row not started"
    return out, method, why


def asked(t: dict, e: dict) -> bool:
    """Pairs worth a Jev question: every rule link and maybe, near-miss rule "no"s, and ECR
    rows at the same GSP named only by size and technology ("67.2MW WIND & 36.3MW BATTERY")."""
    pts, clash, _ = score(t, e)
    placeholder = not match_name(e["name"]) and site_gsp(t["site"]) == gsp(e["site"])
    return rule(pts, clash) != "no" or pts >= 3 or placeholder


def read_overrides(lineage: dict) -> list[tuple]:
    """reference/overrides.csv rows (action, row a, row b, why); rows are named
    `register:key` or `register:source ID`. A name that matches no row stops the build."""
    if not OVERRIDES.exists():
        return []
    ref = {f"{k[0]}:{k[1]}": k for k in lineage}
    for k, vs in lineage.items():
        if vs[-1]["project_id"]:
            ref.setdefault(f"{k[0]}:{vs[-1]['project_id']}", k)
    out = []
    with OVERRIDES.open(encoding="utf-8", newline="") as f:
        for r in csv.DictReader(f):
            missing = [x for x in (r["a"], r["b"]) if x not in ref]
            if missing or r["action"] not in ("link", "separate"):
                sys.exit(
                    f"reference/overrides.csv: no such row {missing} or action {r['action']!r}"
                )
            out.append((r["action"], ref[r["a"]], ref[r["b"]], r["why"]))
    return out


def rank(p: float | None, pts: float) -> float:
    """Which link an ECR row keeps, best first: Jev's probability plus the rule points out
    of 8, so the better name wins a near tie (Braco Battery and Braco West BESS at one GSP)."""
    return (p or 0) + pts / 8


def match(versions: list[dict]) -> tuple[list[tuple], list[tuple], set, dict]:
    """(edges for projects(), possible pairs (tec row, ecr row, P(same), evidence), rows an
    override set apart, counts). Each ECR row links to at most one TEC row, its best; a TEC
    row may link to several ECR rows (a hybrid's wind and battery rows)."""
    lineage = lineages(versions)
    # a Directly Connected project never appears in an ECR
    tec = [
        side(*k, vs[-1])
        for k, vs in lineage.items()
        if k[0] in TRANSMISSION
        and vs[-1]["host_to"] in AREA
        and vs[-1]["agreement_type"] in ("Embedded", None)
    ]
    ecr = [side(*k, vs[-1]) for k, vs in lineage.items() if k[0] not in TRANSMISSION]
    by_reg = defaultdict(list)
    for e in ecr:
        by_reg[e["register"]].append(e)
    pairs = [
        (t, e)
        for t in tec
        for reg in AREA[t["host"]]
        for e in by_reg[reg]
        if blocked(t, e)
    ]
    answers = read_answers()
    ref = lambda r: f"{r['register']}:{r['key']}"
    asks, hashes = {}, {}
    for t, e in pairs:
        if asked(t, e):
            body = jev_request(t, e)
            # two rows with the same fields send the same request: one answer serves both
            h = hashes[ref(t), ref(e)] = hashlib.sha256(body.encode()).hexdigest()
            asks[h] = (ref(t), ref(e), body)
    new = jev_answers(asks, answers)
    found = []
    for t, e in pairs:
        a = answers.get(hashes.get((ref(t), ref(e))))
        p = None if a is None else float(a["p_same"])
        out, method, why = decide(t, e, p)
        if out:
            found.append((out, p, score(t, e)[0], t, e, method, why))
    overrides = read_overrides(lineage)
    apart = {frozenset((a, b)) for act, a, b, _ in overrides if act == "separate"}
    # an override link replaces whatever the matcher found for that ECR row
    forced = {
        b: (a, b, "override", 1.0, why) for act, a, b, why in overrides if act == "link"
    }
    edges, possible, taken = list(forced.values()), [], set(forced)
    for out, p, pts, t, e, method, why in sorted(
        found, key=lambda f: (f[0] != "link", -rank(f[1], f[2]))
    ):
        a, b = (t["register"], t["key"]), (e["register"], e["key"])
        if frozenset((a, b)) in apart:
            continue
        if out == "link" and b not in taken:
            taken.add(b)
            edges.append((a, b, method, p, why))
        else:
            possible.append((a, b, p, why))
    linked = {b for _, b, *_ in edges}
    possible = [x for x in possible if x[1] not in linked]
    counts = {
        "tec rows": len(tec),
        "pairs scored": len(pairs),
        "jev questions": len(asks),
        "jev asked now": new,
        "jev tokens": sum(int(answers[h]["tokens"]) for h in asks if h in answers),
    }
    return edges, possible, {b for x in apart for b in x}, counts


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
    edges, possible, apart, counts = match(versions)
    project, links, ids = projects(versions, edges, apart)
    print("matching:", ", ".join(f"{v} {k}" for k, v in counts.items()))
    # a link kept from an earlier build can put both rows in one project already
    rows = [
        (ids[a], *b, ids[b], p, why) for a, b, p, why in possible if ids[a] != ids[b]
    ]
    print(f"matching: {len(edges)} links, {len(rows)} possible matches")
    save("source_snapshot", pl.concat(all_snaps), SNAP_SCHEMA)
    save("possible_match", rows, POSSIBLE_SCHEMA)
    save("project", [[p[c] for c in PROJECT_SCHEMA] for p in project], PROJECT_SCHEMA)
    save("project_link", [[x[c] for c in LINK_SCHEMA] for x in links], LINK_SCHEMA)
    # `project_id` is now the Gridline ID; the register's own ID becomes `source_id`
    for name, rows, schema in (
        ("event", events, EVENT_SCHEMA),
        ("project_version", versions, VERSION_SCHEMA),
    ):
        df = pl.DataFrame(
            [[r[c] for c in schema] for r in rows], schema=schema, orient="row"
        )
        gl = pl.Series([ids[r["register"], r["key"]] for r in rows], dtype=pl.String)
        df = df.rename({"project_id": "source_id"}).with_columns(project_id=gl)
        save(name, df.select("project_id", pl.exclude("project_id")), schema)


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
