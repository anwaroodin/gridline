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
import urllib.parse
import urllib.request
from collections import Counter, defaultdict
from collections.abc import Iterator
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
                "gsp_id": gsp(b["connection_site"]) or None,
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
    project, links, ids = projects(versions)
    save("source_snapshot", pl.concat(all_snaps), SNAP_SCHEMA)
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
