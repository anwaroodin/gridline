# Gridline

**A public history of Great Britain's grid connections queue, release by release.**

[![daily](https://img.shields.io/github/actions/workflow/status/anwaroodin/gridline/daily.yml?label=daily&logo=github)](https://github.com/anwaroodin/gridline/actions/workflows/daily.yml)
[![Licence: Apache-2.0](https://img.shields.io/badge/licence-Apache--2.0-blue)](LICENSE)
[![Python 3.12+](https://img.shields.io/badge/python-3.12%2B-blue?logo=python&logoColor=white)](https://www.python.org/)
[![Data: NESO Open Data Licence](https://img.shields.io/badge/data-NESO%20Open%20Data%20Licence-green)](https://www.neso.energy/data-portal/neso-open-licence)

## Why this exists

New solar farms, wind farms and batteries in Great Britain can't be built until they have a grid
connection, and the queue for those connections is the main bottleneck. The National Energy System
Operator (NESO) publishes that queue as the Transmission Entry Capacity (TEC) register. The register
only shows the current state: each new release overwrites the last, and NESO keeps no past releases.
Smaller projects connect to the regional networks instead, and each Distribution Network Operator
(DNO) publishes its own Embedded Capacity Register (ECR), monthly, again overwriting the last.
In its September 2026 "Transforming Connections Data" consultation, Ofgem said stakeholders can't get
a complete and consistent picture of the queue. Gridline keeps the history.

## What it does today

- **Archives every release byte for byte**, named by its sha256, with a manifest of when it was
  fetched and published. Sources today:

  | Source | Register | Cadence |
  |---|---|---|
  | `tec` | NESO [TEC Register](https://www.neso.energy/data-portal/transmission-entry-capacity-tec-register) | about twice a week |
  | `ssen` | SSEN Distribution [Embedded Capacity Register](https://data.ssen.co.uk/dataset/655eb750-7b3f-4c55-93b6-9bc4c26a57ab), 1 MW and above | monthly |
  | `npg` | Northern Powergrid [Embedded Capacity Register](https://northernpowergrid.opendatasoft.com/explore/dataset/embedded-capacity-register/), 1 MW and over | monthly |

  The other four DNOs are planned: UKPN, SP Energy Networks and SP Electricity North West need an
  API key, and NGED's licence for redistribution is being checked.
- **Turns the differences between releases into typed events**: a project added or withdrawn, a
  capacity change, a connection date that slipped, a status change, a gate assigned, a first
  connection.

Example output, from the test fixtures (real rows with hand edits, **not real changes**):

| event_type | project | old | new | days | low_confidence |
|---|---|---|---|---|---|
| `added` | BESS Harker (PRO-002010) | | | | yes |
| `added` | Example New Solar (no ID) | | | | yes |
| `withdrawn` | Artfield Forest Wind Farm (PRO-003578) | | | | |
| `status_changed` | Arbroath Battery Substation (PRO-000950) | Consents Approved | Built | | |
| `connected` | Arbroath Battery Substation (PRO-000950) | 0.0 | 35.0 | | |
| `date_slipped` | Aberarder Wind Farm (PRO-000368) | 2026-08-31 | 2026-11-30 | 91 | |
| `status_changed` | Blarghour Wind (PRO-000398) | Consents Approved | Built | | |
| `capacity_changed` | Al Boum Photo (Norton) (PRO-001651) | 275.0 | 300.0 | | |
| `status_changed` | Beaw Field Wind Farm (PRO-000026) | Consents Approved | Under Construction/Commissioning | | |
| `gate_assigned` | Barking BESS (PRO-003518) | | 2 | | |

## Who it's for

- **Developers** checking how their project and its neighbours move in the queue
- **Analysts and network planners** who need a consistent time series rather than a single snapshot
- **Journalists and researchers** asking how connections reform is playing out

## How it works

```mermaid
flowchart LR
    A[NESO, SSEN, NPg APIs] -->|gridline fetch| B[archive/SOURCE/*.csv<br/>+ snapshots.csv]
    B -->|gridline diff| C[data/project_version.parquet]
    B -->|gridline diff| D[data/event.parquet]
```

TEC rows are matched between releases on **Project ID + Stage**; ECR rows on the DNO's own row ID
(SSEN `Unique ID` without its status suffix, NPg `ecrid`). A row without a Project ID falls back to
its normalised name, connection site, plant type and stage. Rows that this key can't match
one-to-one (no Project ID, or a key repeated within one release) are kept and flagged
`low_confidence` instead of being hidden.

## Quick start

Requires [uv](https://docs.astral.sh/uv/).

```sh
git clone https://github.com/anwaroodin/gridline && cd gridline
uv sync
uv run gridline fetch   # archive each source's current release (skips ones already archived)
uv run gridline fetch ssen   # or just some sources
uv run gridline diff    # rebuild data/*.parquet from the whole archive
uv run gridline check   # exit non-zero if a source has had no new release for too long
uv run pytest
```

Read the events with polars:

```python
import polars as pl

events = pl.read_parquet("data/event.parquet")
print(events.group_by("event_type").len())
print(
    events.filter(pl.col("event_type") == "date_slipped")
    .sort("days", descending=True)
    .head()
)
```

## Data model

| Output | One row per |
|---|---|
| `archive/<source>/snapshots.csv` (committed; also `data/source_snapshot.parquet`) | archived release: snapshot_id, source, fetched_at, published, sha256, raw_path, rows |
| `data/project_version.parquet` | version of a register row, valid from `valid_from` until `valid_to` (empty = current) |
| `data/event.parquet` | change between two consecutive releases |

| Event | Meaning |
|---|---|
| `added` | the row appears for the first time |
| `withdrawn` | the row is no longer in the TEC register |
| `stage_split` | a TEC Project ID that had one Stage now has several: it stays one project. `old_value` is the old key, `new_value` the new stage keys (`;`-separated), instead of `withdrawn` + `added` |
| `removed_from_register` | the row is no longer in a DNO register. Not called withdrawn: under connections reform some projects moved to Gate 1 and were removed from ECRs |
| `capacity_changed` | TEC: Cumulative Total Capacity (MW) changed. ECR: the registered capacity (accepted-to-connect, or already-connected once connected) changed |
| `date_slipped` | TEC: MW Effective From moved. ECR: Target Energisation Date moved (NPg leaves it blank, so none there). `days` is positive when later, negative when brought forward |
| `status_changed` | TEC: Project Status changed (for example Scoping → Consents Approved). ECR: accepted to connect → connected |
| `gate_assigned` | the Gate column was set or changed (Gate 1 / Gate 2 under connections reform) |
| `connected` | MW Connected (ECR: already-connected capacity) rose above 0 for the first time |

Every event carries `register` (`tec`, `ssen`, `npg`), `low_confidence`, `project_id`, `project_number`, `old_value` and `new_value`.

## Data and archive

- Raw releases live in [`archive/<source>/`](archive/), committed to this repository. The registers
  overwrite themselves, so this archive is the history. (SSEN and NPg also offer older
  monthly workbooks; loading those is not built yet.)
- `.gitattributes` marks the archive `-text`, so git never rewrites line endings. A changed byte
  would change the file's sha256.
- A [daily GitHub Action](.github/workflows/daily.yml) runs `fetch` and `diff` and commits any new
  release to `main`, one commit per source. One failing source doesn't stop the others; the run
  still fails afterwards so someone notices.
- A release is refused, and nothing is written, if it has no data rows (a login-gated portal returns
  just the header), under half the rows of the latest release (a cut-off download), or no IDs when
  the latest release had them (a renamed ID column). If the drop is real, `gridline fetch --force
  SOURCE` archives it anyway.
- **History starts on 19 November 2026** for TEC, because NESO publishes no earlier files, and in
  October 2026 for SSEN.
- If you hold older TEC register CSVs, `gridline backfill` loads them. The release date must be in the
  file name, for example `tec-register-28-february-2025.csv`:

  ```sh
  uv run gridline backfill path/to/tec-register-28-february-2025.csv
  ```

  Contributions of past releases are very welcome. Please open an issue.

## Roadmap

- [x] Phase 0: TEC register archive and events
- [ ] Phase 1: the six DNO Embedded Capacity Registers (SSEN and NPg done)
- [ ] Planned: cross-register project matching
- [ ] Planned: REST API
- [ ] Planned: web app with map, project history and reform view
- [ ] Planned: delivery-risk model

## Contributing

- Use [Conventional Commits](https://www.conventionalcommits.org/) (`feat:`, `fix:`, `docs:`, ...).
- Before opening a pull request, run `uv run pytest`, `uvx ruff format` and `uvx ruff check`.
- For larger changes, open an issue first so we can agree on the approach.

## Licence and attribution

Code: [Apache-2.0](LICENSE).

Data, by source:

- **NESO (`tec`):** Contains data from the National Energy System Operator (NESO), published under
  the [NESO Open Data Licence](https://www.neso.energy/data-portal/neso-open-licence). Source:
  [TEC Register](https://www.neso.energy/data-portal/transmission-entry-capacity-tec-register).
- **SSEN (`ssen`):** Contains data from SSEN Distribution, licensed under
  [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/). Source:
  [Embedded Capacity Register](https://data.ssen.co.uk/dataset/655eb750-7b3f-4c55-93b6-9bc4c26a57ab).
  The raw files in `archive/ssen/` are unchanged; the derived data in `data/` was changed by
  Gridline (columns renamed and reduced, values normalised, events computed between releases).
- **Northern Powergrid (`npg`):** Supported by Northern Powergrid Open Data, under the
  [Northern Powergrid Open Data Licence](https://northernpowergrid.opendatasoft.com/p/opendatalicence/).
  Source: [Embedded Capacity Register](https://northernpowergrid.opendatasoft.com/explore/dataset/embedded-capacity-register/).
  Its export includes derived location fields: Contains OS data © Crown copyright and database
  right. Office for National Statistics licensed under the Open Government Licence v.3.0.
