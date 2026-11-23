# Gridline

An open tracker for Great Britain's grid connections queue. Phase 0 archives every release of the
NESO Transmission Entry Capacity (TEC) register and turns release-to-release differences into typed events.

## Run

```sh
uv sync
uv run gridline fetch                 # archive the release NESO currently publishes (skips known hashes)
uv run gridline backfill FILE_OR_URL  # archive past releases; the date must be in the name, e.g. tec-register-28-february-2025.csv
uv run gridline diff                  # rebuild data/project_version.parquet and data/event.parquet
uv run pytest
```

Outputs, all in `data/` (gitignored for now):

- `raw/tec/<sha256>.csv`: each release byte for byte
- `source_snapshot.parquet`: one row per archived release (id, source, fetched_at, published, sha256, raw_path, rows)
- `project_version.parquet`: each register row with `valid_from` / `valid_to` publication dates
- `event.parquet`: `added`, `withdrawn`, `capacity_changed`, `date_slipped` (`days`; negative means brought forward),
  `stage_changed` (Project Status), `gate_assigned`, `connected`

Rows are keyed on `Project ID` + `Stage`. A row without a Project ID falls back to its normalised name, site,
plant type and stage. Rows whose key repeats within a release, or that have no Project ID, are flagged `low_confidence`.

A GitHub Action (`.github/workflows/daily.yml`) runs fetch and diff every day.

## Data licence

Contains data from the National Energy System Operator (NESO), published under the
[NESO Open Data Licence](https://www.neso.energy/data-portal/neso-open-licence).
Source: [TEC Register](https://www.neso.energy/data-portal/transmission-entry-capacity-tec-register).

Code: Apache-2.0 (see `LICENSE`).
