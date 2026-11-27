# Reference tables

Small hand-kept tables that `gridline diff` reads. Edit them by pull request.

## gsp.csv

One row per spelling of a Grid Supply Point (GSP): `name`, `gsp_id`, `dno` (the network area the
GSP is in) and `source`.

- `neso` rows are the region names from NESO's "GSP - Gnode - Direct Connect - Region Lookup"
  (dataset [GIS Boundaries for GB Grid Supply Points](https://www.neso.energy/data-portal/gis-boundaries-gb-grid-supply-points)),
  with `gsp_id` its `gsp_name` and `dno` taken from its GSP group.
- `alias` rows are other spellings seen in the registers, such as SSEN's codes before October
  2023 (`MELK`) and its names since (`MELKSHAM GSP`). Each alias was checked against rows that
  appear in both spellings in consecutive releases. A GSP that NESO's lookup doesn't list gets
  SSEN's old code as its ID (`BERB` for Berryburn).

Names are compared after lower-casing, dropping punctuation and a trailing "GSP" or "Grid".
A site that isn't in the table is keyed by that cleaned name, so a missing row costs nothing
worse than before.

Contains data from the National Energy System Operator (NESO), published under the
[NESO Open Data Licence](https://www.neso.energy/data-portal/neso-open-licence).
