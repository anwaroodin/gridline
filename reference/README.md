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

## labels.csv

About 470 pairs of a TEC row and an ECR row, labelled by hand: `same`, `related` (another
phase, a part of the site, or an older row) or `different`, with `unsure` set where the label
is a guess, and the `evidence` it rests on (names, capacity, dates, companies). It holds every
link and possible match of a full build, a random sample of rejected pairs, the rejected pairs
closest to a match, a sample of NESO Embedded Register pairs, and pairs found by hand for TEC
rows the matcher missed (`stratum`). Each row keeps both rows' fields, so `pytest` rebuilds every decision without
downloading anything, and fails if link precision falls under 97%, review recall (true pairs
linked or listed as possible) under 90%, or blocking recall (pairs found by hand that the
matcher scores at all) under 85%.
