"""tec-register-19-november-2026.csv: 10 verbatim lines of the real 19 Nov 2026 release.
tec-register-01-december-2026-EDITED.csv: the same rows with hand edits (only one release is retrievable):
PRO-001651 total 275 -> 300 MW; PRO-000368 date +91 days; PRO-000950 -> Built, 0 -> 35 MW connected;
PRO-003518 gate -> 2; PRO-000026 -> Under Construction; PRO-001624-1 connected 200 -> 250 MW (no event);
PRO-000398 -> Built with 0 MW (no `connected`) and MW Increase "TBC"; PRO-003578 removed;
PRO-002010 gets a second line with the same Project ID and blank Stage; one new PV row with no Project ID."""

from pathlib import Path

from gridline import build, published_from_name, read_tec

FIX = Path(__file__).parent / "fixtures"


def test_events_between_fixtures():
    snaps = [(p.stem, published_from_name(p.name), read_tec(p))
             for p in sorted(FIX.glob("*.csv"), key=lambda p: published_from_name(p.name))]
    events, versions = build(snaps)
    got = {(e["project_number"] or e["project_name"], e["event_type"], e["new_value"], e["days"], e["low_confidence"])
           for e in events}
    assert got == {
        ("PRO-001651", "capacity_changed", "300.0", None, False),
        ("PRO-000368", "date_slipped", "2026-11-30", 91, False),
        ("PRO-000950", "stage_changed", "Built", None, False),
        ("PRO-000950", "connected", "35.0", None, False),
        ("PRO-000398", "stage_changed", "Built", None, False),
        ("PRO-003518", "gate_assigned", "2", None, False),
        ("PRO-000026", "stage_changed", "Under Construction/Commissioning", None, False),
        ("PRO-003578", "withdrawn", None, None, False),
        ("PRO-002010", "added", None, None, True),         # repeated Project ID + Stage: can't match one-to-one
        ("Example New Solar", "added", None, None, True),  # no Project ID: normalised name key
    }
    assert len(events) == len(got)
    assert next(r for r in snaps[1][2] if r["project_number"] == "PRO-000398")["mw_change"] is None  # "TBC"
    # 9 rows kept from the first release + 2 added rows
    assert sum(v["valid_to"] is None for v in versions) == 11
    assert [v["valid_to"] for v in versions if v["project_number"] == "PRO-003578"] == [snaps[1][1]]
