"""Cloud-agnostic hub backfill integration tests.

Exercises: compile -> upload diffs -> backfill -> poll workflow to success.
Replaces the former test_gcp_hub_quickstart.py and test_aws_hub_quickstart.py.
"""

import pytest
from click.testing import CliRunner

from .helpers.cli import compile_configs, submit_backfill, submit_check_partitions
from .helpers.workflow import poll_workflow

# Demo join conf paths differ across clouds (variable names / versions vary).
DEMO_DERIVATIONS = {
    "gcp": "compiled/joins/gcp/demo.derivations_v1__2",
    "aws": "compiled/joins/aws/demo.derivations_v1__2",
    "azure": "compiled/joins/azure/demo.derivations_v3",
}


@pytest.mark.integration
def test_backfill_no_data(confs, chronon_root, hub_url, cloud):
    """Backfill with dates that have no input data should result in a failed workflow."""
    runner = CliRunner()
    compile_configs(runner, chronon_root)

    workflow_id = submit_backfill(
        runner, chronon_root, hub_url,
        confs(DEMO_DERIVATIONS[cloud]), "1969-01-01", "1969-01-01",
    )
    with pytest.raises(RuntimeError, match="ended with status FAILED"):
        poll_workflow(hub_url, workflow_id, timeout=1800, interval=45)


# Conf for multi-day backfill that expects success.
# GCP/AWS: join derivation (exercises full multi-step DAG).
# Azure: staging query (user_activities/checkouts not yet seeded in Snowflake canary).
MULTIDAY_BACKFILL = {
    "gcp": "compiled/joins/gcp/demo.derivations_v1__2",
    "aws": "compiled/joins/aws/demo.derivations_v1__2",
    "azure": "compiled/staging_queries/azure/exports.dim_listings__0",
}


@pytest.mark.integration
def test_backfill_multiday(confs, chronon_root, hub_url, cloud):
    """Multi-day backfill exercises multi-step allocation."""
    runner = CliRunner()
    compile_configs(runner, chronon_root)

    workflow_id = submit_backfill(
        runner, chronon_root, hub_url,
        confs(MULTIDAY_BACKFILL[cloud]),
        "2026-03-01", "2026-03-03",
    )
    poll_workflow(hub_url, workflow_id, timeout=1800, interval=45)


@pytest.mark.integration
def test_backfill_start_cutoff_enforcement(
    test_id, confs, chronon_root, hub_url, cloud, version
):
    """TableDependency.start_cutoff is enforced end-to-end by the orchestrator.

    Fixture: python/test/canary/staging_queries/gcp/cutoff_example.py
      - downstream depends on export_a with plain offset=0
      - downstream depends on export_b with start_cutoff="2026-02-25"

    Backfilling downstream for [2026-03-01, 2026-03-03] requires:
      - export_a partitions for 3 days (2026-03-01..03-03) — matches the range.
      - export_b partitions for 7 days (2026-02-25..2026-03-03) — the platform
        orchestrator expands the dep range to [start_cutoff, query_end] and
        requires every date to be Filled on the upstream.
      - downstream partitions for 3 days (2026-03-01..03-03).

    If start_cutoff were ignored the platform would have scheduled export_b
    for only 3 days; the presence of partitions 2026-02-25..02-28 is the load-
    bearing assertion.
    """
    if cloud != "gcp":
        pytest.skip("cutoff_example fixture is GCP-only")

    runner = CliRunner()
    compile_configs(runner, chronon_root)

    start_ds, end_ds = "2026-03-01", "2026-03-03"
    downstream_conf = confs("compiled/staging_queries/gcp/cutoff_example.downstream__0")
    workflow_id = submit_backfill(
        runner, chronon_root, hub_url, downstream_conf, start_ds, end_ds,
    )
    poll_workflow(hub_url, workflow_id, timeout=1800, interval=45)

    team_metadata = f"compiled/teams_metadata/{cloud}/{cloud}_team_metadata"

    def check(table: str, dates: list[str]):
        for ds in dates:
            submit_check_partitions(
                runner, chronon_root, team_metadata, version,
                f"{table}/ds={ds}",
            )

    backfill_range = ["2026-03-01", "2026-03-02", "2026-03-03"]
    check(f"data.{cloud}_cutoff_example_{test_id}_downstream__0", backfill_range)
    check(f"data.{cloud}_cutoff_example_{test_id}_export_a__0", backfill_range)
    # export_b must have the 4 pre-backfill days demanded by start_cutoff.
    check(
        f"data.{cloud}_cutoff_example_{test_id}_export_b__0",
        [
            "2026-02-25", "2026-02-26", "2026-02-27", "2026-02-28",
            "2026-03-01", "2026-03-02", "2026-03-03",
        ],
    )
