"""Build derived annual and quarterly Compustat query tables from saved files."""

from __future__ import annotations

import json
from pathlib import Path

import click

from pipelines.fundamentals_query import build_compustat_query_tables
from pipelines.utils.enums import DatabaseName


@click.command()
@click.option("--compustat-root", type=click.Path(exists=True, file_okay=False, path_type=Path), required=True)
@click.option("--run-id", multiple=True, required=True, help="Completed source collection run ID; may be repeated.")
@click.option("--mapping-snapshot", type=click.Path(exists=True, dir_okay=False, path_type=Path), required=True)
@click.option("--mapping-date", type=click.DateTime(formats=["%Y-%m-%d"]), required=True)
@click.option("--version-policy", type=click.Choice(["latest", "specific"]), required=True)
@click.option("--versions-file", type=click.Path(exists=True, dir_okay=False, path_type=Path), default=None)
@click.option(
    "--target-database",
    type=click.Choice([DatabaseName.DEVELOPMENT.value, DatabaseName.RESEARCH.value, DatabaseName.PRODUCTION.value]),
    default=DatabaseName.DEVELOPMENT.value,
    show_default=True,
)
@click.option("--rebuild-scope", is_flag=True, help="Replace all existing derived Compustat query-table years in this target database.")
def main(compustat_root, run_id, mapping_snapshot, mapping_date, version_policy, versions_file, target_database, rebuild_scope):
    """Materialize local reports only; this command does not call a vendor API."""
    if version_policy == "specific" and versions_file is None:
        raise click.UsageError("--versions-file is required when --version-policy=specific")
    if version_policy == "latest" and versions_file is not None:
        raise click.UsageError("--versions-file is valid only with --version-policy=specific")
    selected_policy = "latest"
    if versions_file is not None:
        try:
            selected_policy = json.loads(versions_file.read_text())
        except (OSError, json.JSONDecodeError) as exc:
            raise click.ClickException(f"Cannot read versions file: {exc}") from exc
        if not isinstance(selected_policy, dict) or not all(
            isinstance(key, str) and isinstance(value, str)
            for key, value in selected_policy.items()
        ):
            raise click.ClickException("Versions file must be a JSON object mapping report identity keys to snapshot IDs")
    try:
        result = build_compustat_query_tables(
            compustat_root=compustat_root,
            run_ids=list(run_id),
            mapping_snapshot_path=mapping_snapshot,
            mapping_date=mapping_date.date(),
            version_policy=selected_policy,
            target_database=DatabaseName(target_database),
            rebuild_scope=rebuild_scope,
        )
    except (FileNotFoundError, ValueError) as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
