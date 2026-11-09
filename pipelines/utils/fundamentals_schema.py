"""Column names for the derived Compustat query tables."""

import polars as pl

ANNUAL_FUNDAMENTAL_FIELDS = (
    "at", "act", "che", "rect", "invt", "ppent", "lct", "dlc", "dltt", "lt", "seq",
    "sale", "cogs", "gp", "xrd", "xsga", "oiadp", "xint", "txt", "ni", "epspx", "epsfx",
    "pi", "dp",
    "oancf", "ivncf", "fincf", "capx", "dpc", "chech", "cdvc", "pdvc", "dltis", "dltr",
    "dlcch", "prstkc", "prstkcc", "prstkpc",
    "csho", "cshpri", "cshfd", "dvpsp_f", "dvpsx_f", "dvc", "dvp", "dvt", "dvpd",
)

QUARTERLY_FUNDAMENTAL_FIELDS = (
    "atq", "actq", "cheq", "rectq", "invtq", "ppentq", "lctq", "dlcq", "dlttq", "ltq", "seqq",
    "saleq", "cogsq", "gpq", "xrdq", "xsgaq", "oiadpq", "xintq", "txtq", "niq", "epspxq", "epsfxq",
    "piq", "dpq",
    "oancfy", "ivncfy", "fincfy", "capxy", "dpcy", "chechy", "cdvcy", "pdvcy", "dltisy", "dltry",
    "dlcchy", "prstkcy", "prstkccy", "prstkpcy",
    "cshoq", "cshprq", "cshfdq", "dvpspq", "dvpsxq", "dvpq",
)

QUERY_PROVENANCE_COLUMNS = (
    "mapping_date", "mapping_snapshot_id", "mapping_snapshot_path", "mapping_provenance",
    "mapping_relationships_json",
    "compustat_efffrom", "compustat_effthru", "barra_start_date", "barra_end_date",
    "source_snapshot_id", "source_report_path", "source_balance_sheet_path",
    "source_income_statement_path", "source_cash_flow_path", "source_capital_metrics_path",
    "source_field_definitions_json", "materialization_id",
)


def fundamentals_table_schema(frequency: str) -> dict:
    if frequency == "annual":
        fiscal = {"fyear": pl.Int32}
        metrics = ANNUAL_FUNDAMENTAL_FIELDS
    elif frequency == "quarterly":
        fiscal = {"fyearq": pl.Int32, "fqtr": pl.Int32}
        metrics = QUARTERLY_FUNDAMENTAL_FIELDS
    else:
        raise ValueError("frequency must be annual or quarterly")

    schema = {
        "barrid": pl.String,
        "gvkey": pl.String,
        "datadate": pl.Date,
        "fyr": pl.Int32,
        **fiscal,
        "currency": pl.String,
        "rdq": pl.Date,
        "indfmt": pl.String,
        "consol": pl.String,
        "popsrc": pl.String,
        "datafmt": pl.String,
        "mapping_status": pl.String,
        "barra_mapping_status": pl.String,
        **{name: pl.Date for name in ("mapping_date", "compustat_efffrom", "compustat_effthru", "barra_start_date", "barra_end_date")},
        **{name: pl.String for name in QUERY_PROVENANCE_COLUMNS if name not in {
            "mapping_date", "compustat_efffrom", "compustat_effthru", "barra_start_date", "barra_end_date"
        }},
        **{name: pl.Float64 for name in metrics},
    }
    return schema


def fundamentals_table_keys(frequency: str | None = None) -> list[str]:
    """Return logical keys, including fiscal labels where quarterly dates collide."""
    if frequency == "annual":
        fiscal_keys = ["fyear", "fyr"]
    elif frequency == "quarterly":
        fiscal_keys = ["fyearq", "fqtr", "fyr"]
    elif frequency is None:
        fiscal_keys = []
    else:
        raise ValueError("frequency must be annual or quarterly")
    return [
        "barrid", "gvkey", "datadate", "indfmt", "consol", "popsrc", "datafmt",
        *fiscal_keys, "mapping_date", "mapping_snapshot_id", "source_snapshot_id", "materialization_id",
    ]
