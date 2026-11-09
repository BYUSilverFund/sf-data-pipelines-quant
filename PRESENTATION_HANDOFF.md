# Compustat to Barrid query: presentation handoff

## Status

The selected full-universe run completed successfully and the derived annual and
quarterly development tables are materialized and validated. This handoff uses
only saved collection, archive, mapping, and query-table files. No data was
collected during this validation; the original source extracts remain
unchanged.

## Problem solved and pipeline flow

Quants can now work with Compustat financial metrics through the repository's
`Database` and Polars interface, using Barrid-linked rows without manually
locating GVKEYs or archive folders.

1. The saved Russell membership snapshot is mapped through Compustat security
   history and the saved Barra security mapping. The complete security mapping
   remains a separate Parquet dataset.
2. The collection retains annual `comp.funda` and quarterly `comp.fundq`
   extracts, coverage and completion reports, and per-report versioned
   statement bundles.
3. The query builder verifies the completed run, exact mapping snapshot,
   format filters, archive identity, and extraction version. It reads values
   and metadata from the same immutable report snapshot.
4. `Database.fundamentals_annual_table` and
   `Database.fundamentals_quarterly_table` expose separate yearly Parquet
   datasets as Polars `LazyFrame`s.

## Collection and build scope

- Run: `run_bd766d2869a3a83fce895048`
- Collection dates: 2024-01-01 through 2026-10-29
- Membership snapshot date: 2026-08-31
- Membership: 2,970 securities; 2,949 eligible GVKEYs
- Selected series: `INDL/C/D/STD`
- Mapping snapshot:
  `/home/gunnjake/groups/grp_quant/database/fundamentals/compustat/security_mappings/membership_2026-08-31_d2afd3aa91a238f6a5b679a7a611c22dfea6a3c1a6b9c5472a768cc7f7bc95f3.parquet`
- Extraction-version policy: `latest`, resolved per report through the saved
  `latest_snapshot.json` pointer
- Materialization ID:
  `d8ae2e933d77d9e144988cc2306f5370ba4a8a4626880c9d473dc7c657f5657f`
- Manifest status: `complete`; 0 quarantined report identities
- Source run completion: `complete: true`, collector exit code 0, process
  exited; completion audit found 0 missing reports, missing artifacts,
  identity mismatches, or sampled-value mismatches

## Duplicate finding and resolution

The retained quarterly extract has 30,336 rows and 30,324 original report
identities. The 12 extra rows form 12 repeated identities when keyed only by
`(gvkey, datadate, indfmt, consol, popsrc, datafmt)`. They are not exact
duplicates: each pair has different fiscal labels, and some accounting values
also differ. For example, GVKEY `017001` at `2025-12-31` has both FY2025 Q4
(`fyr=12`) and FY2026 Q1 (`fyr=9`).

All 24 rows were compared against their selected archived report snapshot and
the four statement files. Fiscal labels and values matched; there were no
archive mismatches. The archive stores both rows in the same versioned bundle,
so this was an incomplete query key rather than an overwrite. The derived
quarterly key now includes `(fyearq, fqtr, fyr)`. Exact full-row duplicates are
collapsed deterministically in memory if present; source extracts are never
rewritten. Conflicting rows with the same fiscal identity are quarantined and
mark a build `partial`. For this run, exact duplicates removed: 0;
quarantined identities: 0; build status: `complete`.

## Source and output counts

| Dataset | Source rows | Source report identities | Query rows | Year partitions |
| --- | ---: | ---: | ---: | --- |
| Annual | 6,261 | 6,261 | 6,306 | 2024, 2025, 2026 |
| Quarterly | 30,336 | 30,324 | 30,542 | 2024, 2025, 2026 |

Every output row has a unique declared logical key. The output count can exceed
the source-row count because a GVKEY can validly map to multiple Barrids; repeated
IID/CUSIP relationships for the same Barrid/GVKEY are collapsed before joining
financial rows. One company has no valid Barrid mapping, leaving 12 report
identities out of the query tables. Mapping audit counts: 0 ambiguous Barrid
mappings, 0 ambiguous company mappings, 0 invalid effective intervals, 1
missing Barra mapping, and 1 company without a valid Barrid.

Additional formats remain separate from this collection series. The collection
reported 1,184 annual `FS/STD` rows and 5,753 annual `INDL/SUMM_STD` rows as
excluded; quarterly excluded-format count was zero. No excluded format was
merged into the current series.

## Duplicate-safe physical query schemas

Annual Parquets have 77 columns. They contain:

- Identity and period: `barrid`, `gvkey`, `datadate`, `fyear`, `fyr`,
  `currency`, `rdq`, `indfmt`, `consol`, `popsrc`, `datafmt`.
- Mapping and extraction provenance: `mapping_status`,
  `barra_mapping_status`, `mapping_date`, `mapping_snapshot_id`,
  `mapping_snapshot_path`, `mapping_provenance`,
  `mapping_relationships_json`, effective-date columns, `source_snapshot_id`,
  `source_report_path`, four statement paths,
  `source_field_definitions_json`, and `materialization_id`.
- Annual Compustat fields: `at`, `act`, `che`, `rect`, `invt`, `ppent`, `lct`,
  `dlc`, `dltt`, `lt`, `seq`, `sale`, `cogs`, `gp`, `xrd`, `xsga`, `oiadp`,
  `xint`, `txt`, `ni`, `epspx`, `epsfx`, `pi`, `dp`, `oancf`, `ivncf`,
  `fincf`, `capx`, `dpc`, `chech`, `cdvc`, `pdvc`, `dltis`, `dltr`, `dlcch`,
  `prstkc`, `prstkcc`, `prstkpc`, `csho`, `cshpri`, `cshfd`, `dvpsp_f`,
  `dvpsx_f`, `dvc`, `dvp`, `dvt`, `dvpd`.

Quarterly Parquets have 75 columns. They contain the same mapping/provenance
columns as the annual table. Their period fields are `barrid`, `gvkey`,
`datadate`, `fyearq`, `fqtr`, `fyr`, `currency`, `rdq`, `indfmt`, `consol`,
`popsrc`, and `datafmt`, plus `atq`, `actq`, `cheq`, `rectq`, `invtq`,
`ppentq`, `lctq`,
`dlcq`, `dlttq`, `ltq`, `seqq`, `saleq`, `cogsq`, `gpq`, `xrdq`, `xsgaq`,
`oiadpq`, `xintq`, `txtq`, `niq`, `epspxq`, `epsfxq`, `piq`, `dpq`, `oancfy`,
`ivncfy`, `fincfy`, `capxy`, `dpcy`, `chechy`, `cdvcy`, `pdvcy`, `dltisy`,
`dltry`, `dlcchy`, `prstkcy`, `prstkccy`, `prstkpcy`, `cshoq`, `cshprq`,
`cshfdq`, `dvpspq`, `dvpsxq`, and `dvpq`.

Logical keys are:

- Annual: `(barrid, gvkey, datadate, indfmt, consol, popsrc, datafmt, fyear,
  fyr, mapping_date, mapping_snapshot_id, source_snapshot_id,
  materialization_id)`.
- Quarterly: same base plus `(fyearq, fqtr, fyr)` in place of `(fyear, fyr)`.

Parquet does not enforce constraints; uniqueness was checked during build and
again against the saved output tables.

## Paths and archive layout

Development query tables:

```text
/home/gunnjake/groups/grp_quant/database/development/fundamentals_annual/fundamentals_annual_2024.parquet
/home/gunnjake/groups/grp_quant/database/development/fundamentals_annual/fundamentals_annual_2025.parquet
/home/gunnjake/groups/grp_quant/database/development/fundamentals_annual/fundamentals_annual_2026.parquet
/home/gunnjake/groups/grp_quant/database/development/fundamentals_quarterly/fundamentals_quarterly_2024.parquet
/home/gunnjake/groups/grp_quant/database/development/fundamentals_quarterly/fundamentals_quarterly_2025.parquet
/home/gunnjake/groups/grp_quant/database/development/fundamentals_quarterly/fundamentals_quarterly_2026.parquet
```

Build manifest:
`/home/gunnjake/groups/grp_quant/database/compustat_query_manifest.json`.
It records run IDs, exact mapping snapshot/date, version policy and selected
snapshot IDs, row counts, output paths/schemas, format and mapping exclusions,
duplicate resolution, and key checks.

Compustat archive layout:

```text
fundamentals/compustat/
  security_mappings/membership_<date>_<hash>.parquet
  source_extracts/run_<id>/{annual,quarterly}.parquet
  source_extracts/run_<id>/{run,coverage_report,completion_report}.json
  <calendar-year>/gvkey_<gvkey>/<frequency>_<datadate>_<indfmt>_<datafmt>/
    report.json
    latest_snapshot.json
    balance_sheet.parquet
    income_statement.parquet
    cash_flow.parquet
    capital_metrics.parquet
    snapshots/<source-snapshot-id>/{report.json,*.parquet}
```

Source extract checksums after materialization match the values recorded before
the build:

- Annual: `7aef91681bf0ca3a1ef0f93708b99dac9183918ced67114199b71284e0544acb`
- Quarterly: `beb593f5ed459a5773028f52eccf232050d10d2d8af854841d8e6eb1fb6e9ff6`

## Query examples and verified results

The examples use the same repository interface quants use. Reads are lazy until
`.collect()` is called.

```python
import polars as pl
from pipelines.utils.enums import DatabaseName
from pipelines.utils.tables import Database

db = Database(DatabaseName.DEVELOPMENT)
annual = db.fundamentals_annual_table.read()

air_gross_profit = (
    annual
    .filter(
        (pl.col("barrid") == "USAA191")
        & (pl.col("gvkey") == "001004")
        & pl.col("fyear").is_between(2023, 2025)
    )
    .select("barrid", "gvkey", "fyear", "fyr", "datadate", "currency", "gp")
    .sort("fyear")
    .collect()
)
```

| Barrid | GVKEY | Fiscal year | Fiscal year end month | Reporting date | Currency | `gp` (USD millions) |
| --- | --- | ---: | ---: | --- | --- | ---: |
| USAA191 (AIR) | 001004 | 2023 | 5 | 2024-05-31 | USD | 485.8 |
| USAA191 (AIR) | 001004 | 2024 | 5 | 2025-05-31 | USD | 583.1 |
| USAA191 (AIR) | 001004 | 2025 | 5 | 2026-05-31 | USD | 694.1 |

The FY2024 row is explicitly `fyear=2024`; it is not inferred from its 2025
calendar-year `datadate`.

```python
pilot_barrids = ["USA91R1", "USAA191", "USAA5A1", "USAACU1", "USAKWP1"]
fy2025 = (
    annual
    .filter(pl.col("barrid").is_in(pilot_barrids) & (pl.col("fyear") == 2025))
    .select("barrid", "gvkey", "fyear", "datadate", "currency", "sale", "oiadp")
    .sort("barrid")
    .collect()
)
```

| Company | Barrid | GVKEY | Reporting date | Currency | `sale` (USD millions) | `oiadp` (USD millions) |
| --- | --- | --- | --- | --- | ---: | ---: |
| AAL | USA91R1 | 001045 | 2025-12-31 | USD | 54,633.000 | 1,629.000 |
| AIR | USAA191 | 001004 | 2026-05-31 | USD | 3,308.000 | 298.800 |
| PRG | USAA5A1 | 001076 | 2025-12-31 | USD | 2,409.223 | 207.827 |
| CECO | USAACU1 | 001050 | 2025-12-31 | USD | 774.381 | 52.332 |
| PNW | USAKWP1 | 001075 | 2025-12-31 | USD | 5,339.939 | 1,067.630 |

`oiadp` is Compustat's **operating income after depreciation** field. It is not
being substituted with EBIT.

```python
quarterly = db.fundamentals_quarterly_table.read()
air_cash_flow = (
    quarterly
    .filter(
        (pl.col("barrid") == "USAA191")
        & (pl.col("gvkey") == "001004")
        & (pl.col("fyearq") == 2024)
        & (pl.col("fqtr") == 3)
    )
    .select("barrid", "gvkey", "fyearq", "fqtr", "fyr", "datadate", "currency", "oancfy", "source_snapshot_id")
    .collect()
)
```

Result: AIR, GVKEY `001004`, FY2024 Q3, `datadate=2025-02-28`, USD,
`oancfy=-15.3` million. `oancfy` is explicitly **fiscal year-to-date operating
cash flow** as reported by Compustat. It is not a standalone-quarter value.
The selected report metadata defines `oancfy` as `fiscal year-to-date duration`.

The nine rows above were compared with their retained annual/quarterly source
extract rows and selected archived statement files. Values matched, and each
saved `report.json` snapshot ID matched the output row's `source_snapshot_id`.
All 24 source rows involved in the 12 quarterly shared-date identities also
matched their archived statement rows.

## Field limitations

- Annual `dvpd` remains null for all 6,261 source rows; the output field is null
  for all 6,306 mapped rows. No dividend value is filled or substituted.
- `gpq` is unavailable in the selected `comp.fundq` schema; it remains an
  all-null output column for schema stability.
- Quarterly cash-flow values ending in `y`, including `oancfy`, are fiscal
  year-to-date. No standalone quarters, gross profit, EBIT, or free cash flow
  were derived.
- The saved series is `INDL/C/D/STD`. The excluded `FS/STD` and
  `INDL/SUMM_STD` records may merit separately designed treatment; they were
  intentionally not merged here.
- This uses the 2026-08-31 Russell membership/mapping snapshot and current
  Compustat extracts. It is not historical index membership or a historical
  point-in-time vendor vintage. `datadate` is the reporting date; `rdq` is
  Compustat's report date, not an SEC filing date.
- The security mapping is effective-dated and retained, but applying this
  current membership snapshot to prior fiscal periods does not establish the
  historical Barrid mapping or information availability for backtests.

## Checks run and remaining work

- 17 focused offline unittests passed across `test_fundamentals_query`,
  `test_compustat_finalizer`, and `test_compustat_fundamentals`.
- Query output key uniqueness passed: 6,306/6,306 annual and 30,542/30,542
  quarterly rows unique by their declared keys.
- Representative query rows matched retained extracts and archived reports;
  quarterly shared-date duplicates had 0 archive mismatches.
- Source extract SHA256 checksums matched before and after the build.
- `git diff --check` passed. No collection, SEC, or IBKR calls were made; no
  commit or push was made.

Implemented: current-format full-universe archive, completion audit,
Barrid-linked annual/quarterly development query tables, explicit mapping and
version provenance, duplicate-safe fiscal-period keys, and offline regressions.

Pending: decide separate handling for `FS` and `SUMM_STD`; design historical
membership snapshots and historical-vintage availability before any historical
backfill; investigate source-unavailable fields such as `dvpd` and `gpq` only
if the source schema or collection format changes.

## Three screenshot suggestions

1. Run the AIR annual gross-profit Polars query and show fiscal years 2023–2025
   with `datadate`, `fyear`, and `gp` together.
2. Run the FY2025 five-company query and show Barrid, GVKEY, `sale`, and
   `oiadp` side by side.
3. Run the AIR FY2024 Q3 cash-flow query and include `fyearq`, `fqtr`,
   `datadate`, `oancfy`, `source_snapshot_id`, plus a caption that `oancfy` is
   fiscal YTD.
