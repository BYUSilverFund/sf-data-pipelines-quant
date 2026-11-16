# SEC fallback investigation handoff

## Findings

The earlier SEC attempt did not save Company Facts or submissions responses,
filing folders, or fact Parquets. At the start of this investigation, the only
file under the documented SEC archive was the non-overwriting security mapping
snapshot for the 2026-08-31 membership date. The SEC collection therefore had
no prior fact values that could be traced back to Company Facts. Its saved mapping resolves AIR to CIK
`0000001750` / GVKEY `001004` and AAT to CIK `0001500217` / GVKEY `185549`.
The snapshot has 2,970 distinct Russell CUSIPs: 2,965 have one current CIK
mapping and 5 have no CIK; none of the securities has multiple CIK or GVKEY
candidates in that snapshot. The 3,027 relationship rows represent the
many-to-many security/issuer mappings. CIK metadata is explicitly current,
not historical point-in-time identity.

The SEC collector preserves facts by accession, concept, unit, start/end,
filing date, and form, but its broad archive is not a bounded quant fallback.
It retains both duration observations without labelling a duration as
standalone-quarter or fiscal-YTD. Consumers could therefore mistake a Q3
nine-month cash-flow observation for a standalone quarter. It also stores the
filing link on the report metadata rather than every fact row. No saved SEC
fact rows exist on which to establish that this caused a prior bad result.

## Concrete comparison and selected pilot

The completed Compustat source extract has 30,336 quarterly rows. `gpq` is null
on all 30,336 because the selected Compustat schema lacks it. `oancfy` is null
on 884 rows. These nulls are candidates only after checking each issuer and
period.

The initial pilot is limited to one exact case:

- Company: American Assets Trust (`AAT`), CIK `0001500217`, GVKEY `185549`,
  Barrid `USAC121`.
- Compustat row: `datadate=2026-09-30`, `fyearq=2026`, `fqtr=3`, `fyr=12`,
  `oancfy=null`.
- SEC source: Form 10-Q, accession `0001500217-26-000052`, filed 2026-10-30,
  accepted `2026-10-30T19:25:22.000Z`, report period 2026-09-30.
- Supported metric: `us-gaap:NetCashProvidedByUsedInOperatingActivities`,
  unit `USD`, 2026-01-01 through 2026-09-30. Company Facts returns
  `129807000`; the filing presents `$129,807 thousand` for nine months. This
  is fiscal YTD, not standalone Q3. The current accession's Company Facts
  response contains current and prior-year YTD values; it has no separate
  July 1–September 30 observation for this cash-flow concept.
- Duplicate/amendment check: the response has exactly two rows for this
  tag/unit/accession, with distinct period starts and ends (current and prior
  year); no exact duplicate period fact or later amendment for this filing
  period appears in submissions.
- Comparable check: the same filing reports `$126,545 thousand` for the prior
  year's nine months. Compustat's `oancfy` for AAT FY2025 Q3 is `126.545` USD
  millions, an exact match after dividing the filing's USD value by 1,000,000.
- Availability: the SEC filing date is one day after the collection window's
  2026-10-29 end date. `datadate=2026-09-30` does not make the value available
  on September 30 or by October 29.

The source filing is the [SEC 10-Q](https://www.sec.gov/Archives/edgar/data/1500217/000150021726000052/aat-20260930.htm).
The [SEC filing index](https://www.sec.gov/Archives/edgar/data/1500217/000150021726000052/0001500217-26-000052-index.htm)
records the form, filing date, acceptance timestamp, report period, accession,
and both reporting entities. The cash-flow statement labels the amount as nine
months ended September 30 and reports 129,807 and 126,545 in thousands.

## A same-name concept that must not be substituted

AAR's Compustat annual `gp` and SEC `GrossProfit` are not equivalent in the
observed periods:

| SEC fiscal year ended | Compustat `datadate` | Compustat `fyear` | `sale` | `cogs` | `gp` | SEC statement `GrossProfit` |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| 2025 | 2025-05-31 | 2024 | 2,780.5 | 2,183.9 | 583.1 | 527.7 |
| 2026 | 2026-05-31 | 2025 | 3,308.0 | 2,613.9 | 694.1 | 622.0 |

Compustat `gp` equals `sale - cogs` in these rows; the SEC statement's gross
profit is sales less the reported costs of products and services. Those cost
definitions differ by 55.4 and 72.1, respectively. The SEC filing reports the
consolidated statement values directly, while its separate MD&A gross-profit
table reports segment amounts. The same concept name is insufficient evidence
for substitution. The [FY2025 filing](https://www.sec.gov/Archives/edgar/data/1750/000141057825001475/air-20250531x10k.htm)
and [FY2026 filing](https://www.sec.gov/Archives/edgar/data/1750/000110465926085459/air-20260531x10k.htm)
show these values. `GrossProfit` is excluded from this pilot.

The AAR FY2025 filing also says no dividends were declared in fiscal 2023,
2024, or 2025, while Compustat `dvpd` is null in all sampled annual rows.
That null does not establish that dividend data is missing; the filing gives a
specific reason the declared-dividend measure may have no observation.

## Bounded gap cohort

The cohort has three distinct outcomes; each keeps SEC evidence in its own
archive and does not update a Compustat extract or query table:

| Company / Compustat row | SEC evidence | Classification |
| --- | --- | --- |
| AAT, GVKEY `185549`, 2026-09-30, `oancfy=null` | 10-Q accession `0001500217-26-000052`, filed 2026-10-30; Company Facts has the exact FY2026 YTD fact | **Late filing.** The filing became public one day after the 2026-10-29 collection cutoff. The null is not missing SEC source coverage. |
| Mechanics Bancorp, GVKEY `018948`, 2024-09-30, `oancfy=null` | Original 10-Q accession `0001518715-24-000181`: `-47042000 USD`; later 10-Q comparative accession `0001518715-25-000181`: `217358000 USD` for the same YTD duration | **Unresolved.** Both filing pages corroborate their respective facts. The later comparative is not a 10-Q/A; the accounting-predecessor evidence and exclusion decision are documented in the November 10 follow-up below. |
| Globant S.A., GVKEY `018626`, 2024-09-30, `oancfy=null` | SEC submissions have no 10-Q for the period; Company Facts has `ifrs-full`, not `us-gaap` | **Unsupported standard coverage.** A custom IFRS mapping and 6-K filing review would be needed. Do not use a similarly named concept as an automatic replacement. |

These checks show why nulls need distinct causes: filing timing, Compustat
source coverage, and taxonomy/form coverage are not interchangeable. The
cohort is a targeted diagnostic, not a statement that every `oancfy` null is
fillable.

## Implementation

`pipelines/sec_fallback.py` and `scripts/extract_sec_fallback.py` implement an
offline extractor for only AAT FY2026 Q3 operating cash flow YTD. It requires
the exact CIK, accession, form, report date, filing date, concept, unit,
fiscal labels, and duration start/end. It rejects an absent or duplicate exact
fact, keeps the raw USD amount, and stores CIK, GVKEY, Barrid, membership and
mapping snapshot provenance, concept, unit, period semantics, fiscal labels,
accession, form, filing and acceptance timestamps, source URL, Company Facts
and submissions SHA256 values, extraction time, and extraction version. It
writes only to the explicitly provided Parquet path; it does not access
WRDS/SEC, modify Compustat files, or populate quant-facing tables.

The bounded live fetch succeeded with `curl` and the configured SEC user agent.
The exact responses used for verification were saved at
`/tmp/sec-fallback-validation/live/companyfacts.json` and
`/tmp/sec-fallback-validation/live/submissions.json`. Their SHA-256 hashes are
`5773d485144bf7fd8ba71c44c48b6f6cec13152febad658b4a49a170750851eb` and
`190e85ddaec5a1134b953989607810d6e18e8ba4014e14a3a7eb0766fbc6b5b2`,
respectively. The workspace test fixture is a narrow two-observation slice
from that actual Company Facts response.

The bounded live fetch used these commands (the user-agent value remains in
`.env` and is not printed):

```bash
set -a
source .env
set +a
mkdir -p /tmp/sec-fallback-validation/live
curl --silent --show-error --max-time 30 \
    --user-agent "$SEC_USER_AGENT" \
    --output /tmp/sec-fallback-validation/live/submissions.json \
    --write-out 'submissions_http=%{http_code} bytes=%{size_download}\n' \
    https://data.sec.gov/submissions/CIK0001500217.json
sleep 0.2
curl --silent --show-error --max-time 60 \
    --user-agent "$SEC_USER_AGENT" \
    --output /tmp/sec-fallback-validation/live/companyfacts.json \
    --write-out 'companyfacts_http=%{http_code} bytes=%{size_download}\n' \
    https://data.sec.gov/api/xbrl/companyfacts/CIK0001500217.json
```

Result: submissions HTTP 200, 143,401 bytes; Company Facts HTTP 200,
3,568,249 bytes. The actual-response extraction command was:

```bash
.venv-fips/bin/python -m scripts.extract_sec_fallback \
    --companyfacts /tmp/sec-fallback-validation/live/companyfacts.json \
    --submissions /tmp/sec-fallback-validation/live/submissions.json \
    --output /tmp/sec-fallback-validation/live/output/aat_q3_2026.parquet \
    --extracted-at 2026-11-04T12:00:00+00:00
```

It wrote one row with raw `value=129807000`, `unit=USD`, YTD start
`2026-01-01`, end `2026-09-30`, accession `0001500217-26-000052`, form
`10-Q`, filing date `2026-10-30`, and acceptance timestamp
`2026-10-30T19:25:22.000Z`.

Run the extractor with saved SEC Company Facts and submissions JSON:

```bash
.venv/bin/python -m scripts.extract_sec_fallback \
    --companyfacts /path/to/saved/companyfacts-0001500217.json \
    --submissions /path/to/saved/submissions-0001500217.json \
    --output /path/to/separate/sec_fallback/aat_q3_2026.parquet \
    --extracted-at 2026-11-04T12:00:00+00:00
```

The filed SEC statement's USD thousands are not normalized in the SEC dataset.
For comparison only, `129807000 USD / 1,000,000 = 129.807 USD millions`.
Any later quant integration must retain the raw SEC record and use filing date
as the availability date; it must not overwrite or fill the Compustat table.

## Verification and next step

Offline regression tests use the saved two-observation slice from the actual
Company Facts response. They assert the exact period, accession, filing
metadata, and source hashes; a separate synthetic test adds a standalone
quarter and an amended accession and confirms neither is selected. Duplicate
target facts fail closed. No amendment for this report period appears in the
current submissions response; the amended-row test exercises separation by
accession.

The original uv-managed Python 3.13.15 was built with bundled OpenSSL 3.5.8.
On this host, kernel FIPS mode is enabled and the system crypto policy loads a
Red Hat OpenSSL configuration option that this bundled build cannot parse.
Python therefore failed creating an SSL context with
`[SSL] error in system default config` before any HTTP request. `curl` worked.
Setting `OPENSSL_CONF=/dev/null` was only a diagnostic; it bypasses host crypto
policy and is not an acceptable runtime fix.

The repaired Python 3.13.18 is installed at the durable, user-owned path
`/home/gunnjake/.local/opt/python-3.13.18-fips`. It links to host OpenSSL
3.2.2; `/proc/sys/crypto/fips_enabled` remains `1`, the default SSL context
requires certificates and checks hostnames, and `OPENSSL_CONF` is unset. Both
`.venv` and `.venv-fips` were recreated against this interpreter. The lockfile
and project dependencies were not changed for TLS.

To recreate the runtime on this host if the durable install is lost, run
`scripts/build_fips_python_runtime.sh`. It downloads Python 3.13.18 from the
official source URL, verifies SHA-256
`abb0102e6d4ae027509bcb82327d9dd85515b44a3ba8ad538cfc5e9847029f68`, builds
against `/usr` OpenSSL, installs to the user-owned path, then syncs both
environments. The script refuses to overwrite an existing runtime. It does
not change FIPS settings or disable certificate verification.

To recreate the project environments against an already installed runtime:

```bash
PYTHON_PREFIX=/home/gunnjake/.local/opt/python-3.13.18-fips
UV_PROJECT_ENVIRONMENT=.venv uv sync --python "$PYTHON_PREFIX/bin/python3.13"
UV_PROJECT_ENVIRONMENT=.venv-fips uv sync --python "$PYTHON_PREFIX/bin/python3.13"
```

The SEC request smoke check uses the configured request identification and
normal Requests certificate verification:

```bash
set -a
source .env
set +a
.venv/bin/python -c 'import os,requests; r=requests.get("https://data.sec.gov/submissions/CIK0001500217.json", headers={"User-Agent":os.environ["SEC_USER_AGENT"]}, timeout=30); print(r.status_code, len(r.content))'
```

The Compustat import check is
`.venv/bin/python -c 'import runpy; runpy.run_path("tests/compustat_query_example.py"); import pipelines.compustat_fundamentals, pipelines.fundamentals_query, pipelines.fundamentals_flow'`.

The durable-runtime verification imported
`tests/compustat_query_example.py` and the fundamentals query/flow modules,
then requested SEC submissions with the configured `.env` user agent. Results:
OpenSSL `3.2.2 4 Jun 2024`, FIPS kernel flag `1`, certificate verification
`True`, SEC HTTP `200`, `143401` bytes.

The SEC responses, pilot parquet, and cohort evidence are preserved under the
existing archive:

- AAT API responses: `api_responses/CIK0001500217/{companyfacts,submissions}/`
  with SHA-256 `5773d485144bf7fd8ba71c44c48b6f6cec13152febad658b4a49a170750851eb`
  and `190e85ddaec5a1134b953989607810d6e18e8ba4014e14a3a7eb0766fbc6b5b2`.
- AAT pilot Parquet:
  `fallback_pilots/aat_q3_2026/aat_q3_2026_fd40217d0ed6ac595faafaaba7efb07ff9dfabf2527f54e42cafe2678695a3c4.parquet`,
  SHA-256 `fd40217d0ed6ac595faafaaba7efb07ff9dfabf2527f54e42cafe2678695a3c4`.
  The first preserved extraction remains beside it with its earlier timestamp.
- Cohort evidence and manifest: `fallback_pilots/cohort_2026-11/`, including
  `cohort_validation.json`, Mechanics Company Facts/submissions snapshots, and
  the SEC 10-Q HTML source. The manifest records each response/document hash.

The saved-response extractor was rerun with
`.venv-fips/bin/python -m scripts.extract_sec_fallback` at
`2026-11-04T23:32:33+00:00` and wrote one observation (`129807000 USD`, YTD
2026-01-01 through 2026-09-30) to the archived Parquet. The Mechanics 10-Q
inline fact and Compustat null were checked against the saved source extract;
no SEC value was inserted into it.

## Expanded bounded OCF-YTD cohort (2026-11-05)

`scripts/build_sec_ocf_ytd_cohort.py` audits ten exact Compustat rows with null
`oancfy`, joins each through the saved 2026-08-31 mapping snapshot, and reads
only content-addressed SEC responses and filing pages. It does not make
network requests or write Compustat data. Repeated Company Facts observations
are retained by accession, including later comparatives and amendments.
Outputs are content-addressed, so rerunning with the same archived responses
produces the same paths and does not overwrite earlier evidence.

The cutoff for the completed Compustat collection is **2026-10-29**. No
historical investment cutoff was supplied; the audit records that as unknown
and makes no historical point-in-time claim. SEC filing and acceptance dates
are preserved per accession. `datadate`, `rdq`, and the current Compustat
extract are not treated as proof of SEC availability.

| Company | GVKEY / period | SEC evidence | Classification |
| --- | --- | --- | --- |
| Mechanics Bancorp | `018948`, 2024-09-30 | Original 10-Q filed 2024-11-08: `-47042000 USD`; 2025-11-17 comparative 10-Q: `217358000 USD` for the same 2024 YTD duration | **Unresolved** — both values are corroborated, but the follow-up below shows the filings use different accounting predecessors. |
| American Assets Trust (AAT) | `185549`, 2026-09-30 | 10-Q filed 2026-10-30; `129807000 USD`, 2026-01-01–2026-09-30 | **Filing after the relevant cutoff** — one day after collection cutoff. |
| Globant S.A. | `018626`, 2024-09-30 | Company Facts has `ifrs-full`, no `us-gaap`; no matching 10-Q | **Incompatible definition, period, or taxonomy**. |
| Unity Bancorp | `062243`, 2026-09-30 | No matching 10-Q/10-Q/A in the submissions snapshot retrieved 2026-11-05 | **No matching disclosure** as of that snapshot. |
| Citigroup | `003243`, 2026-09-30 | No matching 10-Q/10-Q/A in the submissions snapshot retrieved 2026-11-05 | **No matching disclosure** as of that snapshot. |
| Goldman Sachs | `114628`, 2026-09-30 | 10-Q filed 2026-11-02; `-21582000000 USD` YTD | **Filing after the relevant cutoff**. |
| Wells Fargo | `008007`, 2026-09-30 | 10-Q accession `0000072971-26-000327`, filed 2026-10-28 and accepted `2026-10-28T12:59:31.000Z`; `24785000000 USD` YTD | **Verified SEC fallback candidate**. Inline XBRL reports `24,785`, `scale=6`, USD, exact context 2026-01-01–2026-09-30. |
| JPMorgan Chase | `002968`, 2026-09-30 | 10-Q filed 2026-11-03; `-203883000000 USD` YTD | **Filing after the relevant cutoff**. |
| FB Financial | `027871`, 2026-09-30 | 10-Q filed 2026-11-02; `126184000 USD` YTD | **Filing after the relevant cutoff**. |
| Home BancShares | `164633`, 2026-09-30 | No matching 10-Q/10-Q/A in the submissions snapshot retrieved 2026-11-05 | **No matching disclosure** as of that snapshot. |

Totals: one verified candidate, four filings after the collection cutoff, three
with no matching disclosure in the saved submissions snapshot, one taxonomy
incompatibility, and one unresolved conflicting-period case. The three “no
matching disclosure” outcomes mean no matching filing was present in the
retrieved submission responses; they do not establish permanent absence of a
future filing.

For Wells Fargo, CIK `0000072971` maps uniquely to GVKEY `008007` / Barrid
`USAK1N1` in the saved snapshot. Company Facts and the filing identify Wells
Fargo, the exact original 10-Q accession contains one USD OCF YTD value, and
the filing page's inline sign/scale/value equals Company Facts. The SEC value
is separate; it has not been inserted into a quant table. Its filing date is
after the period end, so it was not available on `datadate=2026-09-30`; it was
available by the collection cutoff. A historical investment cutoff still
needs to be specified before any backtest use.

For Mechanics, the original 2024 filing's inline fact is `sign="-"`,
`scale="3"`, displayed `47,042`, or `-47042000 USD`. The 2025 comparative
filing's same-period fact is positive `217,358` with `scale="3"`, or
`217358000 USD`. The latter is a later comparative disclosure, not an
identified 10-Q/A. The archived evidence does not establish why the value
changed, so both are retained and the case remains unresolved.

The separate archive contains all ten Companies Facts/submissions responses,
all available target filing pages plus the Mechanics comparative page, the
decision manifest, and accession-level extracted observations:

- `fallback_pilots/ocf_ytd_cohort_2026-11/cohort_decisions_17225816d80fbbd8bfd274505fd24384b8625ce51799a9f527d7441e7d250785.json`
- `fallback_pilots/ocf_ytd_cohort_2026-11/ocf_ytd_observations_94333c1a70259b2dc9ddaa135e773fe283dd35e9e8485092f9bc55098d3421a5.parquet`
- `fallback_pilots/ocf_ytd_cohort_2026-11/source_documents/`

The earlier Mechanics “source gap candidate” note in the preliminary
`cohort_2026-11/cohort_validation.json` is superseded by the accession-level
comparison in this expanded cohort. At the end of this investigation, the
Wells Fargo value was still only a candidate for separate downstream review;
the bounded resolved development view and its policy are recorded below.

## Separate resolved quarterly development view (2026-11-10)

The Wells Fargo candidate was verified from the saved artifacts and is now
available in a separate, immutable, policy/input-scoped development table.
The original Compustat tables and SEC archive were not modified. The resolved
table retains the source `oancfy`, adds `original_oancfy` and
`resolved_oancfy`, and preserves the original rows and all columns. Only the
allowlisted Wells Fargo company-period was supplemented; its current mapping
has one Barra row (`USAK1N1`).

### Baseline and runtime

- `.venv-fips/bin/python examples/compustat_quickstart.py` succeeded. It
  reported the 2026-11-09 assets snapshot and passed the 1:1 Barrid asset join
  (23,753 asset rows, 2,966 profitability rows, 2,943 matched rows).
- Development annual table: 6,306 rows, 6,306 unique quickstart fiscal keys.
- Development quarterly table: 30,542 rows, 30,542 unique quickstart fiscal
  keys; 889 original `oancfy` nulls.
- The durable runtime is Python 3.13.18 at
  `/home/gunnjake/.local/opt/python-3.13.18-fips`, OpenSSL 3.2.2, kernel FIPS
  enabled, `OPENSSL_CONF` unset, and the default TLS context has certificate
  verification and hostname checking enabled. The quickstart and resolver ran
  on this runtime. Pytest is not installed in `.venv-fips`; the offline suite
  ran using `.venv`.
- Focused offline tests: 30 passed, 3 subtests passed. `git diff --check`
  passed. Tests cover late filings, amendment facts and amendments without the
  fact, conflicting accessions, units, exact YTD context, timezone-aware
  cutoff boundaries, original non-null retention, excluded Mechanics/Globant
  rows, policy-scoped reruns, and scope mismatch rejection.

### Wells Fargo decision and policy

- Compustat row: GVKEY `008007`, Barrid `USAK1N1`, `datadate=2026-09-30`,
  `fyearq=2026`, `fqtr=3`, `fyr=12`, currency `USD`, original `oancfy=null`.
- SEC Company Facts: `us-gaap:NetCashProvidedByUsedInOperatingActivities`,
  USD, exact duration 2026-01-01 through 2026-09-30, `fy=2026`, `fp=Q3`,
  accession `0000072971-26-000327`.
- Saved filing HTML independently reports `24,785` with `scale=6` and USD.
  This verifies `24,785 × 10^6 = 24,785,000,000 USD`; Compustat stores millions
  of the reported currency, so the resolved value is `24,785.0` USD millions.
- Filing acceptance was `2026-10-28T12:59:31.000Z`. Build cutoff was explicitly
  supplied as `2026-10-29T23:59:59+00:00`. SEC archive retrieval timestamps
  are from 2026-11-05 and do not establish an original collection timestamp;
  the build therefore labels its cutoff as a **comparison-policy cutoff**.
  This current-vintage row is not historical point-in-time data.
- Selection requires the exact CIK/GVKEY/Barrid, period start/end, fiscal
  labels, `us-gaap` concept, USD currency, inline filing corroboration, and
  acceptance no later than the timezone-aware cutoff. A 10-Q/A can supersede
  the original only when it contains the exact fact and matching inline
  evidence; later comparative accessions are treated as conflicts. No matching
  Wells Fargo amendment exists in the saved evidence.
- Cohort result: 1 accepted fallback candidate, 9 other candidates excluded;
  888 quarterly rows remain unresolved for `oancfy`, including Mechanics and
  Globant. The allowlist remains limited to the Wells Fargo company-period.

### Output and use

Policy/input scope ID:
`3fd44722302d59ad883f9d5017897c5909d280a43c0a02dcefad1868790cf6de`.

Resolved output directory:
`/home/gunnjake/groups/grp_quant/database/development/fundamentals_quarterly_resolved_3fd44722302d59ad/`

It contains yearly files `fundamentals_quarterly_resolved_3fd44722302d59ad_2024.parquet`,
`..._2025.parquet`, and `..._2026.parquet` with 11,849, 11,885, and 6,808
rows, plus `manifest.json`. The manifest includes the scope, policy/cutoff,
source checksums, and output checksums. Re-running the builder verified the
existing manifest and Parquet hashes without rewriting them. Different input
or policy scope gets a different table name; a mismatched existing scope is
rejected.

```python
import polars as pl
from pipelines.utils.enums import DatabaseName
from pipelines.utils.tables import Database

scope = "3fd44722302d59ad883f9d5017897c5909d280a43c0a02dcefad1868790cf6de"
resolved = Database(DatabaseName.DEVELOPMENT).fundamentals_quarterly_resolved_table(scope)
wells_fargo = (
    resolved.read(2026)
    .filter((pl.col("gvkey") == "008007") & (pl.col("datadate") == pl.date(2026, 9, 30)))
    .select("barrid", "fyearq", "fqtr", "fyr", "currency", "oancfy",
            "original_oancfy", "resolved_oancfy", "selected_source",
            "sec_value_raw", "sec_unit", "sec_accession", "policy_cutoff")
    .collect()
)
```

### Checksums and changed files

Original quarterly development Parquet SHA-256 values were checked before and
after materialization:

- `fundamentals_quarterly_2024.parquet`:
  `196934be8e9fa6878f5c8d82e5e8d88ca7b5e33fd88de01760867aa40f140356`
- `fundamentals_quarterly_2025.parquet`:
  `e4f879de33b84b4d6b6b87cee3ddc19180316795a995b89fb6333bf83cd0fed7`
- `fundamentals_quarterly_2026.parquet`:
  `377bcbdcae8792732204085221731859fa6f9688dd1504efc1b70fce038f482a`
- Compustat source `quarterly.parquet`:
  `beb593f5ed459a5773028f52eccf232050d10d2d8af854841d8e6eb1fb6e9ff6`
- Wells Fargo Company Facts:
  `58033a85c9795b5d834c9a4fb60dfb7f8e600c89304d757af7c40eb4bff69990`
- Wells Fargo submissions:
  `c65c0466177cd606e54024bf9389e39bc3186cdc07771fdc2d573ff264ba05b0`
- Wells Fargo filing HTML:
  `328c8db358333ec790017006b774204360800213cc244d3b03b3f1b3734303aa`

Changed source files for this step: `pipelines/resolved_fundamentals.py`,
`scripts/build_resolved_fundamentals.py`,
`pipelines/utils/tables.py`, and `tests/test_resolved_fundamentals.py`.
Pre-existing README, dependency, quickstart, lockfile, and untracked example
changes were preserved.

### Next task and limitations

At the end of the Wells Fargo-only step, Mechanics comparison and a historical
investment cutoff remained open. The November 10 follow-up below completes the
Mechanics exclusion review. A historical investment cutoff is still required
before any point-in-time use. Do not describe these current Compustat or
SEC-derived values as historical point-in-time vintages.

## Mechanics SEC discrepancy follow-up (2026-11-10)

The saved evidence is sufficient; no SEC requests or new data collection were
needed. The discrepancy is now documented and **excluded from fallback**. Both
observations are real, exact-duration, inline-XBRL corroborated values. The
later value is not a verified correction of the HomeStreet observation because
the 2025 comparative uses a different accounting predecessor after a reverse
acquisition. The numerical difference is not reconciled between the filings,
so the Compustat row remains unresolved.

### Accession-level comparison

| Evidence | Original filing | Later comparative |
| --- | --- | --- |
| Accession / form | `0001518715-24-000181`, 10-Q | `0001518715-25-000181`, 10-Q |
| Filing report date | 2024-09-30 | 2025-09-30 |
| Filed / accepted | 2024-11-08 / `2024-11-08T00:51:30.000Z` | 2025-11-17 / `2025-11-17T21:03:49.000Z` |
| Company Facts fiscal labels | `fy=2024`, `fp=Q3` | `fy=2025`, `fp=Q3` |
| Exact fact context | 2024-01-01–2024-09-30, context `c-1` | 2024-01-01–2024-09-30, context `c-12` |
| Concept / unit | `us-gaap:NetCashProvidedByUsedInOperatingActivities`, USD | same concept and unit |
| Filed fact | `47,042`, `sign="-"`, `scale=3` = `-47,042,000 USD` | `217,358`, no negative sign, `scale=3` = `217,358,000 USD` |
| Filing statement | HomeStreet, Inc. and subsidiaries, consolidated cash flows | Mechanics Bancorp and subsidiaries, consolidated cash flows |
| XBRL context | CIK `0001518715`; no dimensions | CIK `0001518715`; no dimensions |

The period conclusion comes from the exact XBRL start/end dates and the
comparative cash-flow statement columns, not from `fy`/`fp`. Both accessions
were accepted before the supplied `2026-10-29T23:59:59+00:00` comparison
cutoff. Units and scale agree; the original filing's negative sign is correctly
preserved. The source evidence is not an extraction error, unit mismatch, or
period mismatch. Normalized to Compustat USD millions for comparison only,
the figures are `-47.042` and `217.358`.

The 2025 filing identifies the transaction as a reverse acquisition, with
Mechanics Bank as accounting acquirer and HomeStreet Bank as accounting
acquiree. It states that pre-2025-09-02 financial results are legacy Mechanics
Bank standalone results. The 2024 filing presents HomeStreet, Inc. results.
HomeStreet, Inc. later became the legal acquirer and changed its name to
Mechanics Bancorp, which explains why the CIK is shared; a shared CIK does not
make the two pre-merger accounting entities equivalent. The later 2024
comparative is applicable to the stated duration and concept, but not to the
same accounting predecessor as the original HomeStreet period. The filing
does not provide a bridge for the `264,400,000 USD` difference. Treat the
later observation as a different-predecessor comparative, not a revision of
the HomeStreet fact.

The Compustat row remains `GVKEY=018948`, `Barrid=USAHMH1`,
`datadate=2024-09-30`, `fyearq=2024`, `fqtr=3`, `fyr=12`, `currency=USD`,
`oancfy=null`. The saved mapping uses current CIK metadata and is not proof of
historical issuer identity. Neither SEC value is inserted into the resolved
value.

### New resolved scope

The earlier Wells Fargo-only scope remains untouched. A second scope records
the Mechanics evidence and exclusion reason while retaining the same fallback
decision: Wells Fargo is the sole SEC-sourced row, Mechanics and Globant remain
unresolved.

- Scope ID:
  `82402b10b01ce62c7b2f6ecb50d94a39d4d9a2cefc818165d524472380216248`
- Output directory:
  `/home/gunnjake/groups/grp_quant/database/development/fundamentals_quarterly_resolved_82402b10b01ce62c/`
- Yearly row counts: 2024 `11,849`; 2025 `11,885`; 2026 `6,808`.
- Total rows / unique keys: `30,542 / 30,542`.
- SEC-selected rows: `1` (Wells Fargo `USAK1N1`, `24,785.0` USD millions).
- Mechanics-supplemented rows: `0`; Mechanics `original_oancfy` and
  `resolved_oancfy` are both null. Its row contains `candidate_evidence_json`
  with both accessions, values, contexts, filing/acceptance dates, and source
  hashes, plus the exclusion reason.
- Globant remains null and carries no SEC candidate evidence.
- Total unresolved resolved-value nulls: `888`.

The new 2026 Q3 resolved rows had 649 distinct Barrids and no duplicate
Barrids. Joining to the saved 2026-11-09 asset snapshot matched 647 rows and
passed `validate="1:1"`. The existing quickstart also passed its annual
Barrid join with 23,753 asset rows, 2,966 profitability rows, and 2,943
matches. Every original quarterly column compared equal in the new resolved
table; all 30,542 keys and all original non-null Compustat `oancfy` values were
preserved. Wells Fargo remains `original_oancfy=null`,
`resolved_oancfy=24785.0`.

Database/Polars query for both Wells Fargo and the unresolved Mechanics case:

```python
import polars as pl
from pipelines.utils.enums import DatabaseName
from pipelines.utils.tables import Database

scope = "82402b10b01ce62c7b2f6ecb50d94a39d4d9a2cefc818165d524472380216248"
resolved = Database(DatabaseName.DEVELOPMENT).fundamentals_quarterly_resolved_table(scope)
rows = (
    resolved.read()
    .filter(
        ((pl.col("gvkey") == "008007") & (pl.col("datadate") == pl.date(2026, 9, 30)))
        | ((pl.col("gvkey") == "018948") & (pl.col("datadate") == pl.date(2024, 9, 30)))
    )
    .select("barrid", "gvkey", "datadate", "oancfy", "original_oancfy",
            "resolved_oancfy", "selected_source", "selection_reason",
            "candidate_evidence_json")
    .collect()
)
```

The output manifest contains all input and Parquet checksums. A repeated build
with the same cutoff verified the existing scope and did not rewrite it. The
previous output scope and all original source/archive files were checksum
verified unchanged.

### Commands and checks

```bash
.venv-fips/bin/python -m scripts.build_resolved_fundamentals \
  --cutoff 2026-10-29T23:59:59+00:00 --output-root /tmp/mechanics-resolved-validation
.venv-fips/bin/python -m scripts.build_resolved_fundamentals \
  --cutoff 2026-10-29T23:59:59+00:00
PYTHONPATH=/home/gunnjake/projects/sf-data-pipelines-quant/.venv/lib/python3.13/site-packages \
  .venv-fips/bin/python -m pytest -q \
  tests/test_compustat_fundamentals.py tests/test_compustat_finalizer.py \
  tests/test_compustat_quickstart.py tests/test_sec_fallback.py \
  tests/test_sec_ocf_cohort.py tests/test_resolved_fundamentals.py
.venv-fips/bin/python examples/compustat_quickstart.py
git diff --check
```

Results: offline tests **31 passed, 3 subtests passed** using the durable FIPS
Python interpreter (pytest was loaded from the existing `.venv` because it is
not installed in `.venv-fips`); quickstart passed; the scoped rerun returned
the same scope ID; `git diff --check` passed.

### Checksums

The following original inputs were hashed before the derived write and
rechecked afterward. These include the original annual and quarterly
development tables, Compustat source extract, Mechanics Company Facts and
submissions, both source filings, mapping snapshot, cohort decision, and
accession observation archive. Mechanics evidence hashes:

- Company Facts:
  `681b41adae2cd344466285cce39a649cea753f7a3ec8d39109a16310b6c1c102`
- Submissions:
  `fd890c29a472c512091d1f3fc4f7ba335f3ccc278e52d8eb67133abe40a16335`
- Original 2024 filing:
  `db6bd1062b1aaad01a2ef4916e1efa825306950836e4fca0875bf2a1d6f57901`
- 2025 comparative filing:
  `604455ece59b7241ca650be0de5d04d9f26895fa59eea527288af8e646ecbddc`

New output SHA-256:

- 2024 Parquet:
  `fa6fd8b79a3c6e30cee2ef3157c2bf3c7cec0e054fbefa80566b34de86204272`
- 2025 Parquet:
  `c46caa13b95e1146508d991c43a9f1dd7b2f0e01aeab1eca54b76985ea9fc767`
- 2026 Parquet:
  `cf876e4c0e636c5ca7fd523b35433169617ba0c7f91144ffb0e104d2dce9a799`
- Manifest:
  `fd478e589d3b9323da294aa045bd977c9fd4896e9e66d9a9dc9682b92e4d976b`

New code/evidence files: updated `pipelines/resolved_fundamentals.py`,
updated `scripts/build_resolved_fundamentals.py`,
`tests/fixtures/mechanics_2024q3_discrepancy_verified.json`, and the expanded
`tests/test_resolved_fundamentals.py`. The prior resolved scope, original
Compustat/SEC archives, and pre-existing unrelated workspace edits remain
preserved.

### Next task

Do not add Mechanics to the fallback allowlist unless primary evidence
establishes that its 2025 comparative restates HomeStreet's same-period OCF or
provides a valid predecessor mapping for the Compustat row. A future step could
compare other Compustat items and filing notes across the reverse-acquisition
boundary, but should remain outside this OCF allowlist until separately
reviewed. Globant remains excluded; no IFRS or other metrics were added.
Keep this analysis labeled current-vintage with the comparison cutoff, not
historical point-in-time data.

## Reusable OCF-YTD workflow and finalized scope (2026-11-11)

The earlier scoped outputs remain in place. A final immutable scope was built
after adding explicit inline XBRL identity and consolidated-statement checks
for the accepted Wells Fargo observation. The original Compustat materialized
files and archived Company Facts, submissions, mapping, and filing documents
were preserved.

### Reusable implementation

- `pipelines.sec_ocf_cohort.OCF_YTD_METRIC` explicitly defines
  `us-gaap:NetCashProvidedByUsedInOperatingActivities`, USD, fiscal-year-to-date
  semantics, eligible 10-Q/10-Q/A forms, and the amendment rule.
- `discover_ocf_ytd_gaps` finds null quarterly `oancfy` rows and marks missing
  issuer or consolidation evidence as `requiring_identity_review`. A matching
  CIK does not pass accounting-entity or consolidation-scope review by itself.
- `collect_ocf_ytd_observations` retains all exact-duration accessions.
  `extract_ocf_ytd_observation` extracts a configured candidate and requires an
  explicit `accept` decision plus identity and scope verification. The former
  AAT function remains as a compatibility wrapper around this reusable path.
- `resolve_quarterly_candidates` accepts reviewed candidate records only. It
  requires exact issuer/security/fiscal keys, YTD labels, unit and scale
  conversion, filing-page value corroboration, an explicit timezone-aware
  cutoff, and a completed conflict/amendment review. Existing non-null
  Compustat values are never overwritten. It retains filing and mapping paths,
  source hashes, accessions, filing and acceptance timestamps, extraction
  version, decision reason, and candidate evidence.
- The Wells Fargo archived-evidence verifier also checks that the target inline
  fact context identifier is CIK `0000072971`, the exact YTD period is
  2026-01-01 through 2026-09-30, the context has no dimensions, and the filing
  labels the statement “Wells Fargo & Company and Subsidiaries Consolidated
  Statement of Cash Flows.” The saved mapping and SEC entity-name checks remain
  required as well.
- The development build adapts this accepted evidence into the generic
  `resolve_quarterly_candidates` interface. Explicit sample decisions live in
  `SEC_OCF_CANDIDATE_DECISIONS`; the same policy records Mechanics as requiring
  identity review and Globant as incompatible.

### Bounded sample outcome

The validation reused the ten previously archived candidate periods. It made
no SEC requests and did not rerun Compustat. Final classifications are:

| Classification | Count | Cases |
| --- | ---: | --- |
| Accepted | 1 | Wells Fargo, `008007`, 2026-09-30 |
| Late | 4 | AAT, Goldman Sachs, JPMorgan Chase, FB Financial |
| Unavailable in the saved submissions snapshot | 3 | Unity Bancorp, Citigroup, Home BancShares |
| Incompatible | 1 | Globant; saved facts lack the supported US-GAAP coverage |
| Requiring identity review | 1 | Mechanics; reverse acquisition changed the accounting entity represented by comparative statements despite the same CIK |

The acceptance cutoff was explicitly supplied as
`2026-10-29T23:59:59+00:00`. It is a comparison-policy cutoff; archive
retrieval timestamps do not prove the original Compustat collection time.
Current-vintage SEC/Compustat data are not historical point-in-time data.

### Final development materialization

Actual build command:

```bash
.venv-fips/bin/python -m scripts.build_resolved_fundamentals \
    --cutoff 2026-10-29T23:59:59+00:00
```

Output directory:
`/home/gunnjake/groups/grp_quant/database/development/fundamentals_quarterly_resolved_d86e0dcada0c1811/`

Scope ID:
`d86e0dcada0c1811e2c950dfb21a3e8a4cdc8dfc0b2418d01bcdb184374c70b9`.
The output contains 30,542 rows and 30,542 unique quarterly keys; the manifest
records 1 accepted fallback and 9 excluded candidates. The three Parquet files
have 11,849 / 11,885 / 6,808 rows for 2024 / 2025 / 2026. There are 888
unresolved `oancfy` rows after the one accepted fill.

SHA-256 checksums:

- `fundamentals_quarterly_resolved_d86e0dcada0c1811_2024.parquet`:
  `b937a3e2397fc7ec50a8939f71c0076360a40fa4e26eac8ec571f7838899cc4f`
- `fundamentals_quarterly_resolved_d86e0dcada0c1811_2025.parquet`:
  `9473e1a9f67f4d76b583d6b1ecefe0e4580a71f16ef53771226a7517423b697b`
- `fundamentals_quarterly_resolved_d86e0dcada0c1811_2026.parquet`:
  `03f00cc5eff4ce85576d56553628af7b7c7b66130b34eb17687ff5b8c6c31227`
- `manifest.json`:
  `61515305db0d9abd9f1f5047be8e6897c4dbb492af04002a2ab08813df6c06ae`

The scope input hashes include the quarterly development files, Compustat
source extract, saved ten-company decision manifest and accession-level
observations, mapping snapshot, Company Facts/submissions responses, and target
filing HTML. Wells Fargo source hashes remain Company Facts
`58033a85c9795b5d834c9a4fb60dfb7f8e600c89304d757af7c40eb4bff69990`,
submissions `c65c0466177cd606e54024bf9389e39bc3186cdc07771fdc2d573ff264ba05b0`,
and filing HTML
`328c8db358333ec790017006b774204360800213cc244d3b03b3f1b3734303aa`.

Integrity checks compared all 75 source columns row-for-row after sorting by
the quarterly key; all matched. All 29,653 originally non-null `oancfy`
values remained unchanged. Wells Fargo resolves to `24,785.0` USD millions.
Mechanics and Globant remain null. A full 30,542-row `Database`/Polars query
returns `resolved_oancfy` with `selected_source`. A latest-per-Barrid join to
the 2026-11-09 assets snapshot used `validate="1:1"`; 2,946 matched rows were
returned from the 23,753-row asset snapshot, including Wells Fargo with source
`sec`. Separate Barrids remain separate share classes.

The builder was rerun with the same cutoff. All three yearly Parquet hashes and
the manifest hash remained unchanged. Focused offline tests:

```bash
.venv/bin/python -m pytest -q \
    tests/test_sec_fallback.py \
    tests/test_resolved_fundamentals.py \
    tests/test_sec_ocf_cohort.py
```

Result: 24 passed. Coverage includes identity changes, entity and scope gates,
conflicting accessions, amendments with and without an exact fact, cutoff
boundaries, units and conversion, preservation of null/non-null values, and
fiscal-YTD labeling. `git diff --check` passed. The FIPS-compatible durable
Python 3.13.18 runtime was used for the build and Database/Polars checks.
The annual/quarterly quickstart also succeeded with
`.venv-fips/bin/python examples/compustat_quickstart.py --asset-date 2026-11-09`:
it read both Database tables and passed the one-to-one annual-to-assets join
(23,753 asset rows, 2,966 profitability rows, 2,943 matched rows).

### Remaining limitation and next task

Only Wells Fargo has enough archived evidence in this bounded sample for
acceptance. The other eight non-Mechanics candidates remain late, unavailable,
or incompatible; Mechanics remains under identity review. A historical
investment availability cutoff is still needed before point-in-time research.
The next concrete task is to collect and independently review identity,
consolidation, and exact-filing evidence for another bounded set of null
`oancfy` company-periods, then add explicit candidate configurations only
where those gates pass.

## One-command development workflow (2026-11-11)

The repeatable development entry point is
`scripts/build_development_fundamentals.py`:

```bash
.venv-fips/bin/python -m scripts.build_development_fundamentals \
    --cutoff 2026-10-29T23:59:59+00:00
```

Its five stages are:

1. Check each source run's completion and coverage reports, successful exit,
   report counts, and validated format filters.
2. Reuse an existing complete query scope when its run, mapping, format, and
   version policy match and its Database tables have the expected row counts
   and unique keys. Otherwise call `build_compustat_query_tables`. A changed
   query scope fails safely unless `--rebuild-query-scope` is explicitly set.
3. Rebuild the ten-case SEC cohort from saved Company Facts, submissions, and
   filing documents. It makes no SEC requests. Unreviewed candidates stay
   unresolved and do not block this stage.
4. Apply the configured Wells Fargo acceptance and Mechanics identity-review
   exclusion, then build a policy/input-scoped resolved table.
5. Reopen the scope through `Database.fundamentals_quarterly_resolved_table`
   and check every source column/value, row count, key uniqueness, accepted
   value, excluded Mechanics/Globant values, and required SEC provenance.

The end-to-end command succeeded against source run
`run_bd766d2869a3a83fce895048`. The matching query scope was reused:
materialization ID `d8ae2e933d77d9e144988cc2306f5370ba4a8a4626880c9d473dc7c657f5657f`,
with 6,306 annual and 30,542 quarterly rows. The bounded cohort contained ten
cases: 1 verified Wells Fargo candidate, 4 late filings, 3 with no matching
filing in saved submissions, 1 incompatible Globant case, and 1 unresolved
Mechanics identity case. These review outcomes did not block the resolved
build; only Wells Fargo was accepted.

Final resolved scope ID:
`b22e2f39af3e10bf8f4beef8b0a224057d92e4eaa28385e39e6c1a676994bf7d`.
Output directory:
`/home/gunnjake/groups/grp_quant/database/development/fundamentals_quarterly_resolved_b22e2f39af3e10bf/`.
Checks: 30,542 rows, 30,542 unique keys, all 75 original columns and values
identical, all 29,653 original non-null `oancfy` values preserved, one accepted
SEC row at `24,785.0` USD millions, and 888 null resolved values. Wells Fargo,
Mechanics, and Globant all matched their configured outcomes.

Output checksums:

- 2024 Parquet:
  `b937a3e2397fc7ec50a8939f71c0076360a40fa4e26eac8ec571f7838899cc4f`
- 2025 Parquet:
  `9473e1a9f67f4d76b583d6b1ecefe0e4580a71f16ef53771226a7517423b697b`
- 2026 Parquet:
  `332f19fc8382129027eda794e7d900a435654d657d3cd1ffb7d3054e69bf6aa0`
- Manifest:
  `1acbb86bd042d546924c037be6380551ae4b3afae83dda014a40daad38381a75`
- Cohort decision manifest:
  `6435961b6d0def5b28888b6787152c0b1a081a29b93b4e83b502150dcbd61af9`
- Accession observations:
  `843ae7cc864c2581234632d2135bf85dbc6d96aa468d6c20324def32e71877a0`

Focused regression tests passed: 32 tests across SEC fallback, resolver,
cohort, and Compustat query-table suites. The complete command was rerun and
returned the same resolved scope and content-addressed candidate artifacts.
The current matching-scope path takes seconds. Building a changed query scope
requires the full archived-report validation; an attempted fresh scan remained
active past ten minutes on the shared filesystem before it was stopped to use
the already verified matching scope. No jobs were scheduled.

The next task is to continue broader candidate identity and filing review.
Unreviewed periods remain null until their explicit decisions and evidence are
added to configuration; no SEC request or full-universe candidate fetch is
part of this workflow.

## Fresh-run resolver validation repair (2026-11-11)

### Diagnosis

The saved Compustat source run `run_e906597c98025ac25cdf006d` completed
successfully. Its quarterly source extract has 31,575 rows. For Wells Fargo,
GVKEY `008007`, Barrid `USAK1N1`, 2026-09-30, the fresh extract already has
`oancfy=24,785.0` USD millions. The archived SEC filing remains an eligible,
reviewed candidate at the supplied `2026-10-29T23:59:59+00:00` cutoff, but the
resolver preserves that non-null Compustat value and therefore correctly
creates **zero SEC-filled rows** for this fresh scope.

Job `15260698` failed after query publication and candidate discovery because
`scripts/build_resolved_fundamentals.py` required exactly one SEC row and
hardcoded one Wells Fargo Barrid/value. That was a pilot assertion applied to a
fresh materialization. The candidate cohort is regenerated from the current
source run and content-addressed archived responses; the latest decision
manifest is `cohort_decisions_dd3668c1580deef55fa5faf196d67b1af6c60c312eb526d10f3d9784f9b4cd45.json`.
Its classifications differ from the earlier null-gap pilot because the fresh
Compustat extract now contains values for AAT, Home BancShares, Goldman Sachs,
Wells Fargo, JPMorgan, and FB Financial. The cohort marks those current rows
unresolved as not null gaps. The SEC archive inputs and comparison cutoff did
not change. Mechanics remains unresolved due conflicting accessions/accounting
predecessors; Globant remains incompatible because supported US-GAAP coverage
is absent. No additional SEC candidates were added and no SEC requests were
made.

### Fixes and checks

- Removed the exact-one fallback expectation. Current SEC-filled keys must be
  a subset of accepted reviewed company-periods and securities mapped in the
  saved mapping snapshot. Expected zero fills are valid when the reviewed
  source row is already non-null.
- The resolved validation no longer requires exactly one accepted row or a
  hardcoded Wells Fargo amount. It checks accepted-row provenance, USD units,
  YTD semantics, period end, original columns and values, original non-null
  values, row counts, and unique keys.
- Accepted Wells Fargo decisions are applied only when the freshly generated
  cohort still classifies that period as a verified fallback candidate. SEC
  candidates are expanded over securities derived from the saved GVKEY/CIK
  mapping, and output validation accepts zero, one, or multiple eligible keys.
- The saved-run wrapper no longer forces a query rebuild. The existing query
  manifest was verified through `_reuse_matching_query_scope`: materialization
  `ab732d13fd3a2d4ea8723e38e350c68b0cd84bd6e10d422c2c05d83660e4b81c`, 6,334
  annual rows, 31,789 quarterly rows, complete matching run/mapping/filter
  scope, and unique keys.
- Offline tests under the durable FIPS-compatible Python runtime, with pytest
  loaded from the existing project environment: **43 passed, 3 subtests
  passed**. `git diff --check` passed. The direct `.venv-fips/bin/python -m
  pytest` invocation is unavailable because pytest is not installed in that
  environment; no package was installed.

### Publication and current checkpoint

The prior failed job published a complete query manifest for the fresh run;
the query builder stages outputs, backs up prior Parquet files, publishes the
manifest after replacing staged files, and restores backups on exceptions.
The current manifest and tables passed the builder's reuse validation. No
resolved output scope for this fresh run has yet been verified.

Saved-run job `15265184` was submitted with the requested resume command. It
revalidated the saved source run and entered query building, but the wrapper
version loaded by that job forces a redundant rebuild. The job remains active
at the time of this checkpoint; current log is
`logs/slurm-development-fundamentals-15265184.out`. Cancellation was rejected
by automatic review because the retained instruction prohibited interrupting
processes. Do not submit another job while 15265184 is active. After it exits,
check `sacct` and its log, then resume with
`sbatch scripts/refresh_development_fundamentals.sh --resume-from-run run_e906597c98025ac25cdf006d`;
the updated wrapper will reuse the matching validated query scope. Only report
completion after verifying the resolved manifest and output through
`Database`/Polars. No schedule, production change, commit, push, or PR was made.

## Saved-run recovery checkpoint (2026-11-11)

The fresh Compustat source run `run_e906597c98025ac25cdf006d` was verified
before resumption: completion report says `complete=true`, collector exit code
0, collector process exited, zero missing reports/artifacts, zero identity or
sample mismatches. Its exact mapping snapshot is
`/grphome/grp_quant/database/fundamentals/compustat/security_mappings/membership_2026-08-31_2ff2d354474415670bd7527b24822ab41b89feacb1c522b9a9becd3e91c68035.parquet`
with membership date 2026-08-31. Extract row counts are 6,288 annual and
31,575 quarterly; the completion audit reports 6,288 and 31,563 unique report
identities respectively.

The live collector failure was traced to `run_compustat_fundamentals` calling
`wrds.Connection` without its configured password. The project `.env` and
`~/.pgpass` were both present/readable at preflight; values were not logged.
The collector now passes `WRDS_PASSWORD` to the WRDS client. The batch wrapper
also has a fail-fast credential-presence preflight before collection. This is
configuration verification, not a live WRDS authentication test.

The refresh wrapper now supports `--resume-from-run RUN_ID`. This path verifies
the saved run and recorded mapping, skips collection, and builds from the
completed local run. It does not request fresh SEC evidence. The query builder
can reuse existing query rows only when the prior manifest is complete, filters
and mapping ID match, the old Database tables match manifest counts and unique
keys, and the selected content-addressed extraction snapshot is unchanged. It
still validates each selected report metadata and fiscal periods. Cache reuse
and archive reads are recorded in the query manifest.

A foreground validation was interrupted before publish after 18 minutes in
`_read_report_values` on the shared filesystem. The next cache-backed foreground
attempt exposed and fixed a local import initialization error; it too was stopped
before publish to prevent overlap. Prior active annual/quarterly tables and the
query manifest were copied to `/tmp/fundamentals-development-before-e906597c98025ac25cdf006d`
before any rebuild. No partial query output was published by either foreground
attempt.

Durable resume is now Slurm job `15260698`, submitted as
`sbatch scripts/refresh_development_fundamentals.sh --resume-from-run run_e906597c98025ac25cdf006d`.
It was verified RUNNING on node `m9-2-1`; current log:
`logs/slurm-development-fundamentals-15260698.out`. Check with:

```bash
squeue -j 15260698
sacct -j 15260698 --format=JobID,JobName%30,State,Elapsed,ExitCode
 tail -n 80 logs/slurm-development-fundamentals-15260698.out
```

Offline regressions passed before the last small cache initialization correction:
39 passed, 3 subtests passed across Compustat fundamentals, SEC fallback,
cohort, resolved fundamentals, and quickstart suites. The latest attempted test
command using `.venv-fips` could not start because pytest is absent there. Rerun
with `.venv/bin/python -m pytest` after the batch job completes.

Pending: job 15260698 must complete successfully before fresh query-table and
resolved outputs can be reported. Fresh SEC requests and additional SEC
candidate review were not performed; the original ten-case cohort is not
additional coverage. No schedule or alert was configured. After completion,
verify `sacct`, job log, query manifest run ID/counts, resolved output scope and
manifest, and the actual `Database` query. If the job failed, the previous
resolved scopes and the explicit `/tmp` table backup remain available; resume
with the same command only after confirming the old job is no longer active.

## Full mapped null-period evidence review (2026-11-11)

The fresh-run query materialization completed and is reusable:
`ab732d13fd3a2d4ea8723e38e350c68b0cd84bd6e10d422c2c05d83660e4b81c`, with
6,334 annual and 31,789 quarterly rows, matching source/mapping scope, and
unique keys. Job `15265184` ultimately failed at resolved validation with
`NameError: name 'date' is not defined`; the missing import was added. Focused
offline tests after the fix passed: 23 SEC cohort/resolver tests, and
`git diff --check` passed.

Full-universe discovery now returns every uniquely keyed null OCF-YTD period
with one verified current-CIK mapping and retains all mapped Barrids. Applied
to source run `run_e906597c98025ac25cdf006d`, this found 964 candidates across
543 CIKs. Fourteen of 978 unique null company-periods were skipped for ambiguous
keys, missing fiscal labels, or lack of a unique verified mapping. This is
more restrictive than the earlier rough mapping join, which counted some
non-verified or ambiguous links. Tests cover discovery beyond ten cases and
multiple Barrids per company.

Slurm job `15265688` (`sec-ocf-full-review`) was submitted and verified RUNNING
on node `m8-19-2`. Log:
`logs/slurm-sec-ocf-full-review-15265688.out`. It reuses the completed source
run without recollection, refreshes and archives SEC Company Facts,
submissions, and relevant filing documents for the mapped null periods, then
classifies them into a content-addressed decision manifest and observation
Parquet. It does not overwrite Compustat Parquet files or automatically
accept uncertain candidates. Final SEC fallback decisions and a full-universe
resolved output remain pending review and verification.

The full SEC refresh job `15265688` failed after 32 seconds on its first
Company Facts request with `Network is unreachable` from the Slurm compute node.
It discovered 978 null company-periods (the earlier 979 count included one
duplicate key), 964 strictly eligible mapped periods, and 543 distinct CIKs;
no SEC requests completed and no candidate manifest was produced by this job.
A read-only archive check found both Company Facts and submissions already
archived for only 4 of those 543 CIKs; 539 require network access. Retry from a
network-enabled approved execution environment or after compute-node outbound
access is available. Do not describe the full evidence review as completed.

## SEC fetch and Slurm processing separation (2026-11-11)

Fulton ORC's official [Internet Access from Jobs guidance](https://rc.byu.edu/wiki/?id=Internet+Access+from+Jobs)
states that login nodes have internet, recommends login nodes for downloads
when possible, and permits retrieving research data. The same policy prohibits
“scraping, crawling, or automated probing of external systems.” It is unclear
whether this one-time, targeted request set for 543 CIKs and period-specific
filings falls within permitted research-data retrieval or prohibited scraping.
No fetch was run from a Fulton login node pending ORC clarification. Compute
nodes have no direct internet; the documented compute-node route is the
`http_proxy` module/proxy, but the same permitted-use limits apply.

The fetcher now reuses valid cached JSON responses, fetches missing endpoints
once per CIK, keeps the SEC user agent and Requests TLS verification, uses the
existing retry policy and 0.12-second pacing, hashes archived responses, and
writes an atomic per-CIK progress manifest. Failed endpoint/document requests
are recorded; progress distinguishes complete and partial CIK coverage. Resume
command after ORC confirms the login-node activity is allowed:

```bash
./scripts/refresh_all_sec_ocf_candidates.sh
```

Run from the Fulton login node at the project root after loading `.env`; the
wrapper refuses to run inside Slurm. Fetching does not classify or accept
candidates.

Slurm processing is separate and archive-only. Job `15265893` completed in 11
seconds and made zero SEC requests. It processed 7 periods from 3 CIKs with
complete archived evidence: 5 were classified incompatible and 2 had no
matching disclosure in those complete archives; none were accepted. The other
957 periods across 540 CIKs are marked incomplete/not fetched and explicitly
not classified as no disclosure. Coverage manifest:
`full_universe_evidence_coverage_f5e9341e174f710dd4db060ddd0a4e831e1cf5f5e6a9c52f6bdb20fa025add45.json`.
Decision manifest:
`cohort_decisions_a11aeb7ae150309a54d9f066cb2768abee9e91ca820ffe80a97e459832bd7f22.json`.
Archive-only processing command after the evidence-fetch stage:

```bash
sbatch scripts/process_archived_sec_ocf_candidates.sh
```

The saved-run workflow completed successfully as job `15265950`, reusing the
matching query materialization. Resolved scope:
`a673ab8b7645e4c8d339cda7ff3c29cd2fc1a7058b4dc35cb1643dfec10b2914`.
Output directory:
`/home/gunnjake/groups/grp_quant/database/development/fundamentals_quarterly_resolved_a673ab8b7645e4c8`.
Database/Polars validation returned 31,789 rows and unique keys, preserved all
75 original columns and all 30,805 original non-null `oancfy` values, and
accepted zero SEC rows. The resolved table has 984 null `oancfy` values. No
full-universe resolved output exists yet. Resume command after the full
evidence review is generated:

```bash
sbatch scripts/refresh_development_fundamentals.sh --resume-from-run run_e906597c98025ac25cdf006d
```

Offline tests passed: 34 passed plus 3 subtests; the focused fetch/cache/
coverage and resolver suites passed. No recurring schedule, alert,
production change, commit, push, or PR was made.

The persisted full-discovery input is
`full_universe_null_candidates_396eed2b516815e46c8eb02b48c95385a02d37a630773182160070beca7d5e7a_c1f6d65de8e1c818012c72ccfc139fc0c90d1ae30fa30669a1d9c680f1c51ac3.json`;
it records 978 unique null periods, 964 candidates across 543 CIKs, and 14
unmapped/ambiguous periods, with source/mapping checksums. Fetching and Slurm
processing now both read this same content-addressed input. The retryable
fetch-stage job was **not run** because Fulton guidance is ambiguous about
automated multi-issuer SEC downloads. Existing archives supported 3 complete
CIKs (7 periods); 1 additional CIK had both JSON endpoints but incomplete
filing-document coverage. No new CIKs were fetched, and no SEC requests were
made by Slurm processing.

## Legacy exclusion review and corrected fiscal-key discovery (2026-11-11)

The 14 legacy exclusions were caused by a discovery key that grouped all rows
on `(GVKEY, datadate)` and then retained only the first fiscal row. Twelve
company-dates had multiple Compustat rows with distinct fiscal labels; two had
no uniquely mapped current CIK. The count of 14 therefore described excluded
company-dates, not 14 distinct companies. Source rows, report metadata, CUSIP,
IID, CIK, Barra identifiers, effective intervals, and case dispositions are
captured in the versioned review artifact
`docs/sec_ocf_excluded_period_review_2026-11-v1.json`. It links the completed
source run `run_e906597c98025ac25cdf006d`, the original discovery manifest,
and the corrected discovery checksum. The source quarterly Parquet and its
mapping snapshot were not changed.

Discovery now distinguishes fiscal rows using GVKEY, report date, fiscal year,
fiscal quarter, fiscal-year-end month, and currency. It recovers 13 null fiscal
row candidates from the 12 duplicate-date groups. Corrected discovery contains
979 unique null fiscal keys, 977 eligible candidate rows across 546 CIKs, and
two unmapped rows. The latter are First Bank (`177009`, 2026-09-30) and
TowneBank (`120318`, 2026-09-30); both have a security/Barra mapping but no
issuer CIK in the saved mapping. Do not infer a CIK from ticker or CUSIP.

Review outcome for the 14 legacy company-dates: 11 are resolved exclusions
from SEC fallback because the fiscal scope is inconsistent with the otherwise
present non-null Compustat row, the disclosure belongs to a predecessor or
different accounting scope, or the security relationship starts after the
period. The three remaining company-dates are Hyperliquid Strategies
(`036356`, 2025-09-30), First Bank, and TowneBank. Hyperliquid has two null
fiscal row keys (FY2025 Q4/FYR9 and FY2026 Q1/FYR6) sharing one date. Both
remain unresolved: its SEC 10-Q header reports a December 31 year-end, which
matches neither Compustat fiscal label, and the mapped security relationship
starts after the report date. Targeted SEC evidence could clarify the
accounting scope; neither row is accepted. First Bank and TowneBank remain
unresolved for lack of issuer CIK. No fallback value was accepted, and the
Mechanics exclusion is unchanged.

The case review used saved source/mapping data and public SEC filings. It did
not archive new SEC responses or modify the financial dataset. Fulton ORC
guidance permits internet use from login nodes for research data retrieval but
prohibits scraping/crawling/probing; whether even this targeted SEC request is
permitted remains unclear. Before any network fetch, obtain ORC clarification.
If permitted, fetch only the Hyperliquid date with the existing resumable
fetcher from the approved network-enabled login environment, not Slurm:

```bash
cd /home/gunnjake/projects/sf-data-pipelines-quant
.venv-fips/bin/python -u -m scripts.refresh_all_sec_ocf_candidates \
  --run-id run_e906597c98025ac25cdf006d \
  --company-period 036356:2025-09-30
```

This selects the two Hyperliquid fiscal candidates sharing the date and does
not recollect Compustat. After the fetch, use the archive-only Slurm processor
and review its coverage and decisions; incomplete evidence must remain
explicitly incomplete, not be treated as evidence that a filing is absent.

Offline regressions passed after the correction:
`38 passed in 7.13s` across `test_sec_ocf_cohort.py`,
`test_sec_full_universe.py`, `test_sec_fallback.py`, and
`test_resolved_fundamentals.py`. `git diff --check` passed. The corrected
full-universe discovery manifest was generated under `/tmp` for validation;
the versioned case review records its discovery ID and counts. No SEC fetch,
new financial output, schedule, production change, commit, push, or PR was
made in this review.

### Hyperliquid targeted filing follow-up (2026-11-11)

The targeted fetch completed for CIK `0002078856`: Company Facts, submissions,
and the 2025-09-30 10-Q (accession `0001193125-25-311400`, filed 2025-12-08)
are archived. The 10-Q identifies Hyperliquid Strategies Inc and reports the
cash-flow statement from July 2, 2025 (inception) through September 30, 2025;
net cash flows from operating activities are zero for that inception period.
Note 1 says the entity was formed to effect the proposed transaction and its
pre-transaction activity through September 30 was formation and transaction
preparation. Note 4 says the Sonnet BioTherapeutics combination closed
December 2, 2025. The Compustat/Barra relationship for CUSIP `44916Y106`, IID
`01`, Barra `USBR571`, begins December 3, 2025.

Neither Compustat fiscal row is a valid duration match: FY2025 Q4/FYR9 implies
October 1, 2024 through September 30, 2025; FY2026 Q1/FYR6 implies April 1
through September 30, 2025. The SEC filing's inception-to-date duration starts
July 2, 2025. Both fiscal candidates are therefore resolved exclusions from
SEC fallback. Do not accept the filing's zero as either candidate value. The
processor's broad full-universe coverage remains incomplete and still lists
this date pending; the case-specific conclusion and filing provenance are
recorded in `docs/sec_ocf_excluded_period_review_2026-11-v2.json`, which
supersedes v1. Across the 14 legacy company-dates, 12 are now resolved
exclusions and 2 remain unresolved for missing issuer CIK (First Bank and
TowneBank); zero financial values were accepted.

The targeted cohort build wrote content-addressed decision and observation
artifacts. Its automated identity gate still marks both Hyperliquid rows
unresolved because the saved security relationship begins after the report
date; the manual review resolves only SEC fallback eligibility, excluding
both due to fiscal-duration and pre-merger scope mismatch. Keep those two
values null. Reproduce the archive-only full-universe processing with:

```bash
.venv/bin/python -u -m scripts.process_archived_sec_ocf_candidates \
  --run-id run_e906597c98025ac25cdf006d
```

This makes no SEC requests. It does not override the identity gate or convert
the Hyperliquid rows into accepted values.

### First Bank and TowneBank filing-history review (2026-11-11)

The two missing-CIK names resolve to the issuers themselves, not a parent:
First Bank of Hamilton, New Jersey (CUSIP `31931U102`, IID `01`, Barra
`USBBML1`, ticker `FRBA`, FDIC certificate `58481`, RSSD `3401970`) and
TowneBank of Portsmouth, Virginia (CUSIP `89214P109`, IID `01`, Barra
`USA2NT1`, ticker `TOWN`, FDIC certificate `35095`). Both have Exchange Act
filing histories through the FDIC's Securities Exchange Act Filing system.
The FDIC confirms its system contains 10-K, 10-Q, and 8-K reports submitted
by FDIC-supervised institutions; these are not SEC EDGAR CIK records.

First Bank's Q3 2025 10-Q identifies First Bank as the exact registrant,
lists FRBA common stock, and states its consolidated statements include its
wholly owned subsidiaries. The bank published Q3 2026 results on October 22;
no Q3 2026 10-Q was located in this review. TowneBank's filing page listed a
Q3 2026 10-Q on November 4. Its 2025 8-K identifies TowneBank itself as
registrant, ticker TOWN, FDIC certificate `35095`, and EIN `54-1910608`.
TowneBank's consolidated scope also includes mortgage, resort/vacation, and
insurance segments, which must be respected in any later value review.

The versioned evidence is in
`docs/sec_ocf_bank_filing_review_2026-11-v1.json`. Both issuer identities are
resolved to FDIC filers, but neither has a supported SEC CIK mapping. Keep
both rows out of SEC EDGAR candidate inputs; do not infer a CIK or use a parent
filing. The 12 resolved exclusions stay skipped, the broader SEC workflow can
continue, and no financial value was accepted or data snapshot changed.
The validated resolved table applies the exact selection reason
`FDIC filer; separate source required.` to both 2026-09-30 rows and checks that
both values remain null.

### Full-universe resolved development output (2026-11-12)

The development builder now consumes the completed archive-processing job
15268690 artifacts named in `logs/slurm-sec-ocf-archive-process-15268690.out`.
It reconciles 977 discovered company-periods as 976 classified decisions plus
one pending incomplete-evidence case (CIK `0001569650`). The six
`Verified SEC fallback candidate` labels did not satisfy the explicit accepted
decision and review-field gates, so accepted company-periods: 0 and SEC-filled
Barra rows: 0. The pending case is not classified as no disclosure.

Build command (reuses the saved source run and existing query materialization;
no collection or SEC fetch):

```bash
.venv/bin/python -m scripts.build_development_fundamentals \
  --run-id run_e906597c98025ac25cdf006d
```

Validated resolved scope:
`/home/gunnjake/groups/grp_quant/database/development/fundamentals_quarterly_resolved_696b42c67f4c197e`
(scope `696b42c67f4c197e95eac3f3d905c47bfe2b47ec06302ed15efed242c4e0ee45`).
It has 31,789 rows and unique keys, preserves all 75 original columns and
30,805 non-null `oancfy` values, has 984 remaining null resolved values, and
records 976 classified, 1 pending, and 977 excluded/unaccepted periods. The
existing query materialization `ab732d13fd3a2d4ea8723e38e350c68b0cd84bd6e10d422c2c05d83660e4b81c`
was reused. A Database/Polars query returned 1,891 rows for 2026-09-30 and zero
SEC fills. Output hashes for the 2024, 2025, and 2026 Parquets match the scope
manifest. A repeated build returned the same scope and verified the immutable
output.

The prior failed pilot scope `31076c8505223e5c…` remains on disk as an
unvalidated historical artifact; it is not referenced by the active Compustat
query manifest. The current validated full-universe output is exposed through
its scope-specific `Database.fundamentals_quarterly_resolved_table` interface.

## Full-universe reviewed-candidate resolver output (2026-11-16)

The six archived “Verified SEC fallback candidate” periods in the 976-case
full-universe manifest were checked against the separate review artifact
`docs/sec_ocf_full_universe_candidate_review_v1.json`. The resolver now verifies
the review schema, source-manifest SHA-256, source run, timezone-aware cutoff,
mapping snapshot SHA-256, original null Compustat values, archived Company
Facts/submissions/filing hashes, exact accession values, filing acceptance
metadata, and inline XBRL amount before passing a candidate to the existing
`resolve_quarterly_candidates` gates. The review artifact hash is part of the
resolved scope. The full-universe workflow explicitly selects the generic
reviewed-candidate resolver even when the broad manifest also contains a
Wells Fargo candidate label.

Accepted company-periods and converted values (Compustat USD millions):

| GVKEY | Period end | SEC value | Resolved value |
| --- | --- | ---: | ---: |
| 013562 | 2026-09-30 | 84,357,000 USD | 84.357 |
| 033066 | 2026-09-30 | 56,989,000 USD | 56.989 |
| 166435 | 2026-09-30 | -109,723,000 USD | -109.723 |
| 039836 | 2024-09-30 | -1,566,598 USD | -1.566598 |
| 039836 | 2024-06-30 | -1,983,883 USD | -1.983883 |
| 039836 | 2024-03-31 | -289,841 USD | -0.289841 |

The three Syntec observations are raw USD amounts with no inline scale, and
were converted by dividing by 1,000,000. The original filing and later
comparatives agree for each exact YTD duration. Brandywine's undimensioned
consolidated fact and its operating-partnership dimension fact agree in value;
the consolidated context was selected. Vanda's negative operating cash flow
sign is retained as reported.

The immutable development output is scope
`554ee70eae30809295529849ef67eea627ae928ccb1d34cddcf76fb47ab08190` at
`/home/gunnjake/groups/grp_quant/database/development/fundamentals_quarterly_resolved_554ee70eae308092/`.
It has 31,789 rows, 31,789 unique keys, all 75 original columns unchanged,
30,805 original non-null `oancfy` values preserved, six accepted company-periods,
six SEC-filled Barra rows, and 978 remaining `resolved_oancfy` null rows.
Full-universe accounting remains 977 discovered = 976 classified + 1 pending
(CIK `0001569650`); the Company Facts 404 remains unresolved.

Output SHA-256:

- `fundamentals_quarterly_resolved_554ee70eae308092_2024.parquet`:
  `92b383c60d7ff6e0675c124e9698897677c22d225010d3cddfe6c260018741d2`
- `fundamentals_quarterly_resolved_554ee70eae308092_2025.parquet`:
  `dbdc967786dd52afaed8b151894d259a3c21fe556c8c149b80ba9d9e5d4dec33`
- `fundamentals_quarterly_resolved_554ee70eae308092_2026.parquet`:
  `590c63bcd70450d5dfc7482334a9a79c3d16f025ee9171785083d73c4ce546bf`
- `manifest.json`: `f073b2d3766291befa4df98defd7746ed469351287cffd39a3230342ae2e110f`

The full build command, also used for the idempotency rerun, was:

```bash
.venv/bin/python -m scripts.build_development_fundamentals \
  --cutoff 2026-10-29T23:59:59+00:00 \
  --run-id run_e906597c98025ac25cdf006d \
  --review-artifact docs/sec_ocf_full_universe_candidate_review_v1.json
```

The second run returned the same scope ID and output hashes without changing
the resolved output. Focused tests:

```bash
.venv/bin/python -m pytest -q tests/test_sec_reviewed_candidates.py
```

Result: 5 passed. The tests cover the six reviewed values, Syntec negative
raw-USD conversion, Brandywine consolidated versus dimensional contexts,
source-hash mismatch rejection, 977/976/1 full-universe accounting, Database
query values, unique keys, and preservation of original columns and non-null
Compustat values.

Working Database/Polars query:

```python
import polars as pl
from pipelines.utils.enums import DatabaseName
from pipelines.utils.tables import Database

scope = "554ee70eae30809295529849ef67eea627ae928ccb1d34cddcf76fb47ab08190"
resolved = Database(DatabaseName.DEVELOPMENT).fundamentals_quarterly_resolved_table(scope)
supplemented = (
    resolved.read()
    .filter(pl.col("selected_source") == "sec")
    .select("gvkey", "barrid", "datadate", "original_oancfy", "resolved_oancfy",
            "sec_value_raw", "sec_unit", "sec_accession", "sec_acceptance_time",
            "sec_source_url", "sec_filing_html_sha256", "selection_reason",
            "candidate_evidence_json")
    .sort(["datadate", "gvkey"])
    .collect()
)
print(supplemented)
```
