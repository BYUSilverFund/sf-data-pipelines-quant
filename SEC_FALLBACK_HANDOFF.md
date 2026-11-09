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
| Mechanics Bancorp, GVKEY `018948`, 2024-09-30, `oancfy=null` | Original 10-Q accession `0001518715-24-000181`: `-47042000 USD`; later 10-Q comparative accession `0001518715-25-000181`: `217358000 USD` for the same YTD duration | **Unresolved.** Both filing pages corroborate their respective values. The later comparative is not a 10-Q/A, and the cause of the revision is not established here. Both accessions are retained; neither value is selected as a fallback. |
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
| Mechanics Bancorp | `018948`, 2024-09-30 | Original 10-Q filed 2024-11-08: `-47042000 USD`; 2025-11-17 comparative 10-Q: `217358000 USD` for the same 2024 YTD duration | **Unresolved** — conflicting accession values; both are corroborated by inline XBRL and preserved. |
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
comparison in this expanded cohort. Do not integrate SEC data into
quant-facing tables. The next step is to resolve the Mechanics comparative
change and get a historical investment cutoff before any point-in-time test;
Wells Fargo is the sole current candidate for separate downstream review.
