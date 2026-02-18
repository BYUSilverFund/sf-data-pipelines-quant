# Silver Fund Quant Data Pipelines

This repository contains ingestion and production pipelines for the quant platform.  
All pipelines are executed via a Click-based command line interface (CLI).


# Infrastructure Overview

![Infrastructure](graphics/sf-pipelines-infrastructure.png)



The system supports:

- **Backfills** — historical processing over a date range
- **Daily Updates** — incremental updates over recent market days

---

# Table of Contents

- [Architecture Overview](#architecture-overview)
- [Environment Setup](#environment-setup)
- [Databases & Environments](#databases--environments)
- [CLI Usage](#cli-usage)
- [Pipeline Reference](#pipeline-reference)
  - [Barra](#1-barra-pipeline-barra)
  - [CRSP](#2-crsp-pipeline-crsp)
  - [CRSP v2](#3-crsp-v2-pipeline-crsp_v2)
  - [FTSE](#4-ftse-pipeline-ftse)
  - [Signals](#5-signals-pipeline-signals)
  - [Portfolio (Full Production Flow)](#6-portfolio-pipeline-portfolio)
- [Operational Notes](#operational-notes)
- [Common Workflows](#common-workflows)
- [Command Summary](#command-summary)

---

# Architecture Overview

The system consists of:

### Ingestion Pipelines
- Barra
- CRSP
- CRSP v2
- FTSE

These pull raw vendor data into year-partitioned parquet tables.

### Production Model Pipelines
- Signals
- Portfolio

The **Portfolio pipeline** runs the full modeling stack:

1. Signals
2. Scores (cross-sectional z-scores)
3. Signal alphas  
   `signal_alpha = score * ic * specific_risk`
4. Combined alpha (via combinator)
5. Optimal weights (mean-variance optimization)

All intermediate stages are persisted for auditability.

---

# Environment Setup

## Setup
1. Initialize Python virtual environment

```bash
uv sync
```

2. Add a .env file in the root of your working directory with the following environment variables
- ROOT: The path to your home directory
- WRDS_USER: The username to your WRDS account
- ASSETS_TABLE
- EXPOSURES_TABLE
- COVARIANCES_TABLE
- FACTORS_TABLE
- FF_TABLE
- CRSP_V2_DAILY_TABLE
- CRSP_V2_MONTHLY_TABLE
- CRSP_DAILY_TABLE
- CRSP_MONTHLY_TABLE
- SIGNALS_TABLE
- SCORES_TABLE
- ALPHAS_TABLE
- COMBINED_ALPHAS_TABLE
- WEIGHTS_TABLE

3. Activate Python virtual environment

```bash
source .venv/bin/activate
```

# CLI Usage

All commands require:
```bash
--database {research|development|production}
```

### General Syntax

```bash
python <pipelines> <command> <pipeline_type> [options]
```

General Example:
```bash
python pipelines barra backfill --database production --start 2020-01-01 --end 2020-12-31
```

# Pipeline Reference

### Barra Pipeline 
Purpose: Ingest and update barra vendor data.

Modes:
- backfill
- update


Backfill Example:
```bash
python pipelines barra backfill \
  --database research \
  --start 2010-01-01 \
  --end 2010-12-31
```

Daily Update Example:
```bash
python pipelines barra update --database production
```

### CRSP Pipeline 
Purpose: Backfill CRSP data from WRDS.

Modes:
- backfill only

Requires:
- WRDS_USER environment variable

Example:
```bash
python pipelines crsp backfill \
  --database research \
  --start 1990-01-01 \
  --end 1999-12-31
```

### CRSP v2 Pipeline 
Purpose: Second-generation CRSP ingestion.

Modes:
- backfill only

Requires:
- WRDS_USER environment variable

Example:
```bash
python pipelines crsp_v2 backfill \
  --database development \
  --start 2000-01-01 \
  --end 2005-12-31
```

### FTSE Pipeline 
Purpose: Backfill FTSE data from WRDS.

Modes:
- backfill only

Requires:
- WRDS_USER environment variable

Example:
```bash
python pipelines ftse backfill \
  --database research \
  --start 1995-07-31 \
  --end 2000-12-31
```

### Signals Pipeline 
Purpose: Compute and persist signal values.

Signals are dynamically discovered by subclasses of BaseSignal.

Modes:
- backfill only


Backfill All Signals Example:
```bash
python pipelines signals backfill \
  --database research \
  --signal all \
  --start 2015-01-01 \
  --end 2016-12-31
```

Backfill Single Signals Example:
```bash
python pipelines signals backfill \
  --database production \
  --signal momentum \
  --start 2020-01-01 \
  --end 2020-06-30
```

### Portfolio Pipeline 
Purpose: Runs the full production modeling pipeline.
1. Signals
2. Scores
3. Signal-level alphas
4. Combined alpha
5. Optimal weights

Signals are dynamically discovered by subclasses of BaseSignal.

Options: 
- days (Default = 3)
- ic (Default = 0.5)
- combinator (Default = mean)

Modes:
- backfill only


Backfill Example:
```bash
python pipelines portfolio backfill \
  --database production \
  --start 2022-01-01 \
  --end 2022-12-31 \
  --ic 0.5 \
  --combinator mean
```

Daily Update Example:
```bash
python pipelines_cli.py portfolio update \
  --database production \
  --days 3 \
  --ic 0.5 \
  --min-price 5 \
  --combinator mean
```