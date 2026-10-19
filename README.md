<div align="center">

# Retail Sales ETL Pipeline

### End-to-end data pipeline built with Python & SQL

From messy CSV, JSON and API exports to a tested, analytics-ready star schema, with incremental loads, SCD Type 2 history and a data quality gate.

![Python](https://img.shields.io/badge/Python-3.10%E2%80%933.13-3776AB?logo=python&logoColor=white)
![SQL](https://img.shields.io/badge/SQL-ANSI-4479A1?logo=databricks&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-4169E1?logo=postgresql&logoColor=white)
![SQLite](https://img.shields.io/badge/SQLite-3-003B57?logo=sqlite&logoColor=white)
![pandas](https://img.shields.io/badge/pandas-2.x%20%7C%203.x-150458?logo=pandas&logoColor=white)
![SQLAlchemy](https://img.shields.io/badge/SQLAlchemy-2.0-D71F00)
![Docker](https://img.shields.io/badge/Docker-ready-2496ED?logo=docker&logoColor=white)
![Tests](https://img.shields.io/badge/tests-37%20passing-2EA44F)
![License](https://img.shields.io/badge/license-MIT-green)

**Built by [Aitomation](https://aitomation.com)** · Data Engineering · Automation · Analytics

</div>

---

> [!NOTE]
> This repository is a **sanitised, fully runnable reference build** of the kind of ETL work we deliver for clients. All client names and data have been replaced with synthetic data. The generator injects the same kinds of defects we see in real source systems, so the pipeline has the same problems to solve.

## Table of contents

- [The business problem](#the-business-problem)
- [What this pipeline delivers](#what-this-pipeline-delivers)
- [Architecture](#architecture)
- [How a run works](#how-a-run-works)
- [Data model](#data-model)
- [Data quality](#data-quality)
- [Incremental loading & change history](#incremental-loading--change-history)
- [Quick start](#quick-start)
- [Sample output](#sample-output)
- [Project structure](#project-structure)
- [Configuration](#configuration)
- [Testing & CI/CD](#testing--cicd)
- [Design decisions](#design-decisions)
- [Taking it to production](#taking-it-to-production)
- [About Aitomation](#about-aitomation)

---

## The business problem

A multi-channel retailer keeps its data in three systems that were never designed to talk to each other:

| Source system | Format | Typical issues |
|---|---|---|
| **E-commerce platform** (orders) | Monthly CSV drops | Mixed date formats, `"15%"` vs `0.15` discounts, `$`-formatted prices, re-sent duplicate rows, status changes arriving weeks later |
| **CRM** (customers) | JSON export / REST API | Inconsistent casing and whitespace, invalid emails, duplicate customer versions from re-syncs, customers who move or change segment |
| **ERP** (product catalogue) | CSV export | Pricing errors, missing names, category spelling drift |

Reporting was assembled by hand in spreadsheets. Numbers didn't reconcile between teams, nobody could tell what a customer's segment *was* at the time of a sale, and a single bad file could quietly corrupt a month of figures.

## What this pipeline delivers

| Capability | How it's implemented |
|---|---|
| 🔌 **Multi-source extraction** | Pluggable extractors for CSV (glob over many files), JSON and paginated REST APIs (retries + exponential back-off) |
| 🧹 **Cleaning & standardisation** | Vectorised pandas transforms: whitespace, casing, phone formats, multi-format dates, currency and percentage parsing, alias mapping |
| ✅ **Declarative validation** | A small rule engine splits every batch into *valid* and *rejected* rows. Each rejected row records **every** rule it broke |
| 🗃️ **Quarantine, not silent drops** | Rejected records are written to `etl_rejected_records` with the reason and the full JSON payload, ready for follow-up |
| ⭐ **Kimball star schema** | `fact_sales` at order-line grain plus `dim_customer`, `dim_product` and `dim_date`, all built with plain SQL files |
| 🕰️ **SCD Type 2 history** | Customer moves and segment changes create new dimension versions, so historic sales keep their original attributes |
| ⚡ **Incremental loads** | High-water-mark on `updated_at`. Late-arriving status changes (e.g. *delivered → returned*) are upserted into existing facts |
| 🔁 **Idempotent** | Re-running over the same data changes **zero** rows: row-hash change detection and natural-key upserts |
| 🛡️ **Quality gate + atomic commit** | Six SQL checks plus a reject-rate threshold run *inside* the load transaction. A failure rolls back the whole load |
| 📒 **Full audit trail** | Every run logs its status, row counts, quality results and rejects, even when the load fails |
| 🐘 **Portable SQL** | The same SQL files run on **SQLite** (zero-setup dev/CI) and **PostgreSQL** (production) |
| 🧪 **Tested & containerised** | 37 unit + end-to-end tests, a GitHub Actions matrix (Python 3.10–3.13), a real-Postgres integration job, Docker Compose |

---

## Architecture

```mermaid
flowchart LR
    subgraph SRC["📥 Source systems"]
        direction TB
        S1["🛒 E-commerce platform<br/>orders_*.csv"]
        S2["👥 CRM<br/>customers.json / REST API"]
        S3["📦 ERP<br/>products.csv"]
    end

    subgraph PY["🐍 Python layer"]
        direction TB
        E["<b>Extract</b><br/>CSV · JSON · REST<br/>raw strings only"]
        W{{"Incremental filter<br/>updated_at &gt; watermark"}}
        T["<b>Transform</b><br/>clean · standardise<br/>deduplicate · enrich"]
        V{"<b>Validate</b><br/>declarative rules"}
    end

    subgraph DB["🗄️ SQL warehouse · SQLite / PostgreSQL"]
        direction TB
        STG[("Staging<br/>stg_customers<br/>stg_products<br/>stg_orders")]
        subgraph STAR["⭐ Star schema"]
            DC[("dim_customer<br/>SCD2")]
            DP[("dim_product<br/>SCD1")]
            DD[("dim_date")]
            F[("fact_sales")]
        end
        Q{"🛡️ Quality gate<br/>6 SQL checks +<br/>reject-rate limit"}
        META[("Audit & metadata<br/>etl_run_log · etl_watermarks<br/>etl_quality_results")]
        REJ[("🚫 Quarantine<br/>etl_rejected_records")]
    end

    BI["📊 Analytics<br/>sql/analytics/*.sql<br/>BI tools"]

    S1 & S2 & S3 --> E --> W --> T --> V
    V -- valid --> STG
    V -- rejected --> REJ
    STG -- "MERGE SQL<br/>sql/transform" --> DP & DC
    DP & DC & DD --> F
    F --> Q
    Q -- "pass → COMMIT" --> BI
    Q -. "fail → ROLLBACK" .-> META
    Q --> META
```

**Layers at a glance**

| Layer | Responsibility | Lives in |
|---|---|---|
| Extract | Read sources *as raw strings*. No type coercion, so nothing is lost before validation | [`src/etl_pipeline/extract/`](src/etl_pipeline/extract) |
| Transform | Parse, clean, standardise, deduplicate, derive measures and hashes | [`src/etl_pipeline/transform/`](src/etl_pipeline/transform) |
| Validate | Rule engine → valid rows + rejected rows with reasons | [`transform/validation.py`](src/etl_pipeline/transform/validation.py) |
| Stage | Truncate-and-load typed staging tables for the current batch | [`sql/ddl/001_staging.sql`](sql/ddl/001_staging.sql) |
| Model | SCD1 / SCD2 / fact upserts as plain, reviewable SQL | [`sql/transform/`](sql/transform) |
| Assure | Post-load SQL checks; the gate decides commit vs rollback | [`sql/quality/`](sql/quality) |
| Serve | Ready-made analytics queries | [`sql/analytics/`](sql/analytics) |

---

## How a run works

All warehouse writes for a run happen in **one database transaction**. The audit trail is written in a **separate** transaction, so it survives a rollback and always explains what happened.

```mermaid
sequenceDiagram
    autonumber
    actor Op as Scheduler / Operator
    participant P as EtlPipeline
    participant S as Sources
    participant WH as Warehouse (txn)
    participant A as Audit tables

    Op->>P: etl-pipeline run
    P->>A: INSERT etl_run_log (status = running)
    P->>WH: BEGIN
    P->>WH: read watermark for orders
    P->>S: extract customers, products, orders
    S-->>P: raw DataFrames
    Note over P: filter orders where updated_at > watermark

    P->>P: transform + validate products
    P->>WH: stage → 010_dim_product.sql (SCD1 upsert)
    P->>P: transform + validate customers
    P->>WH: stage → 020_dim_customer_scd2.sql (expire + insert)
    P->>WH: fetch known customer / product ids
    P->>P: transform + validate orders (FK checks)
    P->>WH: upsert dim_date, stage → 030_fact_sales.sql

    P->>WH: run sql/quality/*.sql + reject-rate check
    alt all blocking checks pass
        P->>WH: advance watermark
        P->>WH: COMMIT ✅
    else a blocking check fails
        P->>WH: ROLLBACK ❌ (warehouse untouched)
    end
    P->>A: UPDATE run log · INSERT rejects + quality results
    P-->>Op: summary (exit 0, or 2 if the gate failed)
```

---

## Data model

A Kimball-style star schema. The fact grain is **one row per order line** (`order_item_id`).

```mermaid
erDiagram
    dim_date ||--o{ fact_sales : "date_key"
    dim_customer ||--o{ fact_sales : "customer_key"
    dim_product ||--o{ fact_sales : "product_key"

    fact_sales {
        bigint sales_key PK
        varchar order_item_id UK "natural key, grain"
        varchar order_id
        int date_key FK
        bigint customer_key FK "version valid at time of sale"
        bigint product_key FK
        int quantity
        numeric unit_price
        numeric gross_amount
        numeric discount_amount
        numeric net_amount
        numeric cost_amount
        numeric profit_amount
        varchar order_status
        varchar payment_method
        timestamp source_updated_at "drives upsert ordering"
        varchar batch_id "lineage: run that loaded it"
    }
    dim_customer {
        bigint customer_key PK "surrogate"
        varchar customer_id "business key"
        varchar full_name
        varchar email
        varchar city
        varchar state
        varchar segment
        char row_hash "MD5 of tracked attributes"
        timestamp valid_from
        timestamp valid_to "NULL while current"
        smallint is_current "unique per customer when 1"
    }
    dim_product {
        bigint product_key PK
        varchar product_id UK
        varchar product_name
        varchar category
        varchar subcategory
        varchar brand
        numeric unit_cost
        numeric list_price
        char row_hash
    }
    dim_date {
        int date_key PK "YYYYMMDD"
        date full_date
        varchar day_name
        smallint month
        varchar month_name
        smallint quarter
        smallint year
        smallint is_weekend
    }
```

Supporting tables:

| Table | Purpose |
|---|---|
| `stg_customers`, `stg_products`, `stg_orders` | Typed, validated copy of the current batch (truncate + load) |
| `etl_run_log` | One row per run: status, timings, rows extracted / rejected / loaded, error message |
| `etl_watermarks` | High-water mark per incremental source |
| `etl_rejected_records` | Quarantine: source, business key, failed rules, full JSON payload |
| `etl_quality_results` | Outcome of every quality check, for every run |

---

## Data quality

Quality is enforced at **two levels**.

### 1. Record-level validation (Python, before load)

Each entity declares its rules ([example](src/etl_pipeline/transform/orders.py)). Failing rows are quarantined with *every* broken rule listed, e.g. `"missing_id, invalid_email"`.

| Entity | Rules |
|---|---|
| Customers | `missing_customer_id` · `missing_name` · `invalid_email` · `unknown_segment` |
| Products | `missing_product_id` · `missing_product_name` · `missing_category` · `invalid_list_price` · `invalid_unit_cost` |
| Orders | `missing_order_item_id` · `missing_order_id` · `invalid_order_date` · `invalid_updated_at` · `invalid_quantity` · `invalid_unit_price` · `invalid_discount` · `unknown_status` · `unknown_customer` · `unknown_product` |

Problems that can be fixed safely are repaired rather than rejected: `"  cust-00012 "` → `CUST-00012`, `"$1,299.00"` → `1299.00`, `"15%"` → `0.15`, `"03/02/2026"` → `2026-03-02`, `"Canceled"` → `cancelled`, `"VISA"` → `credit_card`.

### 2. Warehouse-level checks (SQL, inside the load transaction)

Each check is a standalone SQL file that returns the number of violating rows ([`sql/quality/`](sql/quality)). Adding a check means dropping in a new file. No Python changes needed.

| Check | Severity | Guarantees |
|---|---|---|
| `fact_orphan_keys` | 🔴 error | Every fact references an existing date, customer and product |
| `fact_amounts_reconcile` | 🔴 error | `net = gross − discount` to the cent on every line |
| `fact_valid_measures` | 🔴 error | Positive quantities and prices, and a discount never above gross |
| `dim_customer_single_current` | 🔴 error | Exactly one current SCD2 version per customer |
| `dim_customer_valid_windows` | 🔴 error | Expired versions are closed and current versions are open-ended |
| `fact_not_empty` | 🟡 warn | The fact table has data |
| `reject_rate` | 🔴 error | Rejected share of extracted records ≤ `max_reject_rate` (default 5%) |

A failed **error** check raises `QualityGateError`, the transaction is **rolled back**, the watermark does not move, and the CLI exits with code `2`. Schedulers treat that as a failed task. The rejects and check results are still saved for diagnosis.

---

## Incremental loading & change history

| Entity | Strategy | Why |
|---|---|---|
| `fact_sales` | **Incremental**: only records with `updated_at` > stored watermark, then `INSERT … ON CONFLICT DO UPDATE` *only if the incoming record is newer* | Order volumes grow without bound, and late status changes must update existing lines |
| `dim_customer` | **SCD Type 2**: full snapshot compared by `row_hash`. Changed customers get their current row expired and a new version inserted | Sales must keep the segment and location the customer had *at the time of sale* |
| `dim_product` | **SCD Type 1**: upsert, overwriting only when `row_hash` changed | Catalogue corrections should apply everywhere |
| `dim_date` | Generated for the span of loaded order dates, `ON CONFLICT DO NOTHING` | Static reference data |

**Real example from the April increment.** Customer `CUST-00010` moved from Nashville to Houston:

| customer_key | customer_id | city | state | valid_from | valid_to | is_current |
|---:|---|---|---|---|---|:---:|
| 279 | CUST-00010 | Nashville | TN | run 1 | run 3 | 0 |
| 403 | CUST-00010 | Houston | TX | run 3 | *NULL* | 1 |

January–March sales still join to key `279` (Nashville). April sales join to `403` (Houston). Regional reports stay correct for both periods.

The SCD2 logic is two plain SQL statements, [`sql/transform/020_dim_customer_scd2.sql`](sql/transform/020_dim_customer_scd2.sql):

```sql
-- 1. expire the current version of customers whose attributes changed
UPDATE dim_customer
SET valid_to = :run_ts, is_current = 0
WHERE is_current = 1
  AND EXISTS (SELECT 1 FROM stg_customers s
              WHERE s.batch_id = :batch_id
                AND s.customer_id = dim_customer.customer_id
                AND s.row_hash <> dim_customer.row_hash);

-- 2. insert a new current version for new + changed customers
INSERT INTO dim_customer (customer_id, ..., valid_from, is_current)
SELECT s.customer_id, ..., :run_ts, 1
FROM stg_customers s
WHERE s.batch_id = :batch_id
  AND NOT EXISTS (SELECT 1 FROM dim_customer d
                  WHERE d.customer_id = s.customer_id AND d.is_current = 1);
```

---

## Quick start

### Option A: local (SQLite, zero setup)

```bash
git clone <this-repo-url> && cd python-sql-etl-pipeline
python -m venv .venv && source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -e ".[dev,postgres]"

etl-pipeline run          # baseline load: Q1 2026 (Jan–Mar)
etl-pipeline run          # re-run → 0 rows changed (idempotent)

python scripts/generate_sample_data.py --increment   # April drop: new orders, returns, movers, price changes
etl-pipeline run          # incremental load: SCD1 + SCD2 + late updates

etl-pipeline report       # analytics
etl-pipeline status       # run history
```

The SQLite warehouse is written to `warehouse/warehouse.db`. Open it with any SQL client (DBeaver, DataGrip, the `sqlite3` CLI).

### Option B: Docker (PostgreSQL)

```bash
docker compose up --build --abort-on-container-exit etl   # starts Postgres 16 and runs the pipeline
docker compose run --rm etl --log-level WARNING report    # query the loaded warehouse
docker compose down -v                                    # tear down
```

Postgres is exposed on `localhost:5432` (`etl` / `etl`, database `warehouse`), so you can connect a BI tool to it directly.

### CLI reference

```text
etl-pipeline [-c CONFIG] [--log-level LEVEL] <command>

  init-db                 create / update the warehouse schema (idempotent)
  run [--full-refresh]    run the pipeline; --full-refresh ignores the watermark
  report [--query NAME]   run sql/analytics/*.sql (or a single query by name)
  status [--limit N]      show recent runs from etl_run_log

exit codes: 0 success · 1 error · 2 quality gate failed (load rolled back)
```

<details>
<summary><b>Make targets</b> (Linux / macOS / WSL)</summary>

| Target | Action |
|---|---|
| `make install` | Install with dev + postgres extras |
| `make demo` | Full walkthrough: fresh baseline → re-run → increment → report |
| `make run` / `make run-full` | Incremental run / full refresh |
| `make data` / `make increment` | Regenerate baseline / append the April drop |
| `make report` / `make status` | Analytics / run history |
| `make test` / `make lint` / `make format` | Tests with coverage / ruff checks / auto-format |
| `make docker-up` / `make docker-report` | Run on PostgreSQL in Docker |
| `make clean` | Remove the local warehouse and caches |

</details>

---

## Sample output

These are real outputs from the bundled sample data, not mock-ups.

<details open>
<summary><b>Baseline run</b>: 3,198 records extracted, 43 quarantined, 7/7 checks passed</summary>

```text
| INFO    | extract.file_extractors | Extracted 405 rows from customers.json
| INFO    | extract.file_extractors | Extracted 66 rows from 1 file(s) for source 'products'
| INFO    | extract.file_extractors | Extracted 2,727 rows from 3 file(s) for source 'orders'
| WARNING | pipeline                | Products: 64 valid, 2 rejected (top reasons: {'invalid_list_price': 1, 'missing_product_name': 1})
| INFO    | warehouse               | Applied 010_dim_product.sql -> rows affected per statement: [64]
| INFO    | transform.customers     | Customers: collapsed 4 duplicate record(s) to the latest version
| WARNING | pipeline                | Customers: 397 valid, 4 rejected (top reasons: {'invalid_email': 3, 'missing_customer_id': 1})
| INFO    | warehouse               | Applied 020_dim_customer_scd2.sql -> rows affected per statement: [0, 397]
| INFO    | transform.orders        | Orders: collapsed 12 duplicate order line(s) to the latest version
| WARNING | pipeline                | Orders: 2678 valid, 37 rejected (top reasons: {'invalid_quantity': 8, 'invalid_order_date': 7, 'unknown_product': 7})
| INFO    | warehouse               | dim_date: 90 new calendar day(s)
| INFO    | warehouse               | Applied 030_fact_sales.sql -> rows affected per statement: [2678]
| INFO    | quality.checks          | [PASS] fact_orphan_keys               observed=0 threshold=0
| INFO    | quality.checks          | [PASS] fact_amounts_reconcile         observed=0 threshold=0
| INFO    | quality.checks          | [PASS] fact_valid_measures            observed=0 threshold=0
| INFO    | quality.checks          | [PASS] dim_customer_single_current    observed=0 threshold=0
| INFO    | quality.checks          | [PASS] dim_customer_valid_windows     observed=0 threshold=0
| INFO    | quality.checks          | [PASS] fact_not_empty                 observed=0 threshold=0
| INFO    | quality.checks          | [PASS] reject_rate                    observed=0.0134 threshold=0.05
| INFO    | warehouse               | Watermark for 'orders' advanced to 2026-04-01T02:00:00

================================================================
 Run 85179904-cfa6-47df-aa12-e3d67962d52d  |  SUCCESS  |  1s
================================================================
 source         extracted    rejected
 customers            405           4
 products              66           2
 orders             2,727          37
----------------------------------------------------------------
 target                     rows inserted / updated
 dim_product                                     64
 dim_customer                                   397
 dim_date                                        90
 fact_sales                                   2,678
----------------------------------------------------------------
 quality checks: 7/7 passed
================================================================
```

</details>

<details>
<summary><b>Re-run</b>: idempotent, nothing changes</summary>

```text
 source         extracted    rejected
 customers            405           4
 products              66           2
 orders                 0           0      ← watermark skips everything already loaded
----------------------------------------------------------------
 target                     rows inserted / updated
 dim_product                                      0
 dim_customer                                     0
 dim_date                                         0
 fact_sales                                       0
```

</details>

<details>
<summary><b>Incremental run</b>: April drop with SCD1, SCD2 and late returns</summary>

```text
| INFO    | pipeline   | Orders watermark: only records updated after 2026-04-01 02:00:00
| INFO    | warehouse  | Applied 010_dim_product.sql -> rows affected per statement: [4]        ← 4 price changes (SCD1)
| INFO    | warehouse  | Applied 020_dim_customer_scd2.sql -> rows affected per statement: [17, 17]
| INFO    | pipeline   | dim_customer: 17 customer(s) changed -> new SCD2 versions
| INFO    | warehouse  | Applied 030_fact_sales.sql -> rows affected per statement: [927]       ← new April lines + 40 late returns
| INFO    | warehouse  | Watermark for 'orders' advanced to 2026-05-01T02:00:00
```

</details>

<details open>
<summary><b>Analytics report</b> (<code>etl-pipeline report</code>)</summary>

```text
## Monthly revenue & margin

 year  month month_name  orders  units   revenue   profit  margin_pct
 2026      1    January     453   1269 226733.61 92431.54        40.8
 2026      2   February     425   1252 233201.56 92079.92        39.5
 2026      3      March     461   1326 245215.75 97475.06        39.8
 2026      4      April     457   1316 234846.47 94719.28        40.3

## Category performance

         category  order_lines   revenue    profit  return_rate_pct
   Home & Kitchen         1094 312819.01 121189.98              7.4
           Office          678 245134.35 106807.97              9.1
      Electronics         1020 218876.19  85134.37              7.9
Sports & Outdoors          773 163167.84  63573.48              5.8

## Top 10 customers by lifetime revenue

 revenue_rank customer_id      customer_name        segment  orders  revenue  avg_order_value
            1  CUST-00218 Elizabeth Williams      Corporate       9  8598.70           955.41
            2  CUST-00117        Mei Jackson Small Business      11  8425.99           766.00
            3  CUST-00275     Amanda O'Brien       Consumer       7  6910.28           987.18
          ...
```

</details>

<details>
<summary><b>Quarantine</b>: what got rejected and why</summary>

```sql
SELECT reason, COUNT(*) FROM etl_rejected_records WHERE run_id = :first_run GROUP BY reason ORDER BY 2 DESC;
```

```text
invalid_quantity       8      unknown_status        4
unknown_product        7      invalid_unit_price    4
unknown_customer       7      invalid_email         3
invalid_order_date     7      missing_product_name  1
missing_customer_id    1      invalid_list_price    1
```

Every row keeps its full source payload, for example
`{"order_item_id": "OI-0000419", "order_date": null, ...}`, so it can be fixed upstream and re-sent.

</details>

---

## Project structure

```text
python-sql-etl-pipeline/
├── config/
│   └── pipeline.yaml               # sources, DB URL (${ENV} interpolation), quality thresholds
├── data/raw/                       # synthetic source drops (CRM JSON, ERP CSV, monthly order CSVs)
├── scripts/
│   └── generate_sample_data.py     # deterministic, deliberately messy data generator
├── sql/
│   ├── ddl/                        # staging, star schema, metadata tables
│   ├── transform/                  # 010 SCD1 product · 020 SCD2 customer · 030 fact upsert
│   ├── quality/                    # one file per data quality check
│   └── analytics/                  # ready-to-run business queries
├── src/etl_pipeline/
│   ├── extract/                    # CSV, JSON and paginated REST API extractors
│   ├── transform/                  # cleaning helpers, rule engine, entity transforms, dim_date
│   ├── load/                       # staging loader, warehouse ops, watermarks
│   ├── quality/                    # SQL-driven quality gate
│   ├── pipeline.py                 # orchestration, transaction & audit handling
│   ├── db.py                       # engine, dialect templating, safe SQL execution
│   ├── config.py                   # typed config + env interpolation
│   └── cli.py                      # `etl-pipeline` command
├── tests/                          # unit + end-to-end tests (37)
├── .github/workflows/ci.yml        # lint → test matrix → PostgreSQL integration
├── Dockerfile · docker-compose.yml
├── Makefile · pyproject.toml
└── README.md
```

---

## Configuration

Everything is driven by [`config/pipeline.yaml`](config/pipeline.yaml). Secrets come from environment variables via `${VAR}` / `${VAR:-default}`:

```yaml
database:
  url: ${DATABASE_URL:-sqlite:///warehouse/warehouse.db}

sources:
  customers:
    type: api                                   # csv | json | api
    url: https://crm.example.com/api/v1/customers
    options:
      records_key: data
      page_size: 200
      headers:
        Authorization: "Bearer ${CRM_API_TOKEN}"
  orders:
    type: csv
    path: data/raw/orders/*.csv                 # glob: picks up every new drop
    incremental_column: updated_at

quality:
  max_reject_rate: 0.05                         # fail + roll back above 5% rejects
  fail_on_error: true
```

Switching from SQLite to PostgreSQL only needs one environment variable:

```bash
export DATABASE_URL=postgresql+psycopg://etl:etl@localhost:5432/warehouse
```

---

## Testing & CI/CD

```bash
pytest --cov          # 37 tests: unit tests per helper/transform + end-to-end runs against a real database
ruff check . && ruff format --check .
```

The end-to-end tests in [`tests/test_pipeline.py`](tests/test_pipeline.py) cover the guarantees that matter in production:

- ✅ a full run builds the star schema with correct amounts and profit
- ✅ re-runs are idempotent, and `--full-refresh` reprocesses without creating duplicates
- ✅ customer changes produce SCD2 history, old facts keep the old version, new facts get the new one
- ✅ product changes overwrite in place (SCD1)
- ✅ a late status change updates the existing fact line
- ✅ bad records are quarantined with the right reasons
- ✅ a quality-gate failure rolls back **everything**, and the audit trail still records why

**GitHub Actions** ([`ci.yml`](.github/workflows/ci.yml)) runs three jobs:

```mermaid
flowchart LR
    A["🔍 lint<br/>ruff check + format"] --> B["🧪 test matrix<br/>Python 3.10 · 3.11 · 3.12 · 3.13<br/>SQLite"]
    B --> C["🐘 postgres-integration<br/>Postgres 16 service<br/>baseline → re-run → increment → report"]
```

---

## Design decisions

| Decision | Rationale |
|---|---|
| **Extract as raw strings** | Type coercion at read time silently turns `"N/A"` into `NaN` and `"007"` into `7`. Keeping strings means every parsing decision is explicit, testable and can be quarantined. |
| **Business logic in SQL files** | Analysts and reviewers can read `020_dim_customer_scd2.sql` without knowing Python. The SQL is diff-friendly and portable to any warehouse. |
| **One portable SQL dialect** | `INSERT … ON CONFLICT`, partial unique indexes and window functions work in both SQLite and PostgreSQL. One token (`{{ identity_pk }}`) covers the only DDL difference. |
| **Row hashes for change detection** | Comparing a single MD5 per row is cheaper and less error-prone than comparing N nullable columns in SQL. |
| **Gate inside the transaction** | Quality checks see the data *as it would be committed*. Failing means nothing is published, so there are no half-loaded dashboards. |
| **Audit in a separate transaction** | You get the most diagnostic information from exactly the runs that failed. |
| **Upsert only if newer** | `WHERE fact.source_updated_at < excluded.source_updated_at` makes out-of-order or replayed files harmless. |
| **Deterministic sample data** | A fixed seed means the README numbers, tests and CI all reproduce exactly. |

---

## Taking it to production

This reference build is designed to slot into a larger platform:

- **Orchestration**: `etl-pipeline run` is a single idempotent command with meaningful exit codes. It can be scheduled from Apache Airflow, cron, AWS ECS/Fargate scheduled tasks or Azure Container Apps jobs.
- **Cloud warehouses**: the SQL layer ports to Redshift, Synapse or Snowflake with minimal changes. `ON CONFLICT` becomes `MERGE`.
- **Landing zone**: point CSV/JSON sources at S3 / ADLS / GCS mounts, or add an object-storage extractor next to the existing ones.
- **Alerting**: `etl_run_log` and `etl_quality_results` are ready to feed Slack/Teams alerts or a monitoring dashboard.
- **Scale**: once volumes outgrow pandas, the same architecture maps onto PySpark / Databricks. See our other showcase projects.

---

## About Aitomation

**Aitomation** builds data platforms and automation for growing businesses: ETL/ELT pipelines, cloud data warehouses, orchestration, real-time streaming and analytics.

This project is part of our public showcase series:

| # | Project | Stack |
|---|---|---|
| 1 | **End-to-End ETL Data Pipeline** *(this repo)* | Python · SQL · PostgreSQL |
| 2 | Cloud Data Engineering on AWS | S3 · Glue · Lambda · Redshift |
| 3 | Cloud Data Engineering on Azure | Data Factory · Synapse Analytics |
| 4 | Big Data Processing Pipeline | Databricks · PySpark |
| 5 | Modern Data Warehouse & ELT | Snowflake · dbt |
| 6 | Automated Pipeline Orchestration | Apache Airflow |
| 7 | Real-Time Big Data Pipeline | Apache Kafka · Spark Streaming |

**Need something similar?** [Get in touch at aitomation.com](https://aitomation.com).

---

<div align="center">
<sub>Released under the <a href="LICENSE">MIT License</a> · © 2026 Aitomation</sub>
</div>
