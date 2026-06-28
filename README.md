# Bluestock MF Analytics — Capstone Project

> **End-to-end mutual fund data analytics pipeline**  
> Python · SQLite · Power BI · Pandas · reportlab  
> Sravani Surepalli · BSc Data Science (Graduate 2026), University of Mumbai  
> Bluestock Finserv Internship Programme · Batch 2026

---

## Table of Contents

1. [Project Overview](#project-overview)
2. [Repository Structure](#repository-structure)
3. [Dataset Descriptions](#dataset-descriptions)
4. [Setup Instructions](#setup-instructions)
5. [How to Run the ETL Pipeline](#how-to-run-the-etl-pipeline)
6. [How to Open the Dashboard](#how-to-open-the-dashboard)
7. [Key Deliverables](#key-deliverables)
8. [Self-Review Checklist](#self-review-checklist)
9. [Limitations](#limitations)

---

## Project Overview

Bluestock MF Analytics ingests raw mutual fund data — fund master records, NAV history (including per-scheme NAV files for 5 flagship Bluechip funds), AUM by fund house, SIP inflows, category-wise inflows, industry folio counts, scheme performance, investor transactions, portfolio holdings, and benchmark indices.

The data flows through `scripts/run_pipeline.py` — a single, tested script covering ingestion, cleaning, database loading, EDA, and risk/performance metrics — landing in a cleaned, analysis-ready form (`data/processed/`) and a SQLite database (`data/db/bluestock_mf.db`) queryable via `sql/queries.sql`. `notebooks/` contains the original exploratory development of this same logic. Insights are surfaced through a 5-page Power BI dashboard (`dashboard/bluestock_mf.pbix`) and a 24-page final report (`reports/Final_Report.pdf`).

### Objectives Achieved

| # | Objective | Status |
|---|-----------|--------|
| 1 | Raw data ingestion (10 industry files + 5 per-scheme NAV files) | ✅ |
| 2 | Modular data cleaning (`scripts/run_pipeline.py` → `clean()`) | ✅ |
| 3 | SQLite database, built at runtime (`scripts/run_pipeline.py` → `load()`) | ✅ |
| 4 | Analytical SQL queries (`sql/queries.sql`) | ✅ |
| 5 | Exploratory Data Analysis (`scripts/run_pipeline.py` → `eda()`) | ✅ |
| 6 | Performance & risk analytics — alpha/beta, tracking error, VaR/CVaR (`scripts/run_pipeline.py` → `metrics()`) | ✅ |
| 7 | Power BI 5-page investor analytics dashboard | ✅ |
| 8 | Rule-based fund recommender (`scripts/recommender.py`) | ✅ |

---

## Repository Structure

```
BLUESTOCK_MF_CAPSTONE/
│
├── dashboard/
│   ├── bluestock_mf.pbix                 # Power BI Desktop file
│   ├── Dashboard.pdf                     # Exported PDF of all dashboard pages
│   ├── Page1_Industry_Overview.png       # Dashboard page screenshots
│   ├── Page2_Fund_Performance.png
│   ├── Page3_Investor_Analytics.png
│   ├── Page4_SIP_Market_Trends.png
│   └── Page5_NAV_Details.png
│
├── data/
│   ├── db/
│   │   └── bluestock_mf.db               # SQLite database (built by scripts/run_pipeline.py)
│   ├── processed/                        # Cleaned, analysis-ready CSVs
│   │   ├── alpha_beta.csv
│   │   ├── aum_clean.csv
│   │   ├── benchmark_clean.csv
│   │   ├── category_clean.csv
│   │   ├── folio_clean.csv
│   │   ├── fund_clean.csv
│   │   ├── fund_scorecard.csv
│   │   ├── holdings_clean.csv
│   │   ├── ingestion_metadata.csv
│   │   ├── latest_nav_all_schemes.csv
│   │   ├── nav_clean.csv
│   │   ├── performance_clean.csv
│   │   ├── sip_clean.csv
│   │   ├── tracking_error.csv
│   │   ├── transactions_clean.csv
│   │   └── var_cvar_report.csv
│   └── raw/                              # Original, untouched source files
│       ├── 01_fund_master.csv
│       ├── 02_nav_history.csv
│       ├── 03_aum_by_fund_house.csv
│       ├── 04_monthly_sip_inflows.csv
│       ├── 05_category_inflows.csv
│       ├── 06_industry_folio_count.csv
│       ├── 07_scheme_performance.csv
│       ├── 08_investor_transactions.csv
│       ├── 09_portfolio_holdings.csv
│       ├── 10_benchmark_indices.csv
│       ├── nav_Axis_Bluechip_119092.csv
│       ├── nav_ICICI_Bluechip_120503.csv
│       ├── nav_Kotak_Bluechip_120841.csv
│       ├── nav_Nippon_Large_Cap_118632.csv
│       └── nav_SBI_Bluechip_119551.csv
│
├── notebooks/                            # Exploratory, step-by-step analysis
│   ├── 01_data_ingestion.ipynb
│   ├── 02_data_cleaning.ipynb
│   ├── 03_eda_analysis.ipynb
│   ├── 04_performance_analytics.ipynb
│   └── 05_advanced_analytics.ipynb
│
├── reports/
│   ├── charts/                           # Saved chart images used in the report
│   ├── benchmark_comparison.png
│   ├── data_dictionary.md                # Column-level data dictionary
│   ├── Final_Report.pdf                  # 15–20 page capstone report
│   ├── Presentation.pptx                 # 12-slide capstone presentation
│   └── rolling_sharpe_chart.png
│
├── scripts/                              # Reusable, production-style modules
│   ├── live_nav_fetch.py                 # Optional: pulls live NAV from AMFI
│   ├── recommender.py                    # Rule-based fund recommender
│   └── run_pipeline.py                   # Master script — ingest, clean, load,
│                                          #   EDA, performance metrics, recommend
│
├── sql/
│   ├── queries.sql                       # Analytical SQL queries
│   └── schema.sql                        # Reference schema (tables are actually
│                                          #   created at runtime by run_pipeline.py)
│
├── .gitignore
├── README.md
└── requirements.txt
```

> **Note:** `run_pipeline.py` is a single, self-contained script that covers
> ingestion, cleaning, database loading, EDA, performance/risk metrics
> (alpha, beta, tracking error, VaR/CVaR), and the fund recommender — all six
> stages, fully tested end-to-end against the real `data/raw/` files. There
> is no separate `etl_pipeline.py` or `compute_metrics.py`; that logic lives
> inside `run_pipeline.py` as the `clean()` and `metrics()` functions.
> `notebooks/02_data_cleaning.ipynb` and `notebooks/04_performance_analytics.ipynb`
> contain the original exploratory versions of this same logic.

---

## Dataset Descriptions

### Industry-wide source files (`data/raw/`)

| File | Key Columns | Description |
|------|-------------|--------------|
| `01_fund_master.csv` | `amfi_code`, `fund_house`, `scheme_name`, `category`, `sub_category`, `plan`, `launch_date`, `benchmark`, `expense_ratio_pct`, `fund_manager`, `risk_category` | Scheme master — one row per scheme (40 schemes) |
| `02_nav_history.csv` | `amfi_code`, `date`, `nav` | Daily NAV across all 40 schemes (46,000 rows) |
| `03_aum_by_fund_house.csv` | `date`, `fund_house`, `aum_lakh_crore`, `aum_crore`, `num_schemes` | Quarterly AUM snapshots by fund house |
| `04_monthly_sip_inflows.csv` | `month`, `sip_inflow_crore`, `active_sip_accounts_crore`, `new_sip_accounts_lakh`, `sip_aum_lakh_crore`, `yoy_growth_pct` | Industry-wide monthly SIP inflow totals |
| `05_category_inflows.csv` | `month`, `category`, `net_inflow_crore` | Net inflow by fund category, monthly |
| `06_industry_folio_count.csv` | `month`, `total_folios_crore`, `equity_folios_crore`, `debt_folios_crore`, `hybrid_folios_crore`, `others_folios_crore` | Industry folio count over time |
| `07_scheme_performance.csv` | `amfi_code`, `scheme_name`, `fund_house`, `category`, `return_1yr_pct`, `return_3yr_pct`, `return_5yr_pct`, `alpha`, `beta`, `sharpe_ratio`, `sortino_ratio`, `morningstar_rating` | Trailing returns and risk metrics, 40 schemes |
| `08_investor_transactions.csv` | `investor_id`, `transaction_date`, `amfi_code`, `transaction_type` (SIP/Lumpsum/Redemption), `amount_inr`, `state`, `city`, `city_tier`, `age_group` | Investor-level transaction records (32,778 rows) |
| `09_portfolio_holdings.csv` | `amfi_code`, `stock_symbol`, `stock_name`, `sector`, `weight_pct`, `market_value_cr`, `portfolio_date` | Sector/stock-level portfolio weight by scheme |
| `10_benchmark_indices.csv` | `date`, `index_name`, `close_value` | Multiple indices stacked in one file (NIFTY50, NIFTY100, etc.) — filter by `index_name` before use |

> **`amfi_code` is the universal join key** across `fund_master`, `nav_history`,
> `scheme_performance`, `investor_transactions`, and `portfolio_holdings` —
> verified to be consistent across all five files.

### Per-scheme NAV files (`data/raw/`)

Five flagship Bluechip/Large Cap schemes are tracked individually for benchmarking, alpha/beta, and VaR/CVaR analysis:

| File | scheme_code | Fund (per `01_fund_master.csv`) |
|------|-------------|-----------------------------------|
| `nav_SBI_Bluechip_119551.csv` | 119551 | SBI Mutual Fund |
| `nav_Axis_Bluechip_119092.csv` | 119092 | Axis Mutual Fund |
| `nav_ICICI_Bluechip_120503.csv` | 120503 | ICICI Prudential MF |
| `nav_Kotak_Bluechip_120841.csv` | 120841 | Kotak Mahindra MF |
| `nav_Nippon_Large_Cap_118632.csv` | 118632 | Nippon India MF |

> **Data quality note:** these 5 files each include their own `fund_house`
> and `scheme_category` columns, but those values are **unreliable** —
> verified by cross-checking against `01_fund_master.csv` (e.g. the file
> named `nav_SBI_Bluechip_119551.csv` internally claims `fund_house = Aditya
> Birla Sun Life Mutual Fund`, which is incorrect). `run_pipeline.py`
> ignores these two columns entirely and re-attaches the correct
> `fund_house`/`category` via a join on `scheme_code == amfi_code` against
> `01_fund_master.csv`. Only `date` and `nav` are trusted from these files.

### Cleaned outputs (`data/processed/`)

All files below are written by `scripts/run_pipeline.py` — `clean()` for the
first ten, and the per-scheme NAV merge logic for `latest_nav_all_schemes.csv`.

| File | Stage | Contents |
|------|-------|----------|
| `fund_clean.csv` | clean | Cleaned scheme master (40 rows) |
| `nav_clean.csv` | clean | Cleaned, deduplicated NAV history on `(amfi_code, date)` (46,000 rows) |
| `aum_clean.csv` | clean | Cleaned AUM by fund house |
| `sip_clean.csv` | clean | Cleaned monthly SIP inflow data |
| `category_clean.csv` | clean | Cleaned category-wise inflow data |
| `folio_clean.csv` | clean | Cleaned industry folio counts |
| `performance_clean.csv` | clean | Cleaned scheme performance data |
| `transactions_clean.csv` | clean | Cleaned investor transactions (32,778 rows) |
| `holdings_clean.csv` | clean | Cleaned portfolio holdings on `(amfi_code, stock_symbol)` (322 rows) |
| `benchmark_clean.csv` | clean | Cleaned benchmark index data, deduped on `(index_name, date)` |
| `latest_nav_all_schemes.csv` | clean | All 5 per-scheme NAV files combined, with correct `fund_house`/`category` re-attached from `fund_master` (16,707 rows) |
| `ingestion_metadata.csv` | ingest | Row/column counts per source file, for audit purposes |

### Generated reports (`reports/`)

Written by the `metrics()` and `recommend()` functions in `run_pipeline.py`:

| File | Contents |
|------|----------|
| `eda_summary.json` | Summary statistics — NAV, AUM, SIP, transactions |
| `fund_scorecard.csv` | Combined alpha/beta/tracking-error/VaR/CVaR + trailing returns, per scheme |
| `alpha_beta.csv` | Annualised alpha and beta vs. NIFTY50, per scheme |
| `tracking_error.csv` | Annualised tracking error vs. NIFTY50, per scheme |
| `var_cvar_report.csv` | Historical 95% VaR and CVaR, per scheme |
| `recommendations.json` | Rule-based fund allocation + top schemes by category |

---

## Setup Instructions

### Prerequisites

- Python 3.10 or later
- Git 2.x
- Power BI Desktop (free) — for the dashboard

### 1. Clone the repository

```powershell
git clone https://github.com/sravanisurepalli21/bluestock_mf_capstone.git
cd bluestock_mf_capstone
```

### 2. Create a virtual environment (recommended)

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

### 3. Install Python dependencies

```powershell
pip install -r requirements.txt
```

**requirements.txt** (minimum):
```
pandas>=2.0
numpy
reportlab
matplotlib
seaborn
openpyxl
requests
```

> The database layer uses Python's built-in `sqlite3` module — no SQLAlchemy
> or other ORM is required. `requests` is used by `scripts/live_nav_fetch.py`
> for optional live NAV pulls.

---

## How to Run the ETL Pipeline

### Run the full pipeline

```powershell
python scripts/run_pipeline.py
```

This runs all six stages in order — `ingest` → `clean` → `load` → `eda` →
`metrics` → `recommend` — and prints a log line for every file loaded,
every dataset cleaned, every database table written, and every scheme's
computed alpha/beta/VaR. Paths are resolved relative to the project root
regardless of whether you run this from the root or from inside `scripts/`.

### Run a single stage

```powershell
python scripts/run_pipeline.py --stage ingest    # load raw CSVs only
python scripts/run_pipeline.py --stage clean     # clean + write data/processed/*.csv
python scripts/run_pipeline.py --stage load      # build the SQLite database
python scripts/run_pipeline.py --stage eda       # EDA summary only
python scripts/run_pipeline.py --stage metrics   # alpha/beta/VaR/CVaR only
python scripts/run_pipeline.py --stage recommend # fund recommender only
```

### Skip a stage

```powershell
python scripts/run_pipeline.py --skip recommend  # run everything except the recommender
```

### Optional supporting scripts

```powershell
python scripts/live_nav_fetch.py     # optional: pull a live NAV snapshot from AMFI
python scripts/recommender.py        # standalone recommender (same logic as --stage recommend)
```

### Step through the notebooks (exploratory walkthrough)

The notebooks contain the original exploratory development of this same
logic, narrated step by step:

```
notebooks/01_data_ingestion.ipynb
notebooks/02_data_cleaning.ipynb
notebooks/03_eda_analysis.ipynb
notebooks/04_performance_analytics.ipynb
notebooks/05_advanced_analytics.ipynb
```

### Querying the database

Once `data/db/bluestock_mf.db` is built, run the prepared analytical queries:

```powershell
sqlite3 data/db/bluestock_mf.db < sql/queries.sql
```

The schema itself (table definitions) is documented in `sql/schema.sql`.

### Expected outputs after a full run

| Path | Contents |
|------|----------|
| `data/processed/*_clean.csv` | Cleaned, deduplicated CSVs (one per raw source, 11 files total) |
| `data/processed/ingestion_metadata.csv` | Row/column counts per source file |
| `data/db/bluestock_mf.db` | SQLite database — 11 tables, ready for `sql/queries.sql` |
| `reports/eda_summary.json` | EDA summary statistics |
| `reports/fund_scorecard.csv` | Combined per-scheme return/risk scorecard |
| `reports/alpha_beta.csv` | Alpha/beta vs. NIFTY50, per scheme |
| `reports/tracking_error.csv` | Tracking error vs. NIFTY50, per scheme |
| `reports/var_cvar_report.csv` | Historical 95% VaR/CVaR, per scheme |
| `reports/recommendations.json` | Rule-based fund allocation + top schemes |

---

## How to Open the Dashboard

### Power BI Desktop (local)

1. Open **Power BI Desktop** (free download from [powerbi.microsoft.com](https://powerbi.microsoft.com/desktop/)).
2. File → Open → `dashboard/bluestock_mf.pbix`
3. If prompted to update the data source path, go to:
   **Home → Transform Data → Data Source Settings** → point it to your local `data/db/bluestock_mf.db`.
4. Click **Refresh** to reload data from the SQLite database.

### Dashboard pages

| Page | File | Contents |
|------|------|----------|
| 1 | `Page1_Industry_Overview.png` | Total AUM, total folios, active schemes, SIP inflows, AUM by AMC |
| 2 | `Page2_Fund_Performance.png` | Return vs. risk bubble chart, NAV vs. Nifty 50, scheme scorecard table |
| 3 | `Page3_Investor_Analytics.png` | Transaction amount by state, SIP/Lumpsum/Redemption split, age group analysis |
| 4 | `Page4_SIP_Market_Trends.png` | SIP inflows vs. Nifty 50, category-wise net inflow |
| 5 | `Page5_NAV_Details.png` | NAV detail drill-through by fund |

A full PDF export of all 5 pages is also available at `dashboard/Dashboard.pdf` for quick reference without opening Power BI.

### Power BI Service (optional — requires Pro licence for sharing)

If published to Power BI Service, the live dashboard URL will be added here:
`https://app.powerbi.com/...` *(add URL after publishing)*

---

## Key Deliverables

| Deliverable | File / Location |
|-------------|-----------------|
| Final Report (PDF) | `reports/Final_Report.pdf` |
| Presentation (PPTX) | `reports/Presentation.pptx` |
| SQLite Database | `data/db/bluestock_mf.db` |
| Schema DDL | `sql/schema.sql` |
| Analytical SQL | `sql/queries.sql` |
| Data Dictionary | `reports/data_dictionary.md` |
| Power BI Dashboard | `dashboard/bluestock_mf.pbix` |
| Dashboard PDF Export | `dashboard/Dashboard.pdf` |
| Master Pipeline Script | `scripts/run_pipeline.py` (ingest, clean, load, EDA, metrics, recommend) |
| Recommender / Live NAV | `scripts/recommender.py`, `scripts/live_nav_fetch.py` |
| Exploratory Notebooks | `notebooks/01`–`05_*.ipynb` |

**GitHub Release Tag:** `v1.0`

---

## Self-Review Checklist

- [ ] All 8 objectives completed
- [ ] All deliverables present and committed (report, presentation, scripts, notebooks, database, dashboard)
- [x] `python scripts/run_pipeline.py` runs without errors on a clean clone — verified end-to-end against the real `data/raw/` files (15/15 loaded, all 6 stages pass)
- [ ] Dashboard loads and all 5 pages render correctly in Power BI Desktop
- [ ] Final_Report.pdf is professional quality (15–20 pages)
- [x] No debug print statements in `run_pipeline.py` (logging module used throughout)
- [x] Every function in `run_pipeline.py` has a docstring
- [ ] `.gitignore` excludes `.venv/`, `__pycache__/`, `*.pyc`, and any local `.db` temp files
- [ ] Git history has clean, day-wise commits
- [ ] Repo tagged `v1.0` and pushed to `main`

---

## Limitations

- **Per-scheme NAV file metadata is unreliable** — the 5 per-scheme NAV files (`nav_SBI_Bluechip_119551.csv`, etc.) contain `fund_house`/`scheme_category` columns that do not match `01_fund_master.csv` (e.g. the SBI file internally claims `fund_house = Aditya Birla Sun Life`). `run_pipeline.py` works around this by ignoring those two columns and re-joining the correct metadata from `fund_master` on `scheme_code == amfi_code` — but the discrepancy itself is worth flagging to whoever generated the source data.
- **Benchmark file mixes multiple indices** — `10_benchmark_indices.csv` stacks NIFTY50, NIFTY100, etc. in a single table with overlapping dates. `run_pipeline.py` filters to NIFTY50 specifically before computing alpha/beta/tracking error; a future version could compute these metrics against multiple benchmarks.
- **Per-scheme NAV scope** — individual alpha/beta/VaR tracking covers 5 flagship Bluechip/Large Cap funds only; broader scheme-level NAV detail for all 40 schemes relies on the aggregated `02_nav_history.csv`, which does not yet feed into the metrics stage.
- **No live production feed by default** — `live_nav_fetch.py` is optional and pulls a NAV snapshot on demand; it is not scheduled to run continuously.
- **SQLite scalability** — appropriate for this capstone; production deployment would migrate to PostgreSQL or a managed cloud warehouse.
- **Recommender simplicity** — `recommender.py` / the `recommend()` stage uses rule-based allocation logic, not portfolio optimisation or ML-based personalisation.
- **Risk metrics window** — alpha/beta, tracking error, and VaR/CVaR are computed over whatever historical window exists in the per-scheme NAV files; results will shift as more history accumulates.

---

*Sravani Surepalli · Bluestock Finserv Pvt. Ltd. · Data Analytics Internship · 2026*
