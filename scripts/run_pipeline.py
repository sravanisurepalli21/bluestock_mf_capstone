"""
run_pipeline.py
================
Master execution script for the Bluestock MF Analytics capstone project.

This is the SINGLE entry point for the entire pipeline: ingestion, cleaning,
database loading, EDA, performance/risk analytics, and fund recommendation.
Every stage is a documented function in this one file — no separate
clean_data.py or load_db.py is required.

Run from anywhere — paths are resolved relative to the project root
(one level above this file), not the current working directory:

    python scripts/run_pipeline.py        # from the project root
    python run_pipeline.py                # from inside scripts/ — also works

Pipeline stages (run in order by default)
-------------------------------------------
    1. ingest    - Load all raw CSVs from data/raw/
    2. clean     - Standardise columns, dates, numerics; deduplicate
    3. load      - Write cleaned tables into a SQLite database
    4. eda       - Generate exploratory summary statistics
    5. metrics   - Alpha/beta, tracking error, VaR/CVaR, fund scorecard
    6. recommend - Rule-based fund recommendation engine

Usage
-----
    python scripts/run_pipeline.py                  # run the full pipeline
    python scripts/run_pipeline.py --stage clean    # run a single named stage
    python scripts/run_pipeline.py --skip recommend # run all but one stage

Data dictionary (verified against the real source files)
-----------------------------------------------------------
    01_fund_master.csv          amfi_code, fund_house, scheme_name, category,
                                 sub_category, plan, launch_date, benchmark,
                                 expense_ratio_pct, exit_load_pct,
                                 min_sip_amount, min_lumpsum_amount,
                                 fund_manager, risk_category, sebi_category_code
    02_nav_history.csv          amfi_code, date, nav
    03_aum_by_fund_house.csv    date, fund_house, aum_lakh_crore, aum_crore,
                                 num_schemes
    04_monthly_sip_inflows.csv  month (YYYY-MM), sip_inflow_crore,
                                 active_sip_accounts_crore, new_sip_accounts_lakh,
                                 sip_aum_lakh_crore, yoy_growth_pct
    05_category_inflows.csv     month (YYYY-MM), category, net_inflow_crore
    06_industry_folio_count.csv month (YYYY-MM), total_folios_crore,
                                 equity_folios_crore, debt_folios_crore,
                                 hybrid_folios_crore, others_folios_crore
    07_scheme_performance.csv   amfi_code, scheme_name, fund_house, category,
                                 plan, return_1yr_pct, return_3yr_pct,
                                 return_5yr_pct, benchmark_3yr_pct, alpha, beta,
                                 sharpe_ratio, sortino_ratio, std_dev_ann_pct,
                                 max_drawdown_pct, aum_crore, expense_ratio_pct,
                                 morningstar_rating, risk_grade
    08_investor_transactions.csv investor_id, transaction_date, amfi_code,
                                 transaction_type (SIP/Lumpsum/Redemption),
                                 amount_inr, state, city, city_tier, age_group,
                                 gender, annual_income_lakh, payment_mode,
                                 kyc_status
    09_portfolio_holdings.csv   amfi_code, stock_symbol, stock_name, sector,
                                 weight_pct, market_value_cr,
                                 current_price_inr, portfolio_date
    10_benchmark_indices.csv    date, index_name, close_value
                                 (multiple indices stacked — filter before use)
    nav_<Scheme>_<code>.csv     date (DD-MM-YYYY), nav, scheme_code, scheme_name,
                                 fund_house, scheme_type, scheme_category,
                                 date_parsed (YYYY-MM-DD), nav_float
                                 NOTE: fund_house/scheme_category in these 5
                                 per-scheme files are unreliable/scrambled —
                                 the trustworthy metadata is in 01_fund_master.csv,
                                 joined on scheme_code == amfi_code.
"""

import argparse
import json
import logging
import sqlite3
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

# ── Logging setup ──────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("pipeline")

# ── Paths ──────────────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent

RAW_DIR     = PROJECT_ROOT / "data" / "raw"
PROC_DIR    = PROJECT_ROOT / "data" / "processed"
DB_DIR      = PROJECT_ROOT / "data" / "db"
REPORTS_DIR = PROJECT_ROOT / "reports"

for d in (PROC_DIR, DB_DIR, REPORTS_DIR):
    d.mkdir(parents=True, exist_ok=True)

DB_PATH = DB_DIR / "bluestock_mf.db"


# ════════════════════════════════════════════════════════════════════════════
# STAGE 1 — INGEST
# ════════════════════════════════════════════════════════════════════════════

INDUSTRY_FILES = {
    "fund_master":          "01_fund_master.csv",
    "nav_history":          "02_nav_history.csv",
    "aum_by_fund_house":    "03_aum_by_fund_house.csv",
    "monthly_sip_inflows":  "04_monthly_sip_inflows.csv",
    "category_inflows":     "05_category_inflows.csv",
    "industry_folio_count": "06_industry_folio_count.csv",
    "scheme_performance":   "07_scheme_performance.csv",
    "investor_transactions":"08_investor_transactions.csv",
    "portfolio_holdings":   "09_portfolio_holdings.csv",
    "benchmark_indices":    "10_benchmark_indices.csv",
}

# Per-scheme NAV files for the 5 individually tracked Bluechip / Large Cap
# funds. scheme_code (== amfi_code in the industry files) is the reliable
# join key; the fund_house/scheme_category columns INSIDE these files are
# not trustworthy (see module docstring) and are dropped during cleaning.
PER_SCHEME_NAV_FILES = {
    119551: "nav_SBI_Bluechip_119551.csv",
    119092: "nav_Axis_Bluechip_119092.csv",
    120503: "nav_ICICI_Bluechip_120503.csv",
    120841: "nav_Kotak_Bluechip_120841.csv",
    118632: "nav_Nippon_Large_Cap_118632.csv",
}


def ingest() -> dict:
    """Load every raw CSV (industry-wide + per-scheme NAV) from data/raw/.

    Missing files are logged as warnings and skipped rather than raising,
    so the pipeline can still proceed with whatever data is available.
    Also writes data/processed/ingestion_metadata.csv with a row count
    per file.

    Returns
    -------
    dict
        Mapping of logical name (industry file key, or integer scheme_code
        for per-scheme NAV files) to the loaded DataFrame.
    """
    log.info("Stage 1/6 — Ingest: reading industry files + per-scheme NAV files")
    frames: dict = {}
    metadata_rows = []

    for key, filename in INDUSTRY_FILES.items():
        path = RAW_DIR / filename
        if not path.exists():
            log.warning("  missing: %s — skipping", path)
            continue
        df = pd.read_csv(path, low_memory=False)
        log.info("  loaded %-24s (%-28s) %6d rows x %2d cols", key, filename, *df.shape)
        frames[key] = df
        metadata_rows.append({"file": filename, "logical_name": key, "rows": len(df), "cols": df.shape[1]})

    for scheme_code, filename in PER_SCHEME_NAV_FILES.items():
        path = RAW_DIR / filename
        if not path.exists():
            log.warning("  missing: %s — skipping", path)
            continue
        df = pd.read_csv(path, low_memory=False)
        log.info("  loaded %-24s (%-28s) %6d rows x %2d cols", f"nav_{scheme_code}", filename, *df.shape)
        frames[scheme_code] = df
        metadata_rows.append({"file": filename, "logical_name": f"nav_{scheme_code}", "rows": len(df), "cols": df.shape[1]})

    total_expected = len(INDUSTRY_FILES) + len(PER_SCHEME_NAV_FILES)
    log.info("Ingest complete: %d/%d files loaded", len(frames), total_expected)

    if metadata_rows:
        pd.DataFrame(metadata_rows).to_csv(PROC_DIR / "ingestion_metadata.csv", index=False)

    return frames


# ════════════════════════════════════════════════════════════════════════════
# STAGE 2 — CLEAN
# ════════════════════════════════════════════════════════════════════════════

def coerce_numeric(series: pd.Series) -> pd.Series:
    """Strip currency/percent symbols and coerce a column to float.

    Parameters
    ----------
    series:
        Raw string/object column.

    Returns
    -------
    pd.Series
        Float64 series; unparseable values become NaN.
    """
    cleaned = (
        series.astype(str)
        .str.replace(r"[₹RsINR,%\s]", "", regex=True)
        .str.strip()
    )
    return pd.to_numeric(cleaned, errors="coerce")


def deduplicate(df: pd.DataFrame, subset: list[str]) -> pd.DataFrame:
    """Drop duplicate rows on a composite key, keeping the latest record.

    Parameters
    ----------
    df:
        Input DataFrame.
    subset:
        Columns forming the composite dedup key. Columns absent from
        *df* are ignored rather than raising a KeyError. If none of the
        requested columns are present, deduplication is skipped entirely
        (rather than silently deduping on whatever happens to remain,
        which can collapse unrelated rows together).

    Returns
    -------
    pd.DataFrame
        Deduplicated, index-reset DataFrame.
    """
    valid_keys = [k for k in subset if k in df.columns]
    if not valid_keys:
        if subset:
            log.warning(
                "    none of the requested dedup keys %s exist in columns %s — "
                "skipping deduplication to avoid collapsing unrelated rows",
                subset, list(df.columns)
            )
        return df
    if len(valid_keys) < len(subset):
        log.warning(
            "    dedup key(s) %s missing from columns; deduping on %s instead of full key %s",
            [k for k in subset if k not in df.columns], valid_keys, subset
        )
    before = len(df)
    df = df.drop_duplicates(subset=valid_keys, keep="last").reset_index(drop=True)
    dropped = before - len(df)
    if dropped:
        log.info("    deduped on %s: removed %d row(s)", valid_keys, dropped)
    if valid_keys and len(df) < before * 0.5:
        log.warning(
            "    dedup on %s removed more than half the rows (%d -> %d) — "
            "verify this key is actually unique per logical record",
            valid_keys, before, len(df)
        )
    return df


def _clean_fund_master(df: pd.DataFrame) -> pd.DataFrame:
    """Clean 01_fund_master.csv.

    Real columns: amfi_code, fund_house, scheme_name, category, sub_category,
    plan, launch_date, benchmark, expense_ratio_pct, exit_load_pct,
    min_sip_amount, min_lumpsum_amount, fund_manager, risk_category,
    sebi_category_code.
    """
    df = df.copy()
    df["launch_date"] = pd.to_datetime(df["launch_date"], errors="coerce")
    for col in ("expense_ratio_pct", "exit_load_pct", "min_sip_amount", "min_lumpsum_amount"):
        if col in df.columns:
            df[col] = coerce_numeric(df[col])
    return deduplicate(df, ["amfi_code"])


def _clean_nav_history(df: pd.DataFrame) -> pd.DataFrame:
    """Clean 02_nav_history.csv. Real columns: amfi_code, date, nav."""
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d", errors="coerce")
    df["nav"] = coerce_numeric(df["nav"]).clip(lower=0.01)
    df = deduplicate(df, ["amfi_code", "date"])
    df = df.sort_values(["amfi_code", "date"]).reset_index(drop=True)
    df["nav"] = df.groupby("amfi_code")["nav"].ffill()
    return df


def _clean_aum(df: pd.DataFrame) -> pd.DataFrame:
    """Clean 03_aum_by_fund_house.csv.

    Real columns: date, fund_house, aum_lakh_crore, aum_crore, num_schemes.
    Granularity is quarterly snapshots per fund house, NOT daily.
    """
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d", errors="coerce")
    for col in ("aum_lakh_crore", "aum_crore"):
        if col in df.columns:
            df[col] = coerce_numeric(df[col]).clip(lower=0)
    if "num_schemes" in df.columns:
        df["num_schemes"] = pd.to_numeric(df["num_schemes"], errors="coerce")
    return deduplicate(df, ["fund_house", "date"])


def _clean_sip_inflows(df: pd.DataFrame) -> pd.DataFrame:
    """Clean 04_monthly_sip_inflows.csv.

    Real columns: month (YYYY-MM), sip_inflow_crore, active_sip_accounts_crore,
    new_sip_accounts_lakh, sip_aum_lakh_crore, yoy_growth_pct.
    """
    df = df.copy()
    df["month"] = pd.to_datetime(df["month"], format="%Y-%m", errors="coerce")
    numeric_cols = [
        "sip_inflow_crore", "active_sip_accounts_crore",
        "new_sip_accounts_lakh", "sip_aum_lakh_crore", "yoy_growth_pct",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = coerce_numeric(df[col])
    return deduplicate(df, ["month"])


def _clean_category_inflows(df: pd.DataFrame) -> pd.DataFrame:
    """Clean 05_category_inflows.csv. Real columns: month (YYYY-MM), category, net_inflow_crore."""
    df = df.copy()
    df["month"] = pd.to_datetime(df["month"], format="%Y-%m", errors="coerce")
    df["net_inflow_crore"] = coerce_numeric(df["net_inflow_crore"])
    return deduplicate(df, ["category", "month"])


def _clean_folio_count(df: pd.DataFrame) -> pd.DataFrame:
    """Clean 06_industry_folio_count.csv.

    Real columns: month (YYYY-MM), total_folios_crore, equity_folios_crore,
    debt_folios_crore, hybrid_folios_crore, others_folios_crore.
    """
    df = df.copy()
    df["month"] = pd.to_datetime(df["month"], format="%Y-%m", errors="coerce")
    for col in df.columns:
        if col.endswith("_crore"):
            df[col] = coerce_numeric(df[col]).clip(lower=0)
    return deduplicate(df, ["month"])


def _clean_scheme_performance(df: pd.DataFrame) -> pd.DataFrame:
    """Clean 07_scheme_performance.csv.

    Real columns: amfi_code, scheme_name, fund_house, category, plan,
    return_1yr_pct, return_3yr_pct, return_5yr_pct, benchmark_3yr_pct,
    alpha, beta, sharpe_ratio, sortino_ratio, std_dev_ann_pct,
    max_drawdown_pct, aum_crore, expense_ratio_pct, morningstar_rating,
    risk_grade.
    """
    df = df.copy()
    numeric_cols = [
        "return_1yr_pct", "return_3yr_pct", "return_5yr_pct", "benchmark_3yr_pct",
        "alpha", "beta", "sharpe_ratio", "sortino_ratio", "std_dev_ann_pct",
        "max_drawdown_pct", "aum_crore", "expense_ratio_pct",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = coerce_numeric(df[col])
    if "morningstar_rating" in df.columns:
        df["morningstar_rating"] = pd.to_numeric(df["morningstar_rating"], errors="coerce").clip(1, 5).round()
    return deduplicate(df, ["amfi_code"])


def _clean_investor_transactions(df: pd.DataFrame) -> pd.DataFrame:
    """Clean 08_investor_transactions.csv.

    Real columns: investor_id, transaction_date, amfi_code, transaction_type
    (SIP/Lumpsum/Redemption), amount_inr, state, city, city_tier, age_group,
    gender, annual_income_lakh, payment_mode, kyc_status.
    """
    df = df.copy()
    df["transaction_date"] = pd.to_datetime(df["transaction_date"], format="%Y-%m-%d", errors="coerce")
    df["amount_inr"] = coerce_numeric(df["amount_inr"])
    df = df[df["amount_inr"] > 0].copy()
    df["transaction_type"] = df["transaction_type"].str.strip().str.title()
    if "annual_income_lakh" in df.columns:
        df["annual_income_lakh"] = coerce_numeric(df["annual_income_lakh"])
    # No single-column transaction ID exists; the natural key is the full
    # combination of investor, scheme, date, and amount.
    dedup_keys = ["investor_id", "amfi_code", "transaction_date", "amount_inr"]
    return deduplicate(df, dedup_keys)


def _clean_holdings(df: pd.DataFrame) -> pd.DataFrame:
    """Clean 09_portfolio_holdings.csv.

    Real columns: amfi_code, stock_symbol, stock_name, sector, weight_pct,
    market_value_cr, current_price_inr, portfolio_date.
    """
    df = df.copy()
    if "portfolio_date" in df.columns:
        df["portfolio_date"] = pd.to_datetime(df["portfolio_date"], format="%Y-%m-%d", errors="coerce")
    for col in ("weight_pct", "market_value_cr", "current_price_inr"):
        if col in df.columns:
            df[col] = coerce_numeric(df[col])
    return deduplicate(df, ["amfi_code", "stock_symbol"])


def _clean_benchmark(df: pd.DataFrame) -> pd.DataFrame:
    """Clean 10_benchmark_indices.csv.

    Real columns: date, index_name, close_value. The file stacks multiple
    indices (e.g. NIFTY50, NIFTY100) — dedup key MUST include index_name,
    or rows from different indices on the same date will be collapsed
    into one.
    """
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d", errors="coerce")
    df["close_value"] = coerce_numeric(df["close_value"])
    return deduplicate(df, ["index_name", "date"])


CLEANERS = {
    "fund_master":           _clean_fund_master,
    "nav_history":           _clean_nav_history,
    "aum_by_fund_house":     _clean_aum,
    "monthly_sip_inflows":   _clean_sip_inflows,
    "category_inflows":      _clean_category_inflows,
    "industry_folio_count":  _clean_folio_count,
    "scheme_performance":    _clean_scheme_performance,
    "investor_transactions": _clean_investor_transactions,
    "portfolio_holdings":    _clean_holdings,
    "benchmark_indices":     _clean_benchmark,
}

OUTPUT_FILENAMES = {
    "fund_master":           "fund_clean.csv",
    "nav_history":           "nav_clean.csv",
    "aum_by_fund_house":     "aum_clean.csv",
    "monthly_sip_inflows":   "sip_clean.csv",
    "category_inflows":      "category_clean.csv",
    "industry_folio_count":  "folio_clean.csv",
    "scheme_performance":    "performance_clean.csv",
    "investor_transactions": "transactions_clean.csv",
    "portfolio_holdings":    "holdings_clean.csv",
    "benchmark_indices":     "benchmark_clean.csv",
}


def _clean_per_scheme_nav(df: pd.DataFrame, scheme_code: int) -> pd.DataFrame:
    """Clean one per-scheme NAV file, ignoring its unreliable metadata columns.

    The 5 per-scheme NAV files include fund_house/scheme_category columns
    that do not match the official metadata in 01_fund_master.csv (verified
    by cross-checking — e.g. the SBI Bluechip file claims fund_house =
    "Aditya Birla Sun Life"). Only scheme_code, date, and nav are trusted
    from these files; correct fund_house/category are re-attached later
    in clean() via a join against fund_master on scheme_code == amfi_code.

    Parameters
    ----------
    df:
        Raw per-scheme NAV DataFrame.
    scheme_code:
        The AMFI/scheme code this file belongs to (from PER_SCHEME_NAV_FILES).

    Returns
    -------
    pd.DataFrame
        Columns: scheme_code, date, nav — deduplicated and forward-filled.
    """
    out = pd.DataFrame({
        "scheme_code": scheme_code,
        # date_parsed is already YYYY-MM-DD in the source file; prefer it
        # over re-parsing the DD-MM-YYYY 'date' column to avoid ambiguity.
        "date": pd.to_datetime(df["date_parsed"], format="%Y-%m-%d", errors="coerce")
                 if "date_parsed" in df.columns
                 else pd.to_datetime(df["date"], format="%d-%m-%Y", errors="coerce"),
        "nav": coerce_numeric(df["nav_float"] if "nav_float" in df.columns else df["nav"]).clip(lower=0.01),
    })
    out = deduplicate(out, ["scheme_code", "date"])
    out = out.sort_values(["scheme_code", "date"]).reset_index(drop=True)
    out["nav"] = out["nav"].ffill()
    return out


def clean(raw_frames: dict) -> dict[str, pd.DataFrame]:
    """Apply per-domain cleaning rules and persist results to data/processed/.

    Per-scheme NAV files (keyed by integer scheme_code in raw_frames) are
    cleaned, concatenated, and joined back to fund_master for correct
    fund_house/category metadata, producing latest_nav_all_schemes.csv.

    Parameters
    ----------
    raw_frames:
        Output of ingest() — mapping of logical name -> raw DataFrame for
        industry files, or integer scheme_code -> raw DataFrame for the 5
        per-scheme NAV files.

    Returns
    -------
    dict[str, pd.DataFrame]
        Mapping of logical name -> cleaned DataFrame. Per-scheme NAV data
        is available under the key "latest_nav_all_schemes".
    """
    log.info("Stage 2/6 — Clean: processing %d source(s)", len(raw_frames))
    cleaned: dict[str, pd.DataFrame] = {}
    per_scheme_frames = []

    for key, raw_df in raw_frames.items():
        if key in CLEANERS:
            try:
                clean_df = CLEANERS[key](raw_df)
            except Exception as exc:
                log.error("  failed to clean %s: %s", key, exc, exc_info=True)
                continue
            out_path = PROC_DIR / OUTPUT_FILENAMES[key]
            clean_df.to_csv(out_path, index=False, encoding="utf-8")
            log.info("  cleaned %-24s -> %6d rows -> %s", key, len(clean_df), out_path.name)
            cleaned[key] = clean_df
        elif isinstance(key, int) and key in PER_SCHEME_NAV_FILES:
            try:
                clean_df = _clean_per_scheme_nav(raw_df, key)
            except Exception as exc:
                log.error("  failed to clean per-scheme NAV for %s: %s", key, exc, exc_info=True)
                continue
            per_scheme_frames.append(clean_df)
            log.info("  cleaned %-24s -> %6d rows", f"nav_{key}", len(clean_df))

    if per_scheme_frames:
        latest_nav = pd.concat(per_scheme_frames, ignore_index=True)
        # Re-attach correct scheme metadata from fund_master rather than
        # trusting the unreliable fund_house/scheme_category columns that
        # shipped inside the per-scheme files.
        if "fund_master" in cleaned:
            meta = cleaned["fund_master"][["amfi_code", "scheme_name", "fund_house", "category"]].rename(
                columns={"amfi_code": "scheme_code"}
            )
            latest_nav = latest_nav.merge(meta, on="scheme_code", how="left")
        out_path = PROC_DIR / "latest_nav_all_schemes.csv"
        latest_nav.to_csv(out_path, index=False, encoding="utf-8")
        log.info("  wrote combined %-24s -> %6d rows -> %s", "latest_nav_all_schemes", len(latest_nav), out_path.name)
        cleaned["latest_nav_all_schemes"] = latest_nav

    log.info("Clean complete: %d dataset(s) written to %s", len(cleaned), PROC_DIR)
    return cleaned


# ════════════════════════════════════════════════════════════════════════════
# STAGE 3 — LOAD  (SQLite database)
# ════════════════════════════════════════════════════════════════════════════

TABLE_MAP = {
    "fund_master":            "dim_scheme",
    "nav_history":            "fact_nav",
    "aum_by_fund_house":      "fact_aum",
    "monthly_sip_inflows":    "fact_sip_inflows",
    "category_inflows":       "fact_category_inflows",
    "industry_folio_count":   "fact_folio_count",
    "scheme_performance":     "fact_performance",
    "investor_transactions":  "fact_transactions",
    "portfolio_holdings":     "fact_holdings",
    "benchmark_indices":      "dim_benchmark",
    "latest_nav_all_schemes": "fact_nav_per_scheme",
}


def load(cleaned_frames: dict[str, pd.DataFrame]) -> None:
    """Write cleaned DataFrames into the SQLite database at data/db/bluestock_mf.db.

    Each table is fully replaced on every run (idempotent reload), and an
    index is created on the most common join/filter column where present.
    Datetime columns are converted to ISO date strings before writing,
    since SQLite has no native date type.

    Parameters
    ----------
    cleaned_frames:
        Output of clean() — mapping of logical name -> cleaned DataFrame.
    """
    log.info("Stage 3/6 — Load: writing tables to %s", DB_PATH)
    conn = sqlite3.connect(DB_PATH)
    try:
        for key, df in cleaned_frames.items():
            table = TABLE_MAP.get(key)
            if table is None:
                continue
            df_to_write = df.copy()
            for col in df_to_write.columns:
                if pd.api.types.is_datetime64_any_dtype(df_to_write[col]):
                    df_to_write[col] = df_to_write[col].dt.strftime("%Y-%m-%d")
            df_to_write.to_sql(table, conn, if_exists="replace", index=False)
            log.info("  wrote table %-26s (%d rows)", table, len(df_to_write))
            for idx_col in ("amfi_code", "scheme_code", "date", "month", "index_name", "fund_house"):
                if idx_col in df_to_write.columns:
                    conn.execute(
                        f"CREATE INDEX IF NOT EXISTS idx_{table}_{idx_col} "
                        f"ON {table}({idx_col})"
                    )
        conn.commit()
    finally:
        conn.close()
    log.info("Load complete: database ready at %s", DB_PATH)


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    """Return True if *table* exists in the connected SQLite database."""
    tables = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table'", conn)["name"].tolist()
    return table in tables


# ════════════════════════════════════════════════════════════════════════════
# STAGE 4 — EDA
# ════════════════════════════════════════════════════════════════════════════

def eda() -> dict:
    """Compute summary statistics across NAV, AUM, SIP, and transaction data.

    Reads directly from the SQLite database produced by load(). Writes a
    JSON summary to reports/eda_summary.json and returns the same dict.

    Returns
    -------
    dict
        Summary statistics keyed by analytical domain. Empty if the
        database has not yet been built.
    """
    log.info("Stage 4/6 — EDA: computing summary statistics")
    if not DB_PATH.exists():
        log.warning("Database not found at %s — run the 'load' stage first", DB_PATH)
        return {}

    conn = sqlite3.connect(DB_PATH)
    summary: dict = {}
    try:
        if _table_exists(conn, "fact_nav"):
            nav = pd.read_sql("SELECT nav FROM fact_nav", conn)
            summary["nav"] = {
                "mean": float(nav["nav"].mean()), "median": float(nav["nav"].median()),
                "std": float(nav["nav"].std()), "min": float(nav["nav"].min()),
                "max": float(nav["nav"].max()),
            }
        if _table_exists(conn, "fact_aum"):
            aum_df = pd.read_sql("SELECT aum_crore, aum_lakh_crore FROM fact_aum", conn)
            summary["aum"] = {
                "total_crore": float(aum_df["aum_crore"].sum()),
                "mean_crore": float(aum_df["aum_crore"].mean()),
            }
        if _table_exists(conn, "fact_sip_inflows"):
            sip_df = pd.read_sql("SELECT sip_inflow_crore FROM fact_sip_inflows", conn)
            summary["sip"] = {
                "total": float(sip_df["sip_inflow_crore"].sum()),
                "mean": float(sip_df["sip_inflow_crore"].mean()),
                "max": float(sip_df["sip_inflow_crore"].max()),
            }
        if _table_exists(conn, "fact_transactions"):
            txn = pd.read_sql("SELECT transaction_type, amount_inr FROM fact_transactions", conn)
            summary["transactions"] = {
                "count": int(len(txn)),
                "total_amount": float(txn["amount_inr"].sum()),
                "by_type": txn.groupby("transaction_type")["amount_inr"].sum().to_dict(),
            }
    finally:
        conn.close()

    out_path = REPORTS_DIR / "eda_summary.json"
    out_path.write_text(json.dumps(summary, indent=2, default=str))
    log.info("EDA complete: summary written to %s", out_path)
    return summary


# ════════════════════════════════════════════════════════════════════════════
# STAGE 5 — METRICS  (alpha/beta, tracking error, VaR/CVaR, fund scorecard)
# ════════════════════════════════════════════════════════════════════════════

def _compute_alpha_beta(scheme_returns: pd.Series, benchmark_returns: pd.Series) -> tuple[float, float]:
    """Compute annualised alpha and beta of *scheme_returns* vs *benchmark_returns*.

    Both series MUST already have a unique (non-duplicated) date index, or
    the inner-join alignment below will raise. Use a single benchmark
    index (e.g. NIFTY50 only) and a deduplicated-by-date scheme series.

    Parameters
    ----------
    scheme_returns:
        Daily percentage returns for the scheme, indexed by date.
    benchmark_returns:
        Daily percentage returns for the benchmark, indexed by date.

    Returns
    -------
    tuple[float, float]
        (alpha, beta) — alpha annualised assuming 252 trading days.
        (NaN, NaN) if there is insufficient overlap.
    """
    aligned = pd.concat([scheme_returns, benchmark_returns], axis=1, join="inner").dropna()
    if len(aligned) < 30:
        return float("nan"), float("nan")
    y, x = aligned.iloc[:, 0], aligned.iloc[:, 1]
    beta = float(np.cov(y, x)[0, 1] / np.var(x)) if np.var(x) > 0 else float("nan")
    alpha_daily = float(y.mean() - beta * x.mean())
    return alpha_daily * 252, beta


def _compute_tracking_error(scheme_returns: pd.Series, benchmark_returns: pd.Series) -> float:
    """Compute annualised tracking error (std dev of return difference).

    Parameters
    ----------
    scheme_returns:
        Daily percentage returns for the scheme, indexed by date.
    benchmark_returns:
        Daily percentage returns for the benchmark, indexed by date.

    Returns
    -------
    float
        Annualised tracking error, or NaN if insufficient overlap.
    """
    aligned = pd.concat([scheme_returns, benchmark_returns], axis=1, join="inner").dropna()
    if len(aligned) < 30:
        return float("nan")
    diff = aligned.iloc[:, 0] - aligned.iloc[:, 1]
    return float(diff.std() * np.sqrt(252))


def _compute_var_cvar(returns: pd.Series, confidence: float = 0.95) -> tuple[float, float]:
    """Compute historical Value-at-Risk and Conditional VaR at *confidence* level.

    Parameters
    ----------
    returns:
        Daily percentage returns.
    confidence:
        Confidence level, e.g. 0.95 for 95% VaR.

    Returns
    -------
    tuple[float, float]
        (VaR, CVaR), both expressed as negative percentages (losses).
        (NaN, NaN) if there are fewer than 30 observations.
    """
    clean_returns = returns.dropna()
    if len(clean_returns) < 30:
        return float("nan"), float("nan")
    var = float(np.percentile(clean_returns, (1 - confidence) * 100))
    cvar = float(clean_returns[clean_returns <= var].mean())
    return var, cvar


def _get_unique_benchmark_returns(benchmark: pd.DataFrame, preferred_index: str = "NIFTY50") -> pd.Series:
    """Build a daily return series from a possibly multi-index benchmark table.

    10_benchmark_indices.csv stacks multiple indices (NIFTY50, NIFTY100, ...)
    in one table. This filters to a single index BEFORE building the return
    series, which is required to avoid duplicate dates in the index.

    Parameters
    ----------
    benchmark:
        Cleaned benchmark DataFrame with columns date, index_name, close_value.
    preferred_index:
        Index name to prefer if present (substring match, e.g. "NIFTY50"
        also matches "NIFTY 50").

    Returns
    -------
    pd.Series
        Daily percentage returns indexed by date, for a single index only.
        Empty series if *benchmark* is empty or missing required columns.
    """
    if benchmark.empty or "close_value" not in benchmark.columns or "date" not in benchmark.columns:
        return pd.Series(dtype=float)

    benchmark = benchmark.copy()
    if "index_name" in benchmark.columns and benchmark["index_name"].nunique() > 1:
        match = next(
            (name for name in benchmark["index_name"].unique() if preferred_index.replace(" ", "") in str(name).replace(" ", "")),
            benchmark["index_name"].value_counts().idxmax(),
        )
        log.info("  benchmark file has multiple indices — using '%s'", match)
        benchmark = benchmark[benchmark["index_name"] == match]

    benchmark = benchmark.drop_duplicates(subset="date", keep="last").sort_values("date")
    return benchmark.set_index("date")["close_value"].pct_change()


def metrics() -> pd.DataFrame:
    """Compute alpha/beta, tracking error, and VaR/CVaR for each tracked scheme.

    Reads fact_nav_per_scheme and dim_benchmark from the database, aligns
    each scheme's daily return series against a single benchmark index,
    and writes three reports: alpha_beta.csv, tracking_error.csv, and
    var_cvar_report.csv, plus a combined fund_scorecard.csv joining
    everything with trailing returns from fact_performance where available.

    Returns
    -------
    pd.DataFrame
        The combined fund scorecard (empty if required tables are missing).
    """
    log.info("Stage 5/6 — Metrics: computing alpha/beta, tracking error, VaR/CVaR")
    if not DB_PATH.exists():
        log.warning("Database not found at %s — run the 'load' stage first", DB_PATH)
        return pd.DataFrame()

    conn = sqlite3.connect(DB_PATH)
    try:
        if not _table_exists(conn, "fact_nav_per_scheme"):
            log.warning("fact_nav_per_scheme table missing — skipping metrics stage")
            return pd.DataFrame()
        nav = pd.read_sql("SELECT * FROM fact_nav_per_scheme", conn)
        benchmark = pd.read_sql("SELECT * FROM dim_benchmark", conn) if _table_exists(conn, "dim_benchmark") else pd.DataFrame()
        perf = pd.read_sql("SELECT * FROM fact_performance", conn) if _table_exists(conn, "fact_performance") else pd.DataFrame()
    finally:
        conn.close()

    if "date" not in nav.columns or "nav" not in nav.columns or "scheme_code" not in nav.columns:
        log.warning("fact_nav_per_scheme missing expected columns — skipping metrics stage")
        return pd.DataFrame()

    nav["date"] = pd.to_datetime(nav["date"])
    if not benchmark.empty:
        benchmark["date"] = pd.to_datetime(benchmark["date"])
    bench_returns = _get_unique_benchmark_returns(benchmark)

    results = []
    for scheme_code, group in nav.groupby("scheme_code"):
        group = group.drop_duplicates(subset="date", keep="last").sort_values("date")
        scheme_returns = group.set_index("date")["nav"].pct_change()
        scheme_name = group["scheme_name"].iloc[0] if "scheme_name" in group.columns else str(scheme_code)

        if not bench_returns.empty:
            alpha, beta = _compute_alpha_beta(scheme_returns, bench_returns)
            tracking_err = _compute_tracking_error(scheme_returns, bench_returns)
        else:
            alpha, beta, tracking_err = float("nan"), float("nan"), float("nan")
        var_95, cvar_95 = _compute_var_cvar(scheme_returns)

        results.append({
            "scheme_code": scheme_code,
            "scheme_name": scheme_name,
            "alpha_annualised": alpha,
            "beta": beta,
            "tracking_error_annualised": tracking_err,
            "var_95": var_95,
            "cvar_95": cvar_95,
        })
        log.info("  %-32s alpha=%.4f beta=%.2f VaR95=%.4f", scheme_name, alpha, beta, var_95)

    metrics_df = pd.DataFrame(results)

    metrics_df[["scheme_code", "scheme_name", "alpha_annualised", "beta"]].to_csv(
        REPORTS_DIR / "alpha_beta.csv", index=False)
    metrics_df[["scheme_code", "scheme_name", "tracking_error_annualised"]].to_csv(
        REPORTS_DIR / "tracking_error.csv", index=False)
    metrics_df[["scheme_code", "scheme_name", "var_95", "cvar_95"]].to_csv(
        REPORTS_DIR / "var_cvar_report.csv", index=False)

    scorecard = metrics_df
    if not perf.empty and "amfi_code" in perf.columns:
        scorecard = metrics_df.merge(
            perf, left_on="scheme_code", right_on="amfi_code", how="left", suffixes=("", "_perf")
        )
    scorecard.to_csv(REPORTS_DIR / "fund_scorecard.csv", index=False)

    log.info("Metrics complete: scorecard written for %d scheme(s)", len(metrics_df))
    return scorecard


# ════════════════════════════════════════════════════════════════════════════
# STAGE 6 — RECOMMENDER
# ════════════════════════════════════════════════════════════════════════════

ALLOCATION_RULES = {
    ("18-25", "high"):   {"Small Cap": 0.4, "Mid Cap": 0.4, "Hybrid": 0.2},
    ("26-35", "medium"): {"Large Cap": 0.4, "Flexi Cap": 0.2, "Hybrid": 0.4},
    ("36-50", "medium"): {"Large Cap": 0.5, "Hybrid": 0.3, "Short Duration": 0.2},
    ("51+",   "low"):    {"Hybrid": 0.3, "Short Duration": 0.7},
    ("any",   "low"):    {"Liquid": 1.0},
}


def recommend(age_band: str = "26-35", risk: str = "medium", top_n: int = 5) -> dict:
    """Recommend fund categories and top schemes for an investor profile.

    Parameters
    ----------
    age_band:
        One of "18-25", "26-35", "36-50", "51+".
    risk:
        One of "low", "medium", "high".
    top_n:
        Number of top-ranked schemes to return per recommended category.

    Returns
    -------
    dict
        Recommended allocation plus top schemes per category. Written to
        reports/recommendations.json.
    """
    log.info("Stage 6/6 — Recommender: profile age=%s risk=%s", age_band, risk)
    allocation = ALLOCATION_RULES.get((age_band, risk), ALLOCATION_RULES[("26-35", "medium")])
    result = {"age_band": age_band, "risk": risk, "allocation": allocation, "top_schemes": {}}

    if DB_PATH.exists():
        conn = sqlite3.connect(DB_PATH)
        try:
            if _table_exists(conn, "fact_performance"):
                perf = pd.read_sql("SELECT * FROM fact_performance", conn)
                if "category" in perf.columns and "return_1yr_pct" in perf.columns:
                    for category in allocation:
                        subset = perf[perf["category"] == category].sort_values("return_1yr_pct", ascending=False)
                        result["top_schemes"][category] = subset.head(top_n)["amfi_code"].tolist()
        finally:
            conn.close()
    else:
        log.warning("Database not found — returning allocation without scheme-level detail")

    out_path = REPORTS_DIR / "recommendations.json"
    out_path.write_text(json.dumps(result, indent=2, default=str))
    log.info("Recommender complete: written to %s", out_path)
    return result


# ════════════════════════════════════════════════════════════════════════════
# ORCHESTRATION
# ════════════════════════════════════════════════════════════════════════════

STAGES = [
    ("ingest",    lambda ctx: ctx.update(raw=ingest())),
    ("clean",     lambda ctx: ctx.update(cleaned=clean(ctx.get("raw") or ingest()))),
    ("load",      lambda ctx: load(ctx.get("cleaned") or clean(ctx.get("raw") or ingest()))),
    ("eda",       lambda ctx: ctx.update(eda_summary=eda())),
    ("metrics",   lambda ctx: ctx.update(scorecard=metrics())),
    ("recommend", lambda ctx: ctx.update(recommendation=recommend())),
]


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments for stage selection."""
    parser = argparse.ArgumentParser(
        description="Bluestock MF Analytics — single-file master pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="Stages (in order): " + " -> ".join(s[0] for s in STAGES),
    )
    parser.add_argument("--stage", metavar="NAME", help="Run only this stage")
    parser.add_argument("--skip", metavar="NAME", help="Run all stages except this one")
    return parser.parse_args()


def main() -> None:
    """Run the full pipeline (or a subset selected via CLI flags)."""
    args = parse_args()

    stages_to_run = STAGES
    if args.stage:
        stages_to_run = [s for s in STAGES if s[0] == args.stage]
        if not stages_to_run:
            log.error("Unknown stage '%s'. Valid names: %s", args.stage, [s[0] for s in STAGES])
            sys.exit(1)
    elif args.skip:
        stages_to_run = [s for s in STAGES if s[0] != args.skip]

    log.info("Bluestock MF Analytics — starting %d stage(s)", len(stages_to_run))
    log.info("Project root resolved to: %s", PROJECT_ROOT)
    log.info("Looking for raw data in:  %s", RAW_DIR)
    if not RAW_DIR.exists():
        log.error(
            "data/raw/ does not exist at %s — check that your CSVs live in "
            "<project_root>/data/raw/, not inside scripts/data/raw/", RAW_DIR
        )

    ctx: dict = {}
    start = time.perf_counter()
    failures = []

    for name, fn in stages_to_run:
        stage_start = time.perf_counter()
        try:
            fn(ctx)
            log.info("Stage '%s' finished in %.1fs", name, time.perf_counter() - stage_start)
        except Exception as exc:
            log.error("Stage '%s' FAILED: %s", name, exc, exc_info=True)
            failures.append(name)

    elapsed = time.perf_counter() - start
    if failures:
        log.error("Pipeline finished with failures in: %s (%.1fs total)", failures, elapsed)
        sys.exit(1)
    log.info("Pipeline completed successfully in %.1fs", elapsed)


if __name__ == "__main__":
    main()
