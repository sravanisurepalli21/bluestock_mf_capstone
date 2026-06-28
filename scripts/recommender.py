"""
recommender.py — Simple Mutual Fund Recommender
Day 6 | Advanced Analytics Task 5

Usage:
    python recommender.py                  # interactive mode
    python recommender.py --risk Low       # CLI mode
    python recommender.py --risk Moderate
    python recommender.py --risk High
"""

import pandas as pd
import argparse
import sys
import os

# ── Config ────────────────────────────────────────────────────────────────────
DATA_DIR  = r'G:\bluestock_mf_capstone\data\processed'
DATA_PATH = None  # auto-detected below

RISK_MAP = {
    'Low'      : ['Low'],
    'Moderate' : ['Moderate', 'Moderately High'],
    'High'     : ['High', 'Very High'],
}

VALID_APPETITES = list(RISK_MAP.keys())


# ── Core logic ────────────────────────────────────────────────────────────────
def find_performance_file(data_dir: str) -> str:
    """Find performance_clean.csv in data_dir (with or without numeric prefix)."""
    if not os.path.isdir(data_dir):
        raise FileNotFoundError(
            f"Data folder not found:\n  {data_dir}\n"
            "Please update DATA_DIR in recommender.py."
        )
    # Try exact clean name first
    exact = os.path.join(data_dir, 'performance_clean.csv')
    if os.path.exists(exact):
        return exact
    # Fallback: match by suffix (handles numeric-prefixed filenames)
    for f in sorted(os.listdir(data_dir)):
        if f.endswith('performance_clean.csv'):
            return os.path.join(data_dir, f)
    raise FileNotFoundError(
        f"Could not find performance_clean.csv in:\n  {data_dir}\n"
        f"Files present: {os.listdir(data_dir)}"
    )


def load_performance_data(path: str = None) -> pd.DataFrame:
    """Load and validate the performance dataset."""
    if path is None:
        path = find_performance_file(DATA_DIR)
    if not os.path.exists(path):
        raise FileNotFoundError(f"File not found:\n  {path}")
    df = pd.read_csv(path)
    required = ['scheme_name', 'risk_grade', 'sharpe_ratio',
                 'return_1yr_pct', 'return_3yr_pct',
                 'expense_ratio_pct', 'max_drawdown_pct', 'aum_crore']
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns: {missing}")
    print(f"  Loaded: {os.path.basename(path)} ({len(df)} funds)")
    return df


def recommend_funds(risk_appetite: str, df: pd.DataFrame, n: int = 3) -> pd.DataFrame:
    """
    Return top-n funds by Sharpe ratio for the given risk appetite.

    Parameters
    ----------
    risk_appetite : str — 'Low', 'Moderate', or 'High'
    df            : pd.DataFrame — performance data
    n             : int — number of recommendations (default 3)
    """
    risk_appetite = risk_appetite.strip().capitalize()
    if risk_appetite not in RISK_MAP:
        raise ValueError(
            f"Invalid risk appetite '{risk_appetite}'. "
            f"Choose from: {VALID_APPETITES}"
        )

    grades = RISK_MAP[risk_appetite]
    subset = df[df['risk_grade'].isin(grades)].copy()

    if subset.empty:
        print(f"⚠  No funds found for risk grade(s): {grades}")
        return pd.DataFrame()

    top = (subset
           .nlargest(n, 'sharpe_ratio')
           [['scheme_name', 'risk_grade', 'sharpe_ratio',
             'return_1yr_pct', 'return_3yr_pct',
             'max_drawdown_pct', 'expense_ratio_pct', 'aum_crore']]
           .reset_index(drop=True))
    top.index += 1
    return top


def print_recommendation(risk_appetite: str, df: pd.DataFrame):
    """Pretty-print the recommendation table."""
    print()
    print("=" * 72)
    print(f"  🎯  FUND RECOMMENDATIONS  |  Risk Appetite: {risk_appetite.upper()}")
    print("=" * 72)

    top = recommend_funds(risk_appetite, df)
    if top.empty:
        return

    # Rename for display
    display = top.rename(columns={
        'scheme_name'      : 'Fund Name',
        'risk_grade'       : 'Risk Grade',
        'sharpe_ratio'     : 'Sharpe',
        'return_1yr_pct'   : '1Y Ret%',
        'return_3yr_pct'   : '3Y Ret%',
        'max_drawdown_pct' : 'Max DD%',
        'expense_ratio_pct': 'ER%',
        'aum_crore'        : 'AUM (Cr)',
    })

    # Truncate long fund names
    display['Fund Name'] = display['Fund Name'].str[:50]

    pd.set_option('display.max_colwidth', 55)
    pd.set_option('display.width', 200)
    print(display.to_string())
    print()
    print("  Legend: Sharpe = risk-adjusted return | Max DD% = worst drawdown")
    print("          ER% = expense ratio | AUM = assets under management (₹ Cr)")
    print("=" * 72)
    print()


def interactive_mode(df: pd.DataFrame):
    """Run the recommender in interactive CLI mode."""
    print()
    print("╔══════════════════════════════════════════════════════════╗")
    print("║         MUTUAL FUND RECOMMENDER  |  Day 6 Analytics     ║")
    print("╚══════════════════════════════════════════════════════════╝")
    print()
    print("  Available risk profiles:")
    print("    [1] Low       — Liquid / Debt funds, stable returns")
    print("    [2] Moderate  — Large Cap / Balanced, steady growth")
    print("    [3] High      — Mid/Small Cap, maximum growth potential")
    print()

    choice_map = {'1': 'Low', '2': 'Moderate', '3': 'High',
                  'low': 'Low', 'moderate': 'Moderate', 'high': 'High'}

    while True:
        raw = input("  Enter risk appetite (1/2/3 or Low/Moderate/High): ").strip()
        appetite = choice_map.get(raw.lower())
        if appetite:
            break
        print(f"  ❌  Invalid input '{raw}'. Please enter 1, 2, 3, Low, Moderate, or High.")

    print_recommendation(appetite, df)

    again = input("  Explore another profile? (y/n): ").strip().lower()
    if again == 'y':
        interactive_mode(df)


# ── Entry point ───────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(
        description='Mutual Fund Recommender based on risk appetite')
    parser.add_argument('--risk', type=str, choices=VALID_APPETITES,
                        help='Risk appetite: Low, Moderate, or High')
    parser.add_argument('--top', type=int, default=3,
                        help='Number of fund recommendations (default: 3)')
    parser.add_argument('--all', action='store_true',
                        help='Print recommendations for all risk levels')
    args = parser.parse_args()

    try:
        df = load_performance_data()
    except FileNotFoundError as e:
        print(f"\n❌  {e}")
        sys.exit(1)

    if args.all:
        for appetite in VALID_APPETITES:
            print_recommendation(appetite, df)
    elif args.risk:
        print_recommendation(args.risk, df)
    else:
        interactive_mode(df)


if __name__ == '__main__':
    main()
