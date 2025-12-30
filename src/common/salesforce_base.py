# src/common/salesforce_base.py
import pandas as pd
from tabulate import tabulate

from src.common.normalize import normalize_headers
from src.common.franchises import enrich_franchise_columns

FACT_ORDER = [
    "date",
    "franchise",
    "franchise_name",
    "franchise_acro",
    "assigned_role",
    "company_account",
    "account_owner",
    "assigned",
    "contact",
    "activity_type",
    "subject",
    "name",
    "opportunity_stage",
    "event_status",
    "status",
]

DIM_ORDER = [
    "franchise",
    "franchise_name",
    "franchise_acro",
    "account_name",
    "type",
    "industry",
    "account_owner",
    "billing_city",
    "billing_stateprovince",
    "rpn_status",
    "created_date",
    "last_activity_date",
]


def reorder_columns(df: pd.DataFrame, preferred: list[str]) -> pd.DataFrame:
    ordered = [c for c in preferred if c in df.columns]
    rest = [c for c in df.columns if c not in ordered]
    return df[ordered + rest]

def run_salesforce_job(
    in_path,
    out_path,
    *,
    franchise_mode: str = "digits",  # "digits" | "number"
    parse_date: bool = True,
    drop_unmapped_franchise: bool = True,
    preferred_order: list[str] = FACT_ORDER,
) -> None:
    df = normalize_headers(pd.read_csv(in_path))

    # --- Build franchise (two supported modes) ---
    if franchise_mode == "digits":
        d2, d3, d4 = "franchise_digit_2", "franchise_digit_3", "franchise_digit_4"
        if all(c in df.columns for c in [d2, d3, d4]):
            df[[d2, d3, d4]] = df[[d2, d3, d4]].apply(pd.to_numeric, errors="coerce").astype("Int64")
            complete = df[[d2, d3, d4]].notna().all(1)
            df["franchise"] = pd.NA
            df.loc[complete, "franchise"] = (
                df.loc[complete, [d2, d3, d4]].astype(int).astype(str).agg("".join, 1).astype("Int64")
            )

    elif franchise_mode == "number":
        src_col = "franchise_" if "franchise_" in df.columns else ("franchise" if "franchise" in df.columns else None)
        if src_col:
            df["franchise"] = pd.to_numeric(df[src_col], errors="coerce").astype("Int64")

    else:
        raise ValueError("franchise_mode must be 'digits' or 'number'")

    # --- Enrich franchise columns (name + acro) ---
    if "franchise" in df.columns:
        df = enrich_franchise_columns(df, franchise_col="franchise")

    # --- Parse date if present ---
    if parse_date and "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df = df[df["date"].notna()].copy()

    # --- Drop blank or unmapped franchise ---
    if drop_unmapped_franchise and "franchise" in df.columns:
        df = df[df["franchise"].notna()].copy()
        if "franchise_name" in df.columns:
            df = df[df["franchise_name"].notna()].copy()
        if "franchise_acro" in df.columns:
            df = df[df["franchise_acro"].notna()].copy()

    df = df.drop(columns=["franchise_"], errors="ignore")

    # --- Reorder + write ---
    df = reorder_columns(df, preferred_order)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False)

    # --- Console + schema ---
    print("FILE FOUND")
    print(f"TOTAL ROWS: {df.shape[0]:,}")
    print(f"TOTAL COLS: {df.shape[1]:,}")
    print(f"OUTPUT: {out_path}")

    schema_df = pd.DataFrame({"column": df.columns, "dtype": [str(df[c].dtype) for c in df.columns]})
    print("\nOUTPUT SCHEMA")
    print(tabulate(schema_df, headers="keys", tablefmt="github", showindex=True))
