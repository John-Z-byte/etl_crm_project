from src.common.paths import raw_source_dir, processed_source_dir
from src.common.salesforce_base import run_salesforce_job, DIM_ORDER

def run():
    source, filename = "salesforce", "salesforce_accounts.csv"
    in_path = raw_source_dir(source) / filename
    if not in_path.exists():
        print("FILE NOT FOUND"); print(f"Path: {in_path}"); return

    out_path = processed_source_dir(source) / "salesforce_accounts_dim.csv"
    run_salesforce_job(
        in_path,
        out_path,
        franchise_mode="number",
        parse_date=False,
        drop_unmapped_franchise=True,
        preferred_order=DIM_ORDER,
    )

if __name__ == "__main__":
    run()
