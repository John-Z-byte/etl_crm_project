from src.common.paths import raw_source_dir, processed_source_dir
from src.common.salesforce_base import run_salesforce_job

def run():
    source, filename = "salesforce", "salesforce_consults.csv"
    in_path = raw_source_dir(source) / filename
    if not in_path.exists():
        print("FILE NOT FOUND"); print(f"Path: {in_path}"); return

    out_path = processed_source_dir(source) / "salesforce_consults_fact.csv"
    run_salesforce_job(in_path, out_path, parse_date=True, drop_unmapped_franchise=True)

if __name__ == "__main__":
    run()
