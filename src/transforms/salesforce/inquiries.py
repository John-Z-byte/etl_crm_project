# src/transforms/salesforce/inquiries.py
import pandas as pd

from src.common.paths import raw_source_dir, processed_source_dir
from src.common.salesforce_base import run_salesforce_job


INQUIRIES_RENAMES = {
    "source___specific": "source_specific",
    "2nd_source": "secondary_source",
    "2nd_source___specific": "secondary_source_specific",
}


def run():
    source, filename = "salesforce", "salesforce_inquiries.csv"
    in_path = raw_source_dir(source) / filename
    if not in_path.exists():
        print("FILE NOT FOUND")
        print(f"Path: {in_path}")
        return

    out_path = processed_source_dir(source) / "salesforce_inquiries_fact.csv"

    # Run base transform to a temp file
    tmp_out = out_path.with_name(out_path.stem + "__tmp.csv")
    run_salesforce_job(in_path, tmp_out, parse_date=True, drop_unmapped_franchise=True)

    # Load -> rename -> overwrite final
    df = pd.read_csv(tmp_out)
    df = df.rename(columns={k: v for k, v in INQUIRIES_RENAMES.items() if k in df.columns})
    df.to_csv(out_path, index=False)

    # cleanup temp
    tmp_out.unlink(missing_ok=True)

    print(f"[POST] headers fixed -> {out_path}")


if __name__ == "__main__":
    run()
