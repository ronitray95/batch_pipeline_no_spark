import os
import pandas as pd


def load_table(base_path: str, table_name: str) -> pd.DataFrame:
    """
    Loads a Gold table from CSV or Parquet.
    """
    gold_dir = os.path.join(base_path, "gold")

    csv_path = os.path.join(gold_dir, f"{table_name}.csv")
    parquet_path = os.path.join(gold_dir, f"{table_name}.parquet")

    if os.path.exists(parquet_path):
        return pd.read_parquet(parquet_path)

    if os.path.exists(csv_path):
        return pd.read_csv(csv_path)

    raise FileNotFoundError(
        f"Gold table '{table_name}' not found in {gold_dir}"
    )
