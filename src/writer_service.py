import os
import csv

import pyarrow as pa
import pyarrow.parquet as pq


class WriterService:
    """
    Writer service implementing Silver + Gold medallion layers.

    Silver:
      - Always CSV
      - Chunk-based
      - Idempotent

    Gold:
      - Format driven by config (csv, parquet, orc)
    """
    
    def __init__(self, base_output_dir: str, gold_format: str = "parquet"):
        self.base_output_dir = base_output_dir
        self.gold_format = gold_format.lower()

        self.silver_dir = os.path.join(base_output_dir, "silver")
        self.gold_dir = os.path.join(base_output_dir, "gold")

        os.makedirs(self.silver_dir, exist_ok=True)
        os.makedirs(self.gold_dir, exist_ok=True)

    # -------------------------
    # SILVER (unchanged)
    # -------------------------
    def write_silver_chunk(self, source_file, chunk_index, rows):
        if not rows:
            return

        base = os.path.basename(source_file).replace(".csv", "")
        path = os.path.join(
            self.silver_dir,
            f"{base}_chunk_{chunk_index:04d}.csv"
        )

        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)

    # -------------------------
    # GOLD ENTRY POINT
    # -------------------------
    def write_gold_table(self, table_name: str, rows: list):
        if not rows:
            return

        if table_name == "monthly_sales_summary":
            self._upsert_monthly_sales(rows)
        else:
            self._write_gold_full_overwrite(table_name, rows)

    # -------------------------
    # GOLD – FULL OVERWRITE
    # -------------------------
    def _write_gold_full_overwrite(self, table_name: str, rows: list):
        path = os.path.join(
            self.gold_dir,
            f"{table_name}.{self.gold_format}"
        )

        if self.gold_format == "parquet":
            table = pa.Table.from_pylist(rows)
            pq.write_table(table, path)
        else:
            with open(path, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=rows[0].keys())
                writer.writeheader()
                writer.writerows(rows)

    # -------------------------
    # GOLD – UPSERT (NO PARTITIONS)
    # -------------------------
    def _upsert_monthly_sales(self, new_rows: list):
        path = os.path.join(
            self.gold_dir,
            f"monthly_sales_summary.{self.gold_format}"
        )

        existing = {}

        # ---- Read existing Gold (if present) ----
        if os.path.exists(path):
            if self.gold_format == "parquet":
                table = pq.read_table(path)
                rows = table.to_pylist()
            else:
                with open(path, "r", newline="", encoding="utf-8") as f:
                    rows = list(csv.DictReader(f))

            for r in rows:
                existing[r["sale_month"]] = {
                    "sale_month": r["sale_month"],
                    "total_revenue": float(r["total_revenue"]),
                    "total_quantity": int(r["total_quantity"]),
                    "order_count": int(r["order_count"]),
                }

        # ---- Merge new aggregates ----
        for r in new_rows:
            key = r["sale_month"]

            if key in existing:
                existing[key]["total_revenue"] += r["total_revenue"]
                existing[key]["total_quantity"] += r["total_quantity"]
                existing[key]["order_count"] += r["order_count"]
            else:
                existing[key] = r

        merged_rows = list(existing.values())

        # ---- Write back (overwrite) ----
        if self.gold_format == "parquet":
            table = pa.Table.from_pylist(merged_rows)
            pq.write_table(table, path)
        else:
            with open(path, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(
                    f, fieldnames=merged_rows[0].keys()
                )
                writer.writeheader()
                writer.writerows(merged_rows)
