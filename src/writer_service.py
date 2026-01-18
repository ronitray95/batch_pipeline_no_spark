import csv
import os

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    PYARROW_AVAILABLE = True
except ImportError:
    PYARROW_AVAILABLE = False

try:
    import pyorc
    PYORC_AVAILABLE = True
except ImportError:
    PYORC_AVAILABLE = False


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

    def __init__(self, base_output_dir: str, gold_format: str = "csv"):
        self.base_output_dir = base_output_dir
        self.gold_format = gold_format.lower()

        self.silver_dir = os.path.join(base_output_dir, "silver")
        self.gold_dir = os.path.join(base_output_dir, "gold")

        os.makedirs(self.silver_dir, exist_ok=True)
        os.makedirs(self.gold_dir, exist_ok=True)

        if self.gold_format == "parquet" and not PYARROW_AVAILABLE:
            raise RuntimeError("pyarrow is required for Parquet output")

        if self.gold_format == "orc" and not (PYARROW_AVAILABLE or PYORC_AVAILABLE):
            raise RuntimeError("pyarrow or pyorc is required for ORC output")

    # -------------------------
    # SILVER LAYER (CSV)
    # -------------------------
    def write_silver_chunk(
        self,
        source_file: str,
        chunk_index: int,
        rows: list
    ):
        if not rows:
            return

        base_name = os.path.basename(source_file).replace(".csv", "")
        filename = f"{base_name}_chunk_{chunk_index:04d}.csv"
        path = os.path.join(self.silver_dir, filename)

        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)

    # -------------------------
    # GOLD LAYER (FORMAT-AWARE)
    # -------------------------
    def write_gold_table(self, table_name: str, rows: list):
        if not rows:
            return

        if self.gold_format == "csv":
            self._write_gold_csv(table_name, rows)
        elif self.gold_format == "parquet":
            self._write_gold_parquet(table_name, rows)
        elif self.gold_format == "orc":
            self._write_gold_orc(table_name, rows)
        else:
            raise ValueError(f"Unsupported gold format: {self.gold_format}")

    # -------------------------
    # GOLD IMPLEMENTATIONS
    # -------------------------
    def _write_gold_csv(self, table_name: str, rows: list):
        path = os.path.join(self.gold_dir, f"{table_name}.csv")

        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)

    def _write_gold_parquet(self, table_name: str, rows: list):
        path = os.path.join(self.gold_dir, f"{table_name}.parquet")

        table = pa.Table.from_pylist(rows)
        pq.write_table(table, path)

    def _write_gold_orc(self, table_name: str, rows: list):
        path = os.path.join(self.gold_dir, f"{table_name}.orc")

        if PYARROW_AVAILABLE:
            table = pa.Table.from_pylist(rows)
            with pa.OSFile(path, "wb") as f:
                pa.orc.write_table(table, f)
        elif PYORC_AVAILABLE:
            with open(path, "wb") as f:
                writer = pyorc.Writer(f, pyorc.TypeDescription.from_string("struct<>"))
                for row in rows:
                    writer.write(tuple(row.values()))
                writer.close()
