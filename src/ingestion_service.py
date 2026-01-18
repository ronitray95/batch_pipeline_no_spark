import csv
import glob
import os
from typing import Iterator, Dict, List


class IngestionService:
    """
    Handles:
    - Bronze → Silver ingestion
    - Silver → Gold ingestion
    """

    def __init__(self, config, bronze_checkpoint, silver_checkpoint):
        self.config = config
        self.bronze_cp = bronze_checkpoint
        self.silver_cp = silver_checkpoint

        self.bronze_files = self._resolve_bronze_files()
        self.bronze_files.sort()

        self.silver_dir = os.path.join(config.output_dir, "silver")

    # -------------------------
    # BRONZE PHASE
    # -------------------------
    def _resolve_bronze_files(self):
        # single file input
        if self.config.input_type == "file":
            if not os.path.exists(self.config.input_path):
                raise FileNotFoundError(self.config.input_path)
            return [self.config.input_path]

        # directory input
        if not os.path.isdir(self.config.input_path):
            raise NotADirectoryError(self.config.input_path)

        files = glob.glob(
            os.path.join(self.config.input_path, self.config.file_pattern)
        )
        if not files:
            raise FileNotFoundError(f"No files match pattern: {self.config.file_pattern}")
        
        return files
    
    def read_bronze_chunks(self) -> Iterator[Dict]:
        cp = self.bronze_cp.get()

        for file_path in self.bronze_files:
            if cp.file and file_path < cp.file:
                continue

            with open(file_path, "r", newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)  # assumes CSV input and header row present. Reads as dict per row

                chunk = []
                chunk_index = 0
                rows_seen = 0

                for row in reader:
                    rows_seen += 1

                    # Skip rows already processed (resume)
                    if file_path == cp.file and chunk_index < cp.chunk_index:
                        if rows_seen % self.config.chunk_size == 0:
                            chunk_index += 1
                        continue

                    chunk.append(row)

                    if len(chunk) >= self.config.chunk_size:
                        yield {
                            "file": file_path,
                            "chunk_index": chunk_index,
                            "rows": chunk
                        }
                        chunk = []
                        chunk_index += 1

                if chunk:
                    yield {
                        "file": file_path,
                        "chunk_index": chunk_index,
                        "rows": chunk
                    }

    # -------------------------
    # SILVER PHASE
    # -------------------------
    def read_silver_files(self) -> Iterator[Dict]:
        if not os.path.exists(self.silver_dir):
            return

        files = sorted(glob.glob(os.path.join(self.silver_dir, "*.csv")))
        cp = self.silver_cp.get()

        for path in files:
            if cp.file and path <= cp.file:
                continue

            with open(path, "r", newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                rows = list(reader)

                yield {
                    "file": path,
                    "rows": rows
                }