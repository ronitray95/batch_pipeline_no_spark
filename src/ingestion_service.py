import csv
import glob
import os
from typing import Iterator, List, Dict

from config_service import Config
from checkpoint_service import Checkpoint, CheckpointService


class IngestionService:
    def __init__(self, config: Config, checkpoint_service: CheckpointService):
        self.config = config
        self.checkpoint_service = checkpoint_service # Checkpoint is saved only after successful processing + write

        self.files = self._resolve_input_files()
        self.files.sort()  # deterministic order

    # -------------------------
    # Public API
    # -------------------------
    def read_chunks(self) -> Iterator[Dict]:
        """
        Generator yielding:
        {
            'file': file_path,
            'chunk_index': int,
            'rows': List[Dict[str, str]]
        }
        """
        checkpoint = self.checkpoint_service.get()

        for file_path in self.files:
            if checkpoint.file and file_path < checkpoint.file:
                continue

            yield from self._read_file_chunks(file_path, checkpoint) # lazy evaluation

    # -------------------------
    # Internal logic
    # -------------------------
    def _resolve_input_files(self) -> List[str]:
        if self.config.input_type == "file":
            if not os.path.exists(self.config.input_path):
                raise FileNotFoundError(self.config.input_path)
            return [self.config.input_path]

        # directory input
        if not os.path.isdir(self.config.input_path):
            raise NotADirectoryError(self.config.input_path)

        pattern = os.path.join(self.config.input_path, self.config.file_pattern)
        files = glob.glob(pattern)

        if not files:
            raise FileNotFoundError(f"No files match pattern: {pattern}")

        return files

    def _read_file_chunks(
        self,
        file_path: str,
        checkpoint: Checkpoint
    ) -> Iterator[Dict]:

        with open(file_path, "r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f) # assumes CSV input and header row present. Reads as dict per row

            chunk = []
            chunk_index = 0
            rows_seen = 0

            for row in reader:
                rows_seen += 1

                # Skip rows already processed (resume)
                if file_path == checkpoint.file and chunk_index < checkpoint.chunk_index:
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
