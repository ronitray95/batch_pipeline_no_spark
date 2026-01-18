import json
import os
from typing import Optional


class Checkpoint:
    def __init__(self, file: Optional[str] = None, chunk_index: int = 0, rows_processed: int = 0):
        self.file = file
        self.chunk_index = chunk_index
        self.rows_processed = rows_processed

    def to_dict(self):
        return {
            "file": self.file,
            "chunk_index": self.chunk_index,
            "rows_processed": self.rows_processed
        }

    @staticmethod
    def from_dict(data):
        return Checkpoint(
            file=data.get("file"),
            chunk_index=data.get("chunk_index", 0),
            rows_processed=data.get("rows_processed", 0)
        )


class CheckpointService:
    def __init__(self, path: str, enabled: bool = True):
        self.path = path
        self.enabled = enabled
        self._checkpoint = None

        if self.enabled:
            self._checkpoint = self._load()

    def _load(self) -> Checkpoint:
        if not os.path.exists(self.path):
            return Checkpoint()

        with open(self.path, "r") as f:
            data = json.load(f)
            return Checkpoint.from_dict(data)

    def save(self, checkpoint: Checkpoint):
        if not self.enabled:
            return

        tmp_path = self.path + ".tmp"

        with open(tmp_path, "w") as f:
            json.dump(checkpoint.to_dict(), f)

        os.replace(tmp_path, self.path)

        self._checkpoint = checkpoint

    def get(self) -> Checkpoint:
        return self._checkpoint if self._checkpoint else Checkpoint()

    def clear(self):
        if self.enabled and os.path.exists(self.path):
            os.remove(self.path)
