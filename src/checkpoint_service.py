import json
import os
from typing import Optional


class Checkpoint:
    def __init__(self, file: Optional[str] = None, chunk_index: int = 0):
        self.file = file
        self.chunk_index = chunk_index

    def to_dict(self):
        return {
            "file": self.file,
            "chunk_index": self.chunk_index
        }

    @staticmethod
    def from_dict(data):
        return Checkpoint(
            file=data.get("file"),
            chunk_index=data.get("chunk_index", 0)
        )


class CheckpointService:
    """
    One checkpoint service per layer (bronze / silver)
    """

    def __init__(self, path: str, enabled: bool = True):
        self.path = path
        self.enabled = enabled
        self._checkpoint = self._load() if enabled else Checkpoint()

    def _load(self) -> Checkpoint:
        if not os.path.exists(self.path) or os.path.getsize(self.path) == 0:
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
            f.flush()
            os.fsync(f.fileno())

        if os.path.exists(self.path):
            os.remove(self.path)

        os.rename(tmp_path, self.path)
        self._checkpoint = checkpoint

    def get(self) -> Checkpoint:
        return self._checkpoint

    def clear(self):
        if self.enabled and os.path.exists(self.path):
            os.remove(self.path)