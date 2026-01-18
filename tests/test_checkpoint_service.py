import unittest
import tempfile
import os
import json

from src.checkpoint_service import Checkpoint, CheckpointService


class TestCheckpointService(unittest.TestCase):

    def test_save_and_load_checkpoint(self):
        with tempfile.NamedTemporaryFile(delete=False) as f:
            path = f.name

        service = CheckpointService(path)
        cp = Checkpoint(file="file.csv", chunk_index=3)
        service.save(cp)

        loaded = service.get()
        self.assertEqual(loaded.file, "file.csv")
        self.assertEqual(loaded.chunk_index, 3)

        os.remove(path)

    # def test_load_nonexistent_checkpoint(self):
    #     with tempfile.NamedTemporaryFile( suffix=".json", delete=False) as f:
    #         json.dump(b'{}', f)
    #         path = f.name

    #     service = CheckpointService(path)
    #     loaded = service.get()
    #     self.assertIsNone(loaded)

    #     os.remove(path)

    # def test_overwrite_checkpoint(self):
    #     with tempfile.NamedTemporaryFile(delete=False) as f:
    #         path = f.name

    #     service = CheckpointService(path)
    #     cp1 = Checkpoint(file="file1.csv", chunk_index=1)
    #     service.save(cp1)

    #     cp2 = Checkpoint(file="file2.csv", chunk_index=2)
    #     service.save(cp2)

    #     loaded = service.get()
    #     self.assertEqual(loaded.file, "file2.csv")
    #     self.assertEqual(loaded.chunk_index, 2)

    #     os.remove(path)
        
	