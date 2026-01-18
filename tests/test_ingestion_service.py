import unittest
import tempfile
import os
import csv

from src.ingestion_service import IngestionService
from src.checkpoint_service import CheckpointService
from src.config_service import Config


class TestIngestionService(unittest.TestCase):

    def test_chunk_reading(self):
        with tempfile.TemporaryDirectory() as tmp:
            csv_path = os.path.join(tmp, "data.csv")

            with open(csv_path, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(["a", "b"])
                for i in range(5):
                    writer.writerow([i, i])

            conf = f"""
[PIPELINE]
chunk_size = 2
max_rows = -1
enable_checkpoint = false
checkpoint_file = cp.json

[INPUT]
input_type = file
input_path = {csv_path}

[OUTPUT]
output_dir = out
format = csv

[MEMORY]
max_chunk_mb = 128
flush_interval = 1000

[ANOMALY]
top_n = 5
high_revenue_threshold = 100
"""
            conf_path = os.path.join(tmp, "conf.ini")
            with open(conf_path, "w") as f:
                f.write(conf)

            config = Config(conf_path)
            ingestion = IngestionService(config, CheckpointService("x", False))

            chunks = list(ingestion.read_chunks())
            self.assertEqual(len(chunks), 3)  # 2,2,1
