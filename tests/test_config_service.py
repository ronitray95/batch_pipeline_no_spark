import unittest
import tempfile
import os

from src.config_service import Config, ConfigError


class TestConfigService(unittest.TestCase):

    def test_valid_config_loads(self):
        content = """
[PIPELINE]
chunk_size = 10
max_rows = -1
enable_checkpoint = true
checkpoint_file = checkpoint.json

[INPUT]
input_type = file
input_path = data.csv

[OUTPUT]
output_dir = out
format = csv

[MEMORY]
max_chunk_mb = 128
flush_interval = 1000

[ANOMALY]
top_n = 5
high_revenue_threshold = 100000
"""
        with tempfile.NamedTemporaryFile(mode="w", delete=False) as f:
            f.write(content)
            path = f.name

        config = Config(path)
        self.assertEqual(config.chunk_size, 10)
        self.assertTrue(config.enable_checkpoint)

        os.remove(path)

    def test_missing_section_raises(self):
        with tempfile.NamedTemporaryFile(mode="w", delete=False) as f:
            f.write("[PIPELINE]\nchunk_size=10")
            path = f.name

        with self.assertRaises(ConfigError):
            Config(path)

        os.remove(path)

    def test_invalid_value_raises(self):
        content = """
[PIPELINE]
chunk_size = not_an_integer
max_rows = -1
enable_checkpoint = true
[INPUT]
input_type = file
input_path = data.csv
"""
        with tempfile.NamedTemporaryFile(mode="w", delete=False) as f:
            f.write(content)
            path = f.name

        with self.assertRaises(ConfigError):
            Config(path)

        os.remove(path)

    def test_default_values(self):
        content = """
[PIPELINE]
chunk_size = 1000
max_rows = -1
enable_checkpoint = false
checkpoint_file = checkpoint.json

[INPUT]
input_type = file
input_path = data.csv

[OUTPUT]
output_dir = out
format = csv

[MEMORY]
max_chunk_mb = 128
flush_interval = 1000

[ANOMALY]
top_n = 5
high_revenue_threshold = 100000
"""
        with tempfile.NamedTemporaryFile(mode="w", delete=False) as f:
            f.write(content)
            path = f.name

        config = Config(path)
        self.assertEqual(config.chunk_size, 1000)
        self.assertEqual(config.max_rows, -1)
        self.assertFalse(config.enable_checkpoint)

        os.remove(path)
        
# if __name__ == "__main__":
# 	unittest.main()