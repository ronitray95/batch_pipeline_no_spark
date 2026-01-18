import unittest
import tempfile
import os

from src.writer_service import WriterService


class TestWriterService(unittest.TestCase):

    def test_silver_write(self):
        with tempfile.TemporaryDirectory() as tmp:
            writer = WriterService(tmp, gold_format="csv")

            rows = [{"a": 1, "b": 2}]
            writer.write_silver_chunk("file.csv", 0, rows)

            silver_dir = os.path.join(tmp, "silver")
            self.assertEqual(len(os.listdir(silver_dir)), 1)

    def test_gold_write_csv(self):
        with tempfile.TemporaryDirectory() as tmp:
            writer = WriterService(tmp, gold_format="csv")

            rows = [{"x": 1}]
            writer.write_gold_table("test", rows)

            gold_dir = os.path.join(tmp, "gold")
            self.assertTrue(os.path.exists(os.path.join(gold_dir, "test.csv")))
