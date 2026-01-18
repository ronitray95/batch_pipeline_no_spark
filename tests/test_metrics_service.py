import unittest
from src.metrics_service import MetricsService


class TestMetricsService(unittest.TestCase):

    def test_counters(self):
        m = MetricsService()
        m.increment_read()
        m.increment_success()
        m.increment_rejected(["invalid_quantity"])

        summary = m.summary()
        self.assertEqual(summary["rows_read"], 1)
        self.assertEqual(summary["rows_successful"], 1)
        self.assertEqual(summary["rows_rejected"], 1)
