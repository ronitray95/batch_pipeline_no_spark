import unittest
from src.aggregation_service import AggregationService


class TestAggregationService(unittest.TestCase):

    def test_monthly_aggregation(self):
        agg = AggregationService(anomaly_top_n=2)

        row = {
            "sale_month": "2024-01",
            "product_key": "p1",
            "region": "north",
            "category": "electronics",
            "quantity": 1,
            "discount_percent": 0.0,
            "revenue": 100.0
        }

        agg.process(row)
        result = agg.finalize()

        self.assertEqual(result["monthly_sales_summary"][0]["total_revenue"], 100.0)
