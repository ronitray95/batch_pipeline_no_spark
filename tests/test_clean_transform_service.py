import unittest
from src.clean_transform_service import CleanTransformService


class TestCleanTransformService(unittest.TestCase):

    def setUp(self):
        self.service = CleanTransformService()

    def test_valid_row(self):
        row = {
            "order_id": "1",
            "product_name": "Phone",
            "category": "electronics",
            "quantity": "2",
            "unit_price": "100",
            "discount_percent": "0.1",
            "region": "north",
            "sale_date": "2024-01-01",
            "customer_email": "a@b.com"
        }

        result = self.service.process_row(row)
        self.assertTrue(result["is_valid"])
        self.assertEqual(result["clean_row"]["revenue"], 180.0)

    def test_missing_quantity_is_rejected(self):
        row = {
            "order_id": "1",
            "unit_price": "100"
        }
        result = self.service.process_row(row)
        self.assertFalse(result["is_valid"])
