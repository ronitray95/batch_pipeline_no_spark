from collections import defaultdict
import heapq


class AggregationService:
    """
    Streaming-safe business aggregations.
    """

    def __init__(self, anomaly_top_n: int):
        # monthly_sales_summary
        self.monthly = defaultdict(lambda: {
            "revenue": 0.0,
            "quantity": 0,
            "discount_sum": 0.0,
            "count": 0
        })

        # top_products
        self.products = defaultdict(lambda: {
            "revenue": 0.0,
            "quantity": 0
        })

        # region_wise_performance
        self.regions = defaultdict(float)

        # category_discount_map
        self.category_discount = defaultdict(lambda: {
            "discount_sum": 0.0,
            "count": 0
        })

        # anomaly detection (min-heap)
        self.anomaly_top_n = anomaly_top_n
        self.anomalies = []

    def process(self, row: dict):
        revenue = row["revenue"]
        quantity = row["quantity"]

        # ------------------
        # Monthly summary
        # ------------------
        m = self.monthly[row["sale_month"]]
        m["revenue"] += revenue
        m["quantity"] += quantity
        m["discount_sum"] += row["discount_percent"]
        m["count"] += 1

        # ------------------
        # Product aggregation
        # ------------------
        p = self.products[row["product_key"]]
        p["revenue"] += revenue
        p["quantity"] += quantity

        # ------------------
        # Region aggregation
        # ------------------
        self.regions[row["region"]] += revenue

        # ------------------
        # Category discount
        # ------------------
        c = self.category_discount[row["category"]]
        c["discount_sum"] += row["discount_percent"]
        c["count"] += 1

        # ------------------
        # Anomaly detection
        # ------------------
        self._track_anomaly(row)

    def _track_anomaly(self, row: dict):
        entry = (row["revenue"], row)

        if len(self.anomalies) < self.anomaly_top_n:
            heapq.heappush(self.anomalies, entry)
        else:
            heapq.heappushpop(self.anomalies, entry)

    # ------------------
    # Final outputs
    # ------------------
    def finalize(self) -> dict:
        return {
            "monthly_sales_summary": self._finalize_monthly(),
            "top_products": self._finalize_products(),
            "region_wise_performance": dict(self.regions),
            "category_discount_map": self._finalize_category_discount(),
            "anomaly_records": self._finalize_anomalies()
        }

    def _finalize_monthly(self):
        result = []
        for month, data in self.monthly.items():
            result.append({
                "sale_month": month,
                "total_revenue": round(data["revenue"], 2),
                "total_quantity": data["quantity"],
                "avg_discount": round(
                    data["discount_sum"] / data["count"], 4
                )
            })
        return result

    def _finalize_products(self):
        items = [
            {
                "product_key": k,
                "revenue": round(v["revenue"], 2),
                "quantity": v["quantity"]
            }
            for k, v in self.products.items()
        ]

        items.sort(key=lambda x: x["revenue"], reverse=True)
        return items[:10]

    def _finalize_category_discount(self):
        result = []
        for cat, data in self.category_discount.items():
            result.append({
                "category": cat,
                "avg_discount": round(
                    data["discount_sum"] / data["count"], 4
                )
            })
        return result

    def _finalize_anomalies(self):
        return sorted(
            [row for _, row in self.anomalies],
            key=lambda x: x["revenue"],
            reverse=True
        )
