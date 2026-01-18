from datetime import datetime
from typing import Dict, Any, List
import re


DATE_FORMATS = [
    "%Y-%m-%d",
    "%d/%m/%Y",
    "%m-%d-%Y",
    "%Y/%m/%d"
]

DEFAULT_DATE = "1970-01-01"
DEFAULT_MONTH = "1970-01"

REGION_MAP = {
    "north": "north",
    "nort": "north",
    "south": "south",
    "east": "east",
    "west": "west"
}

CATEGORY_MAP = {
    "electronics": "electronics",
    "electronic": "electronics",
    "home appliance": "home_appliance",
    "homeappliance": "home_appliance",
    "home-appl": "home_appliance",
    "fashion": "fashion",
    "cloths": "fashion"
}


class CleanTransformService:
    """
    Cleans, standardizes, and applies DQ rules.
    Rows missing critical economic fields are dropped.
    """

    def process_row(self, row: Dict[str, str]) -> Dict[str, Any]:
        errors: List[str] = []
        clean: Dict[str, Any] = {}

        # -------------------
        # order_id (HARD FAIL)
        # -------------------
        order_id = row.get("order_id")
        if not order_id:
            return self._reject("missing_order_id")
        clean["order_id"] = order_id

        # -------------------
        # quantity (HARD FAIL)
        # -------------------
        try:
            qty = int(row.get("quantity"))
            if qty <= 0:
                raise ValueError
            clean["quantity"] = qty
        except Exception:
            return self._reject("invalid_quantity")

        # -------------------
        # unit_price (HARD FAIL)
        # -------------------
        try:
            price = float(row.get("unit_price"))
            if price <= 0:
                raise ValueError
            clean["unit_price"] = round(price, 2)
        except Exception:
            return self._reject("invalid_unit_price")

        # -------------------
        # product_name (SOFT FAIL)
        # -------------------
        raw_product = (row.get("product_name") or "").strip().lower()
        if not raw_product:
            raw_product = "unknown_product"
            errors.append("default_product_name")

        clean["product_name"] = raw_product
        clean["product_key"] = re.sub(
            r"[^a-z0-9]+", "_", raw_product
        ).strip("_")

        # -------------------
        # category (SOFT FAIL)
        # -------------------
        raw_category = (row.get("category") or "").strip().lower()
        category = CATEGORY_MAP.get(raw_category, "unknown")
        if category == "unknown":
            errors.append("default_category")
        clean["category"] = category

        # -------------------
        # discount_percent (SOFT FAIL)
        # -------------------
        try:
            discount = float(row.get("discount_percent"))
            discount = max(0.0, min(discount, 1.0))
        except Exception:
            discount = 0.0
            errors.append("default_discount")

        clean["discount_percent"] = discount

        # -------------------
        # region (SOFT FAIL)
        # -------------------
        raw_region = (row.get("region") or "").strip().lower()
        region = REGION_MAP.get(raw_region, "north")
        if raw_region not in REGION_MAP:
            errors.append("default_region")
        clean["region"] = region

        # -------------------
        # sale_date (SOFT FAIL, CANONICAL)
        # -------------------
        sale_date_raw = row.get("sale_date")
        sale_date_obj = None

        if sale_date_raw:
            for fmt in DATE_FORMATS:
                try:
                    sale_date_obj = datetime.strptime(sale_date_raw, fmt)
                    break
                except ValueError:
                    continue

        if sale_date_obj:
            clean["sale_date"] = sale_date_obj.strftime("%Y-%m-%d")
            clean["sale_month"] = sale_date_obj.strftime("%Y-%m")
        else:
            clean["sale_date"] = DEFAULT_DATE
            clean["sale_month"] = DEFAULT_MONTH
            errors.append("default_sale_date")

        # -------------------
        # customer_email (OPTIONAL)
        # -------------------
        email = row.get("customer_email")
        if email and "@" not in email:
            email = None
            errors.append("invalid_email")
        clean["customer_email"] = email

        # -------------------
        # revenue (SAFE)
        # -------------------
        revenue = (
            clean["quantity"]
            * clean["unit_price"]
            * (1 - clean["discount_percent"])
        )
        clean["revenue"] = round(revenue, 2)

        return {
            "clean_row": clean,
            "is_valid": True,
            "errors": errors
        }

    def _reject(self, reason: str) -> Dict[str, Any]:
        return {
            "clean_row": None,
            "is_valid": False,
            "errors": [reason]
        }
