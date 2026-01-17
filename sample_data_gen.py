import csv
import random
import string
import os
from datetime import datetime, timedelta

# -----------------------------
# CONFIG
# -----------------------------
SCHEMA_FILE = "schema.txt"

TOTAL_ROWS = 10_000_000

ENABLE_PARTITIONING = False
ROWS_PER_PARTITION = 2_000_000

OUTPUT_DIR = "output"
BASE_FILENAME = "sales_data"

WRITE_CHUNK_SIZE = 1_000

RANDOM_SEED = 42  # deterministic seeding

# -----------------------------
# INIT
# -----------------------------
random.seed(RANDOM_SEED)

if ENABLE_PARTITIONING:
    os.makedirs(OUTPUT_DIR, exist_ok=True)

# -----------------------------
# DIRTY VALUE POOLS
# -----------------------------
PRODUCT_NAMES = [
    "iPhone 14", "iphone-14", "I Phone14",
    "Samsung Galaxy S22", "galaxy s22", "SAMSUNG S22",
    "LG Washing Machine", "LG washer", "WashingMachine LG",
    "Nike Shoes", "nike shoe", "NIKE SHOES",
    "Wooden Table", "wood table", "Table - Wood"
]

CATEGORIES = [
    "electronics", "Electronics", "electronic",
    "home appliance", "HomeAppliance", "home-appl",
    "fashion", "Fashion", "cloths"
]

REGIONS = [
    "North", "north", "NORTH",
    "South", "south",
    "East", "EAST",
    "West", "west",
    "nort"
]

DATE_FORMATS = [
    "%Y-%m-%d",
    "%d/%m/%Y",
    "%m-%d-%Y",
    "%Y/%m/%d"
]

EMAIL_DOMAINS = ["gmail.com", "yahoo.com", "outlook.com"]

# -----------------------------
# HELPERS
# -----------------------------
def random_order_id():
    return "ORD-" + str(random.randint(10000, 99999))

def random_quantity():
    if random.random() < 0.1:
        return str(-random.randint(1, 5))
    if random.random() < 0.1:
        return "zero"
    return str(random.randint(1, 10))

def random_unit_price():
    return round(random.uniform(100, 100_000), 2)

def random_discount():
    if random.random() < 0.1:
        return round(random.uniform(1.1, 2.0), 2)
    return round(random.uniform(0, 0.8), 2)

def random_date():
    if random.random() < 0.05:
        return ""
    base = datetime.now() - timedelta(days=random.randint(0, 1000))
    return base.strftime(random.choice(DATE_FORMATS))

def random_email():
    if random.random() < 0.2:
        return ""
    name = "".join(random.choices(string.ascii_lowercase, k=7))
    return f"{name}@{random.choice(EMAIL_DOMAINS)}"

# -----------------------------
# CORE LOGIC
# -----------------------------
def load_schema(path):
    schema = []
    with open(path, "r") as f:
        for line in f:
            parts = line.strip().split(":")
            if len(parts) != 2:
                raise ValueError(f"Invalid schema line: {line}")
            schema.append((parts[0], parts[1]))
    return schema

def generate_row(schema):
    row = []
    for field, _ in schema:
        if field == "order_id":
            row.append(random_order_id())
        elif field == "product_name":
            row.append(random.choice(PRODUCT_NAMES))
        elif field == "category":
            row.append(random.choice(CATEGORIES))
        elif field == "quantity":
            row.append(random_quantity())
        elif field == "unit_price":
            row.append(str(random_unit_price()))
        elif field == "discount_percent":
            row.append(str(random_discount()))
        elif field == "region":
            row.append(random.choice(REGIONS))
        elif field == "sale_date":
            row.append(random_date())
        elif field == "customer_email":
            row.append(random_email())
        else:
            row.append("")
    return row

def open_writer(part_index, headers):
    if ENABLE_PARTITIONING:
        filename = f"{BASE_FILENAME}_part_{part_index:04d}.csv"
        path = os.path.join(OUTPUT_DIR, filename)
    else:
        path = f"{BASE_FILENAME}.csv"

    f = open(path, "w", newline="", encoding="utf-8")
    writer = csv.writer(f)
    writer.writerow(headers)
    return f, writer

def generate_csv():
    schema = load_schema(SCHEMA_FILE)
    headers = [f[0] for f in schema]

    part_index = 1
    rows_written_in_part = 0

    file_handle, writer = open_writer(part_index, headers)
    buffer = []

    for i in range(1, TOTAL_ROWS + 1):
        buffer.append(generate_row(schema))
        rows_written_in_part += 1

        if len(buffer) >= WRITE_CHUNK_SIZE:
            writer.writerows(buffer)
            buffer.clear()

        if ENABLE_PARTITIONING and rows_written_in_part >= ROWS_PER_PARTITION:
            file_handle.close()
            part_index += 1
            rows_written_in_part = 0
            file_handle, writer = open_writer(part_index, headers)

        if i % 1_000_000 == 0:
            print(f"Generated {i} rows")

    if buffer:
        writer.writerows(buffer)

    file_handle.close()
    print("CSV generation completed.")

if __name__ == "__main__":
    generate_csv()
