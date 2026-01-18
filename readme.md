# Scalable Batch Data Engineering Pipeline (No Spark)

This project implements a **scalable, restartable batch data pipeline** for processing very large, dirty e-commerce CSV datasets **without using distributed engines** such as Spark, Dask, or Ray.

The pipeline is built using **pure Python**, **chunk-based processing**, and a **medallion-style architecture (Bronze → Silver → Gold)** to ensure correctness, observability, idempotency, and restart safety at scale.

A **separate analytics dashboard** consumes only the Gold layer and is fully decoupled from the pipeline.

## Basic pipeline flow

```md
Config
 → Bronze Checkpoint
 → Bronze Ingestion
 → Clean + Transform
 → Silver Writer (chunk-level)
 → Bronze Checkpoint Commit
 → Silver Checkpoint
 → Silver Ingestion
 → Dedup
 → Aggregation
 → Gold Writer (Upsert / Overwrite)
```

## Architecture Overview

### Key Design Principles

- Chunk-based processing
  - Handles 100M+ rows without loading full datasets into memory.

- Idempotency & restart safety
  - Pipeline can be stopped and resumed safely at any time.
  - Independent checkpoints per medallion layer.

- Explicit data quality rules
  - Critical fields enforce hard failures; non-critical fields use defaults.

- Separation of concerns
  - Each module has a single, well-defined responsibility.
  - Dashboard is fully decoupled from pipeline execution.

- Medallion architecture (adapted for file-based pipelines)
  - Bronze: Raw input CSVs
  - Silver: Cleaned, standardized, row-level outputs
  - Gold: Aggregated, analytics-ready outputs

```css
Start pipeline
│
├── Phase 1: Bronze → Silver
│     ├── Check Bronze checkpoint
│     ├── If Bronze remaining:
│     │     ├── Read Bronze chunks
│     │     ├── Clean & validate
│     │     ├── Write Silver chunks
│     │     └── Update Bronze checkpoint
│     └── Else: Skip Bronze phase
│
├── Phase 2: Silver → Gold
│     ├── Check Silver checkpoint
│     ├── If Silver remaining:
│     │     ├── Read Silver files
│     │     ├── Deduplicate
│     │     ├── Aggregate
│     │     └── Update Silver checkpoint
│     └── Else: Skip Silver phase
│
└── Write Gold (upsert or full overwrite)
```

## Directory Structure

```css
project-root/
│
├── src/
│   ├── config_service.py
│   ├── checkpoint_service.py
│   ├── ingestion_service.py
│   ├── clean_transform_service.py
│   ├── dedup_service.py
│   ├── metrics_service.py
│   ├── aggregation_service.py
│   ├── writer_service.py
│   ├── pipeline_orchestrator.py
│   └── dashboard/
│       ├── app.py
│       ├── loaders.py
│       └── charts.py
│
├── tests/
│   ├── test_config_service.py
│   ├── test_checkpoint_service.py
│   ├── test_ingestion_service.py
│   ├── test_clean_transform_service.py
│   ├── test_metrics_service.py
│   ├── test_aggregation_service.py
│   └── test_writer_service.py
│
├── checkpoints/
│   ├── bronze.json
│   └── silver.json
│
├── pipeline.conf
├── sample_data_gen.py
├── requirements.txt
├── requirements_dashboard.txt
└── README.md
```

## Checkpointing Strategy

### Bronze Checkpoint

Tracks progress through raw input files and chunks.

### Silver Checkpoint

Tracks progress through Silver chunk files.

### Guarantees

- Restart-safe at both Bronze and Silver layers

- Exactly-once semantics

- No duplicate processing

- Deterministic re-runs

## Data Quality Rules

Rows are dropped if any of the following are missing or invalid:

- order_id
- quantity
- unit_price

If missing or invalid, defaults are applied:

| Field            | Default             |
| ---------------- | ------------------- |
| product_name     | `"unknown_product"` |
| category         | `"unknown"`         |
| discount_percent | `0.0`               |
| region           | `"north"`           |
| sale_date        | `1970-01-01`        |
| sale_month       | `1970-01`           |
| customer_email   | `NULL`              |

- The sentinel date 1970-01-01 is preserved in Gold but filtered out in the dashboard.

## Deduplication

- Deduplication happens only during Silver → Gold
- Dedup key: order_id
- Implemented using a disk-backed SQLite store
- Restart-safe and memory-bounded

Dedup ensures:

- No double-counting
- Safe Silver replay
- Correct Gold upserts

## Getting started

- Use `sample_data_gen.py` to generate your test data. Modify the field variables accordingly
- Change settings in `pipeline.conf` if needed.
- Create a virtual environment:

```bash
  - python -m venv venv
  - source venv/bin/activate (POSIX)
  - venv\Scripts\activate (Windows)
  - pip install -r requirements.txt
```

- Run the pipeline from the project root: `python -m src.pipeline_orchestrator pipeline.conf`
- Run unit tests: `python -m unittest discover -s tests -v`
- Run dashboard: `streamlit run src/dashboard/app.py`
- Deactivate virtual environment when done: `source venv/bin/deactivate` (On Windows: `venv\Scripts\deactivate.bat`)

## Assumptions

- Input files are CSV and header row is present
  - Assuming csv files are numbered (1,2,3 etc.). This helps us in making the checkpointing logic.
  - In case of random filenames, we can temporarily move files to a different `processed` folder. But this will incur some I/O costs.
- Checkpointing logic - We are assuming the pipeline can fail and it can also be run at any time.
  - We have to send extra data to enable the system to accurately configure the history load.
  - Ensures idempotency
  - Trade-off: Extra data needs to be sent.
  - Checkpoint format (JSON):  `{"file": "sales_data_part_0003.csv","chunk_index": 12}`
- No deletes or updates in Gold (append + upsert only)
- Rows will be dropped if order_id, quantity, unit_price are missing
  - Default values will apply for other columns
- When delivering, clean data is written back using medallion architecture.
