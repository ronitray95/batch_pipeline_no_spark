# Basic pipeline flow

```md
Config
 → Checkpoint
 → Ingestion
 → Clean + Transform
 → Metrics
 → Aggregation
 → Writer (Silver per chunk)
 → Checkpoint commit
 → Writer (Gold at end)
```

## Getting started

- Use `sample_data_gen.py` to generate your test data. Modify the field variables accordingly
- Change settings in `pipeline.conf` if needed.
- Run `src/pipeline_orchestrator.py` using the path of the config file
- Run unit tests: `python -m unittest discover -s tests`

## Assumptions

- Input files are CSV and header row is present
  - Assuming csv files are numbered (1,2,3 etc.). This helps us in making the checkpointing logic.
- Checkpointing logic - We are assuming the pipeline can fail and it can also be run at any time.
  - We have to send extra data to enable the system to accurately configure the history load.
  - Ensures idempotency
  - Trade-off: Extra data needs to be sent.
  - Checkpoint format (JSON):  `{"file": "sales_data_part_0003.csv","chunk_index": 12}`
- Rows will be dropped if order_id, quantity, unit_price are missing
  - Default values will apply for other columns
- When delivering, clean data is written back using medallion architecture.
