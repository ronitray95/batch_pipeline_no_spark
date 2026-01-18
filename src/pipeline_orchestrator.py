import logging
import sys

from config_service import Config, ConfigError
from checkpoint_service import Checkpoint, CheckpointService
from ingestion_service import IngestionService
from clean_transform_service import CleanTransformService
from metrics_service import MetricsService
from aggregation_service import AggregationService
from writer_service import WriterService


def setup_logger():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)]
    )
    return logging.getLogger("pipeline")


def run_pipeline(config_path: str):
    logger = setup_logger()

    try:
        config = Config(config_path)
    except ConfigError as e:
        logger.error(f"Config error: {e}")
        sys.exit(1)

    logger.info("Pipeline configuration loaded")

    checkpoint_service = CheckpointService(
        path=config.checkpoint_file,
        enabled=config.enable_checkpoint
    )

    ingestion = IngestionService(config, checkpoint_service)
    cleaner = CleanTransformService()
    metrics = MetricsService()
    aggregator = AggregationService(
        anomaly_top_n=config.anomaly_top_n
    )

    writer = WriterService(
        base_output_dir=config.output_dir,
        gold_format=config.output_format
    )

    logger.info("Starting pipeline execution")

    for payload in ingestion.read_chunks():
        source_file = payload["file"]
        chunk_index = payload["chunk_index"]
        rows = payload["rows"]

        logger.info(
            f"Processing file={source_file}, chunk={chunk_index}, rows={len(rows)}"
        )

        silver_rows = []

        for row in rows:
            metrics.increment_read()

            result = cleaner.process_row(row)

            if not result["is_valid"]:
                metrics.increment_rejected(result["errors"])
                continue

            metrics.increment_success()
            clean_row = result["clean_row"]

            silver_rows.append(clean_row)
            aggregator.process(clean_row)

        # -----------------------------
        # Write Silver (idempotent)
        # -----------------------------
        writer.write_silver_chunk(
            source_file=source_file,
            chunk_index=chunk_index,
            rows=silver_rows
        )

        # -----------------------------
        # Commit checkpoint ONLY after success
        # -----------------------------
        checkpoint_service.save(
            Checkpoint(
                file=source_file,
                chunk_index=chunk_index + 1
            )
        )

    # -----------------------------
    # Write Gold layer (final)
    # -----------------------------
    logger.info("Finalizing aggregations")

    final_tables = aggregator.finalize()

    for table_name, rows in final_tables.items():
        writer.write_gold_table(table_name, rows)
        logger.info(f"Wrote Gold table: {table_name}")

    # -----------------------------
    # Log final metrics
    # -----------------------------
    logger.info("Pipeline execution complete")
    metrics.log_summary(logger)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python pipeline_orchestrator.py <pipeline.conf>")
        sys.exit(1)

    run_pipeline(sys.argv[1])
