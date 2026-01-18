import logging
import sys
import os

from src.config_service import Config, ConfigError
from src.checkpoint_service import Checkpoint, CheckpointService
from src.ingestion_service import IngestionService
from src.clean_transform_service import CleanTransformService
from src.metrics_service import MetricsService
from src.aggregation_service import AggregationService
from src.writer_service import WriterService
from src.dedup_service import DedupService

def setup_logger():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
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

    bronze_cp = CheckpointService(path=config.bronze_checkpoint, enabled=config.enable_checkpoint)
    silver_cp = CheckpointService(path=config.silver_checkpoint, enabled=config.enable_checkpoint)


    ingestion = IngestionService(config, bronze_cp, silver_cp)
    cleaner = CleanTransformService()
    metrics = MetricsService()
    aggregator = AggregationService(config.anomaly_top_n)

    dedup = DedupService(path=os.path.join(config.output_dir, "dedup", "order_id.db"))

    writer = WriterService(config.output_dir, config.output_format)

    # -------------------------
    # Phase 1: Bronze → Silver
    # -------------------------
    logger.info("Starting Bronze → Silver phase")

    bronze_processed = False
    for payload in ingestion.read_bronze_chunks():
        silver_rows = []
        bronze_processed = True
        logger.info(f"Processing file={payload['file']}, chunk={payload['chunk_index']}, rows={len(payload['rows'])}")

        for row in payload["rows"]:
            metrics.increment_clean_read()
            result = cleaner.process_row(row)

            if not result["is_valid"]:
                metrics.increment_rejected(result["errors"])
                continue

            metrics.increment_success()
            silver_rows.append(result["clean_row"])

        writer.write_silver_chunk(
            payload["file"],
            payload["chunk_index"],
            silver_rows
        )

        bronze_cp.save(
            Checkpoint(
                file=payload["file"],
                chunk_index=payload["chunk_index"] + 1
            )
        )
    
    if not bronze_processed:
        logger.info("No Bronze data to process (checkpoint up-to-date)")

    # -------------------------
    # Phase 2: Silver → Gold
    # -------------------------
    logger.info("Starting Silver → Gold phase")
    silver_processed = False

    for payload in ingestion.read_silver_files():
        silver_processed = True
        
        logger.info(f"Processing file={payload['file']}, rows={len(payload['rows'])}")
        for row in payload["rows"]:
            metrics.increment_read()
            order_id = row["order_id"]
            if dedup.is_duplicate(order_id):
                metrics.increment_deduplicated()
                continue

            dedup.mark_seen(order_id)
            normalized = CleanTransformService.normalize_silver_row(row)
            aggregator.process(normalized)

        silver_cp.save(Checkpoint(file=payload["file"]))
    
    if not silver_processed:
        logger.info("No Silver data to process (checkpoint up-to-date)")
    # -------------------------
    # Gold (FULL OVERWRITE)
    # -------------------------
    logger.info("Writing Gold layer (full overwrite)")

    final_tables = aggregator.finalize()
    for name, rows in final_tables.items():
        if rows:
            writer.write_gold_table(name, rows)
            logger.info(f"Wrote Gold table {name}")

    metrics.log_summary(logger)
    logger.info("Pipeline complete")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python pipeline_orchestrator.py <pipeline.conf>")
        sys.exit(1)
    run_pipeline(sys.argv[1])
