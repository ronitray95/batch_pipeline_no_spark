from collections import defaultdict


class MetricsService:
    """
    Tracks pipeline-level metrics and data quality stats.
    """

    def __init__(self):
        self.rows_read = 0
        self.cleaned_rows = 0
        self.rows_successful = 0
        self.rows_rejected = 0
        self.rejection_reasons = defaultdict(int)
        self.rows_deduplicated = 0

    def increment_deduplicated(self):
        self.rows_deduplicated += 1

    def increment_clean_read(self, count: int = 1):
        self.cleaned_rows += count

    def increment_read(self, count: int = 1):
        self.rows_read += count

    def increment_success(self, count: int = 1):
        self.rows_successful += count

    def increment_rejected(self, reasons):
        self.rows_rejected += 1
        for reason in reasons:
            self.rejection_reasons[reason] += 1

    def summary(self) -> dict:
        return {
            "rows_read": self.rows_read,
            "rows_successful": self.rows_successful,
            "rows_rejected": self.rows_rejected,
            "rejection_reasons": dict(self.rejection_reasons)
        }

    def log_summary(self, logger):
        logger.info(f"Rows read from bronze: {self.cleaned_rows}")
        logger.info(f"Rows read from silver: {self.rows_read}")
        logger.info(f"Rows successful: {self.rows_successful}")
        logger.info(f"Rows rejected: {self.rows_rejected}")

        if self.rejection_reasons:
            logger.info("Top rejection reasons:")
            for reason, count in sorted(
                self.rejection_reasons.items(),
                key=lambda x: x[1],
                reverse=True
            ):
                logger.info(f"  {reason}: {count}")
