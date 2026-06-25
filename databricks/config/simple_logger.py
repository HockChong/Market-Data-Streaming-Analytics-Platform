"""
Simplified Logging Utility

Lightweight logging for Bronze/Silver/Gold layers.

Philosophy: Databricks cluster logs capture infrastructure details.
This logger focuses only on **business-critical events**:
- Job summary (success/failure with key metrics)
- Data quality results
- Errors with remediation steps

Usage:
    import sys
    sys.path.insert(0, "/Workspace/your/path/databricks/config")
    from path_bootstrap import bootstrap_project_paths
    bootstrap_project_paths()
    from simple_logger import SimpleLogger

    logger = SimpleLogger("news_ingestion", dbutils)

    # Log job summary at the end
    logger.log_job_summary(records_read=1000, records_written=980, quality_score=0.98)

    # Log errors when they occur
    logger.log_error("API connection failed", remediation="Check API key in secrets")
"""

import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Any


class JsonFormatter(logging.Formatter):
    """Structured JSON log formatter for integration with log analytics."""

    def __init__(self, job_name: str, correlation_id: str):
        super().__init__()
        self.job_name = job_name
        self.correlation_id = correlation_id

    def format(self, record: logging.LogRecord) -> str:
        log_entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "job": self.job_name,
            "correlation_id": self.correlation_id,
            "message": record.getMessage(),
        }
        # Include extra structured fields if attached to the record
        if hasattr(record, "extra_fields"):
            log_entry.update(record.extra_fields)
        return json.dumps(log_entry, default=str)


class SimpleLogger:
    """
    Simplified logger for Databricks notebooks.

    Logs only business-critical events:
    - Job summary (start/end with metrics)
    - Data quality results
    - Errors with remediation

    All verbose operational logs are left to Databricks cluster logging.
    """

    def __init__(self, job_name: str, dbutils=None):
        """
        Initialize the logger.

        Args:
            job_name: Name of the job (e.g., 'news_ingestion', 'streaming_producer')
            dbutils: Databricks utilities (optional, for notebook exit)
        """
        self.job_name = job_name
        self.dbutils = dbutils
        self.correlation_id = str(uuid.uuid4())[:8]  # Short ID for readability
        self.start_time = datetime.now(timezone.utc)

        # Capture Databricks job/task context for shared cluster troubleshooting
        self.task_name = None
        self.job_id = None
        self.run_id = None
        self.cluster_id = None

        if dbutils:
            try:
                ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
                self.task_name = ctx.taskKey().getOrElse(None)
                self.job_id = ctx.jobId().getOrElse(None)
                self.run_id = ctx.idInJob().getOrElse(None)
                self.cluster_id = ctx.clusterId().getOrElse(None)
            except Exception:
                pass  # Running interactively, not as a job

        self.logger = logging.getLogger(job_name)
        self.logger.setLevel(logging.INFO)
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            handler.setFormatter(JsonFormatter(job_name, self.correlation_id))
            self.logger.addHandler(handler)

        logging.getLogger("py4j").setLevel(logging.WARNING)
        logging.getLogger("pyspark").setLevel(logging.WARNING)

    def log_start(self):
        """Log job start. Call once at the beginning."""
        print(f"{'=' * 60}")
        print(f"Job: {self.job_name} | ID: {self.correlation_id}")
        if self.task_name:
            print(f"Task: {self.task_name} | Job ID: {self.job_id} | Run ID: {self.run_id}")
        if self.cluster_id:
            print(f"Cluster: {self.cluster_id}")
        print(f"Started: {self.start_time.strftime('%Y-%m-%d %H:%M:%S')} UTC")
        print(f"{'=' * 60}")

    def log_job_summary(
        self,
        status: str = "success",
        records_read: int = 0,
        records_written: int = 0,
        records_rejected: int = 0,
        quality_score: float | None = None,
        extra_metrics: dict[str, Any] | None = None,
    ):
        """
        Log job completion summary. Call once at the end.

        Args:
            status: 'success' or 'failed'
            records_read: Total records read from source
            records_written: Records successfully written
            records_rejected: Records that failed validation
            quality_score: Data quality score (0.0 to 1.0)
            extra_metrics: Additional metrics to log
        """
        duration = (datetime.now(timezone.utc) - self.start_time).total_seconds()
        throughput = records_written / duration if duration > 0 else 0

        summary = {
            "job": self.job_name,
            "id": self.correlation_id,
            "status": status,
            "duration_sec": round(duration, 1),
            "records_read": records_read,
            "records_written": records_written,
            "records_rejected": records_rejected,
            "throughput": f"{throughput:.1f}/sec",
        }

        if self.task_name:
            summary["task"] = self.task_name
        if self.job_id:
            summary["job_id"] = self.job_id
        if self.run_id:
            summary["run_id"] = self.run_id

        if quality_score is not None:
            summary["quality_score"] = f"{quality_score:.1%}"

        if extra_metrics:
            summary.update(extra_metrics)

        icon = "[OK]" if status == "success" else "[FAIL]"
        print(f"\n{icon} Job Summary:")
        for key, value in summary.items():
            print(f"   {key}: {value}")

        self._log_structured("JOB_SUMMARY", summary)

    def log_quality_result(
        self,
        quality_score: float,
        checks_passed: int,
        checks_failed: int,
        failed_checks: list | None = None,
        warning_threshold: float = 0.95,
        critical_threshold: float = 0.80,
    ):
        """
        Log data quality check results.

        Args:
            quality_score: Overall quality score (0.0 to 1.0)
            checks_passed: Number of checks that passed
            checks_failed: Number of checks that failed
            failed_checks: List of failed check names
            warning_threshold: Score below this triggers warning
            critical_threshold: Score below this triggers critical alert
        """
        if quality_score >= warning_threshold:
            icon = "[OK]"
            level = "PASS"
        elif quality_score >= critical_threshold:
            icon = "[WARN]"
            level = "WARNING"
        else:
            icon = "[FAIL]"
            level = "CRITICAL"

        print(f"\n{icon} Data Quality: {level} ({quality_score:.1%})")
        print(f"   Checks: {checks_passed} passed, {checks_failed} failed")

        if failed_checks:
            print(f"   Failed: {', '.join(failed_checks)}")

        quality_data = {
            "event": "QUALITY",
            "score": round(quality_score, 3),
            "level": level,
            "checks_passed": checks_passed,
            "checks_failed": checks_failed,
        }
        if failed_checks:
            quality_data["failed_checks"] = failed_checks
        if level == "CRITICAL":
            self._log_structured("QUALITY", quality_data, log_level="error")
        elif level == "WARNING":
            self._log_structured("QUALITY", quality_data, log_level="warning")
        else:
            self._log_structured("QUALITY", quality_data)

    def log_error(self, message: str, error: Exception | None = None, remediation: str | None = None):
        """
        Log an error with optional remediation steps.

        Args:
            message: Error description
            error: Exception object (optional)
            remediation: Suggested fix (optional)
        """
        print(f"\n[ERROR] Error: {message}")

        if error:
            print(f"   Type: {type(error).__name__}")
            print(f"   Details: {str(error)[:200]}")

        if remediation:
            print(f"   Fix: {remediation}")

        error_data = {"event": "ERROR", "message": message}
        if error:
            error_data["error_type"] = type(error).__name__
            error_data["error_detail"] = str(error)[:200]
        if remediation:
            error_data["remediation"] = remediation
        self._log_structured("ERROR", error_data, log_level="error")

    def log_info(self, message: str):
        """Log a simple info message (use sparingly)."""
        print(f"[INFO] {message}")
        self.logger.info(message)

    def _log_structured(self, event: str, data: dict, log_level: str = "info"):
        """Emit a structured JSON log entry with extra fields."""
        record = self.logger.makeRecord(
            self.logger.name,
            getattr(logging, log_level.upper()),
            "(structured)",
            0,
            event,
            (),
            None,
        )
        record.extra_fields = data
        self.logger.handle(record)

    def exit_job(self, status: str = "success", message: str = ""):
        """
        Exit the Databricks notebook with status.

        Args:
            status: 'success' or 'failed'
            message: Exit message
        """
        exit_msg = f"{status.upper()}: {self.job_name} | {self.correlation_id}"
        if message:
            exit_msg += f" | {message}"

        if status == "failed":
            raise RuntimeError(exit_msg)

        if self.dbutils:
            self.dbutils.notebook.exit(exit_msg)
        else:
            print(f"\nExit: {exit_msg}")


if __name__ == "__main__":
    logger = SimpleLogger("example_job")
    logger.log_start()

    logger.log_quality_result(quality_score=0.97, checks_passed=5, checks_failed=1, failed_checks=["missing_title"])

    logger.log_job_summary(
        status="success", records_read=1000, records_written=970, records_rejected=30, quality_score=0.97
    )
