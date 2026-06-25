"""
Bronze Layer Configuration Module

Centralized configuration for all Bronze layer ingestion jobs.
Inherits common settings from BaseConfig.
Loads secrets from Databricks secret scope: `ganhockchong-market-data`
"""

from pathlib import Path

from base_config import BaseConfig


class BronzeConfig(BaseConfig):
    """
    Configuration manager for Bronze layer ingestion.

    Inherits from BaseConfig for shared constants (paths, thresholds, market hours).
    Loads all secrets from Databricks secret scope and provides
    validated configuration for streaming, historical, and news ingestion.
    """

    def __init__(self, dbutils):
        """
        Initialize configuration from Databricks secrets.

        Args:
            dbutils: Databricks utilities object
        """
        super().__init__()
        self.dbutils = dbutils
        self.scope = self.SECRET_SCOPE

        self._load_secrets()

    def _load_secrets(self):
        """Load all secrets from Databricks secret scope.

        All secrets are loaded with required=False so that BronzeConfig can be
        constructed regardless of which secrets exist in the scope.  Each notebook
        calls a specific validate_*() method to assert that the secrets *it* needs
        are present before doing any real work.
        """
        self.polygon_api_key = self._get_secret("polygon-api-key", required=False)

        self.kafka_bootstrap_servers = self._get_secret("kafka-bootstrap-servers", required=False)
        self.kafka_topic_streaming = self._get_secret("kafka-topic", default="polygon-streaming-ohlcv", required=False)
        self.kafka_group_id = self._get_secret("kafka-group-id", default="market-data-consumer-group", required=False)
        self.kafka_sasl_username = self._get_secret("kafka-sasl-username", required=False)
        self.kafka_sasl_password = self._get_secret("kafka-sasl-password", required=False)

        self.schema_registry_url = self._get_secret("schema-registry-url", required=False)
        self.schema_registry_api_key = self._get_secret("schema-registry-api-key", required=False)
        self.schema_registry_api_secret = self._get_secret("schema-registry-api-secret", required=False)

        self.polygon_flatfiles_endpoint = self._get_secret(
            "polygon-flatfiles-endpoint", default="https://files.massive.com", required=False
        )
        self.polygon_flatfiles_bucket = self._get_secret(
            "polygon-flatfiles-bucket", default="flatfiles", required=False
        )
        self.polygon_flatfiles_prefix = self._get_secret(
            "polygon-flatfiles-prefix", default="us_stocks_sip/minute_aggs_v1", required=False
        )
        self.polygon_flatfiles_access_key = self._get_secret("polygon-flatfiles-access-key", required=False)
        self.polygon_flatfiles_secret_key = self._get_secret("polygon-flatfiles-secret-key", required=False)

    def _get_secret(self, key, default=None, required=True):
        """
        Get secret from Databricks secret scope.

        Args:
            key: Secret key name
            default: Default value if secret not found
            required: Whether secret is required (raises error if missing)

        Returns:
            Secret value or default
        """
        try:
            value = self.dbutils.secrets.get(scope=self.scope, key=key)
            return value if value else default
        except Exception as e:
            if required:
                raise ValueError(f"Required secret '{key}' not found in scope '{self.scope}': {e}")
            return default

    def get_checkpoint_path(self, job_name: str) -> str:
        """
        Get checkpoint path for streaming job.

        Args:
            job_name: Name of the streaming job

        Returns:
            Checkpoint path in Unity Catalog Volume
        """
        return super().get_checkpoint_path("bronze", job_name)

    def get_kafka_options(self):
        """
        Get Kafka connection options for Spark Structured Streaming.

        Returns:
            Dictionary of Kafka options
        """
        return {
            "kafka.bootstrap.servers": self.kafka_bootstrap_servers,
            "kafka.security.protocol": "SASL_SSL",
            "kafka.sasl.mechanism": "PLAIN",
            "kafka.sasl.jaas.config": f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="{self.kafka_sasl_username}" password="{self.kafka_sasl_password}";',
            "subscribe": self.kafka_topic_streaming,
            "startingOffsets": "latest",  # Historical data comes from flat-file notebooks; streaming fills real-time gaps only
            "failOnDataLoss": "true",  # Fail query on data loss (e.g. topic compaction); set to "false" to skip lost offsets
            "maxOffsetsPerTrigger": 3000,  # Batch size control - handles 10K msgs/min in ~4 batches
        }

    def get_schema_registry_config(self):
        """
        Get Schema Registry configuration for dynamic schema fetching.

        Returns:
            Dictionary with Schema Registry connection details

        Usage:
            from confluent_kafka.schema_registry import SchemaRegistryClient
            sr_config = config.get_schema_registry_config()
            client = SchemaRegistryClient(sr_config)
            schema = client.get_latest_version(config.get_kafka_value_subject()).schema.schema_str
        """
        return {
            "url": self.schema_registry_url,
            "basic.auth.user.info": f"{self.schema_registry_api_key}:{self.schema_registry_api_secret}",
        }

    def get_kafka_value_subject(self, subject_name: str | None = None) -> str:
        """
        Confluent Schema Registry subject for Kafka value serialization.

        Matches confluent-kafka AvroSerializer default: ``{topic}-value``.
        """
        return subject_name or f"{self.kafka_topic_streaming}-value"

    def get_avro_schema_from_registry(self, subject_name: str | None = None):
        """
        Fetch the latest Avro schema from Confluent Schema Registry.

        This enables schema evolution - when Polygon adds new fields,
        the schema is automatically updated without code changes.

        Args:
            subject_name: Schema subject name in registry (default: ``{kafka_topic}-value``)

        Returns:
            Avro schema as JSON string

        Raises:
            ImportError: If confluent-kafka is not installed
            Exception: If schema fetch fails

        Example:
            schema_json = config.get_avro_schema_from_registry()
            parsed_df = kafka_df.select(from_avro(col("value"), schema_json))
        """
        try:
            from confluent_kafka.schema_registry import SchemaRegistryClient
        except ImportError:
            raise ImportError(
                "confluent-kafka package required for Schema Registry integration. "
                "Install with: pip install confluent-kafka[avro]"
            )

        client = SchemaRegistryClient(self.get_schema_registry_config())
        resolved_subject = self.get_kafka_value_subject(subject_name)

        try:
            schema_version = client.get_latest_version(resolved_subject)
            return schema_version.schema.schema_str
        except Exception as e:
            raise Exception(f"Failed to fetch schema '{resolved_subject}' from registry: {e}")

    def get_fallback_avro_schema(self):
        """
        Get fallback Avro schema if Schema Registry is unavailable.

        Reads from schemas/avro/ohlcv_aggregate.avsc — the single source of truth.
        This ensures the fallback always stays in sync with the canonical schema file.

        Returns:
            Avro schema as JSON string

        Raises:
            FileNotFoundError: If the schema file cannot be found
        """
        schema_path = Path(__file__).parents[2] / "schemas" / "avro" / "ohlcv_aggregate.avsc"
        return schema_path.read_text()

    def get_avro_schema(self, use_registry: bool = True, subject_name: str | None = None):
        """
        Get Avro schema with automatic fallback.

        Tries Schema Registry first, falls back to hardcoded schema if unavailable.
        This ensures streaming jobs don't fail if Schema Registry is temporarily down.

        Args:
            use_registry: Whether to try Schema Registry first (default: True)
            subject_name: Schema subject name in registry

        Returns:
            Avro schema as JSON string
        """
        if use_registry:
            try:
                return self.get_avro_schema_from_registry(subject_name)
            except Exception as e:
                print(f"WARNING: Schema Registry unavailable, using fallback schema: {e}")
                return self.get_fallback_avro_schema()
        else:
            return self.get_fallback_avro_schema()

    # =========================================================================
    # Bronze Layer Tuning Parameters (Externalized from Scripts)
    # =========================================================================

    # Ticker Details Ingestion
    TICKER_DETAILS_MAX_WORKERS = 20  # Parallel API calls (reduced from 50 to avoid rate limits)
    TICKER_DETAILS_MAX_RETRIES = 3
    TICKER_DETAILS_BACKOFF_INITIAL = 1.0
    TICKER_DETAILS_BACKOFF_MULTIPLIER = 2.0
    TICKER_DETAILS_LOG_BATCH_SIZE = 1000

    # Streaming Ingestion - Drain Mode
    DRAIN_MODE_ENABLED = True
    DRAIN_MODE_MAX_WAIT_SECONDS = 120
    DRAIN_MODE_FINAL_WAIT_SECONDS = 30

    # Streaming Ingestion - Trigger and Tuning
    STREAMING_TRIGGER_INTERVAL = "5 seconds"
    QUARANTINE_TRIGGER_INTERVAL = "30 seconds"
    SHUFFLE_PARTITIONS = 8

    # WebSocket Producer
    MAX_RECONNECT_ATTEMPTS = 5

    # =========================================================================
    # Per-Pipeline Validation
    #
    # Each notebook calls the method that matches its secret requirements.
    # This avoids the previous all-or-nothing gate where a flat-file job
    # would crash because Kafka secrets were missing.
    # =========================================================================

    def _validate_fields(self, fields: list[tuple[str, str]]) -> bool:
        """Assert that the listed attributes are non-empty.

        Args:
            fields: List of (attribute_name, human_readable_name) tuples.

        Returns:
            True if all present.

        Raises:
            ValueError with a list of missing secret names.
        """
        missing = [name for attr, name in fields if not getattr(self, attr, None)]
        if missing:
            raise ValueError(f"Missing required configuration: {', '.join(missing)}")
        return True

    def validate_streaming(self) -> bool:
        """Validate secrets for Kafka streaming pipelines (producer + consumer).

        Required by: streaming_producer.py, streaming_ingestion.py
        """
        return self._validate_fields(
            [
                ("polygon_api_key", "Polygon.io API Key"),
                ("kafka_bootstrap_servers", "Kafka Bootstrap Servers"),
                ("kafka_sasl_username", "Kafka Username"),
                ("kafka_sasl_password", "Kafka Password"),
                ("schema_registry_url", "Schema Registry URL"),
            ]
        )

    def validate_api(self) -> bool:
        """Validate secrets for Polygon REST API pipelines.

        Required by: ticker_details_ingestion.py, news_ingestion.py
        """
        return self._validate_fields(
            [
                ("polygon_api_key", "Polygon.io API Key"),
            ]
        )

    def validate_flatfiles(self) -> bool:
        """Validate secrets for S3 flat-file ingestion pipelines.

        Required by: historical_ingestion_flatfiles.py, incremental_ingestion_flatfiles.py
        """
        return self._validate_fields(
            [
                ("polygon_flatfiles_access_key", "Polygon Flat Files Access Key"),
                ("polygon_flatfiles_secret_key", "Polygon Flat Files Secret Key"),
            ]
        )

    def validate(self) -> bool:
        """Validate all streaming secrets (backward-compatible default).

        Prefer calling validate_streaming(), validate_api(), or
        validate_flatfiles() to validate only the secrets your pipeline needs.
        """
        return self.validate_streaming()

    def print_config(self):
        """Print safe configuration summary (no secrets)."""
        print("=" * 80)
        print("Bronze Layer Configuration")
        print("=" * 80)
        print(f"Secret Scope: {self.scope}")
        print(f"Volume Base Path: {self.volume_base_path}")
        print(f"Kafka Bootstrap: {self.kafka_bootstrap_servers}")
        print(f"Kafka Topic: {self.kafka_topic_streaming}")
        print(f"Schema Registry: {self.schema_registry_url}")
        print(f"Bronze Streaming Path: {self.get_bronze_path('streaming')}")
        print(f"Bronze Historical Path: {self.get_bronze_path('historical')}")
        print(f"Bronze News Path: {self.get_bronze_path('news')}")
        print(f"Bronze Ticker Details Path: {self.get_bronze_path('ticker_details')}")
        if self.polygon_flatfiles_endpoint:
            print(f"Polygon Flat Files Endpoint: {self.polygon_flatfiles_endpoint}")
            print(f"Polygon Flat Files Bucket: {self.polygon_flatfiles_bucket}")
            print(f"Polygon Flat Files Prefix: {self.polygon_flatfiles_prefix}")
        print("=" * 80)


if __name__ == "__main__":
    config = BronzeConfig(dbutils)
    config.validate()
    config.print_config()
