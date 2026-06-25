"""Validate Avro schema files for correctness and project conventions."""

import json
import sys
from pathlib import Path

SCHEMA_DIR = Path(__file__).parent.parent / "schemas" / "avro"
REQUIRED_SCHEMAS = ["ohlcv_aggregate.avsc"]
PROJECT_ROOT = Path(__file__).parent.parent
STREAMING_TRANSFORMS_PATH = PROJECT_ROOT / "databricks" / "utils"
OHLCV_SILVER_DLT_PATH = PROJECT_ROOT / "databricks" / "silver" / "ohlcv_silver_dlt.py"


def _load_avro_parser():
    """Return a callable that validates schema with an Avro parser."""
    try:
        from avro.schema import parse as avro_parse

        return lambda schema_json: avro_parse(schema_json)
    except Exception:
        pass

    try:
        from fastavro import parse_schema

        return lambda schema_json: parse_schema(json.loads(schema_json))
    except Exception:
        return None


def _load_transform_field_names() -> set[str]:
    """Load producer transform output keys from streaming_transforms."""
    sys.path.insert(0, str(STREAMING_TRANSFORMS_PATH))
    from streaming_transforms import transform_polygon_to_avro

    sample = transform_polygon_to_avro(
        {
            "ev": "AM",
            "sym": "AAPL",
            "o": 100.0,
            "h": 101.0,
            "l": 99.0,
            "c": 100.5,
            "v": 1000,
            "s": 1700000000000,
            "e": 1700000060000,
            "av": 2000,
            "op": 99.5,
            "z": 10.0,
            "otc": False,
        }
    )
    return set(sample.keys())


def _extract_silver_projected_fields() -> set[str]:
    """Best-effort extraction of projected Bronze fields in _read_bronze_ohlcv_source."""
    source = OHLCV_SILVER_DLT_PATH.read_text(encoding="utf-8")
    marker = ".select("
    idx = source.find(marker)
    if idx < 0:
        return set()
    tail = source[idx + len(marker) :]
    end = tail.find(")")
    block = tail[:end]
    fields = set()
    for line in block.splitlines():
        line = line.strip()
        if line.startswith('"') and line.endswith('",'):
            fields.add(line.strip('",'))
        elif line.startswith('"') and line.endswith('"'):
            fields.add(line.strip('"'))
    return fields


def validate_schema_file(schema_path: Path) -> tuple[list[str], list[str]]:
    """Validate a single Avro schema file. Returns (errors, warnings)."""
    errors = []
    warnings = []

    # 1. Parse as valid JSON
    try:
        with open(schema_path, encoding="utf-8") as f:
            schema = json.load(f)
    except json.JSONDecodeError as e:
        return [f"Invalid JSON: {e}"], warnings

    # 2. Check required top-level fields
    for field in ["type", "name", "fields"]:
        if field not in schema:
            errors.append(f"Missing required top-level field: '{field}'")

    if errors:
        return errors, warnings

    if schema["type"] != "record":
        errors.append(f"Expected top-level type 'record', got '{schema['type']}'")

    required_field_names = set()
    optional_field_names = set()
    schema_field_names = set()

    # 3. Validate each field definition
    for field in schema.get("fields", []):
        name = field.get("name", "<unnamed>")
        schema_field_names.add(name)

        if "name" not in field:
            errors.append("Field missing 'name' property")
            continue

        if "type" not in field:
            errors.append(f"Field '{name}' missing 'type' property")
            continue

        if "default" in field and field["default"] is None:
            optional_field_names.add(name)
        elif "default" not in field:
            required_field_names.add(name)

        # 4. Convention check: nullable fields must use ["null", type] with default null
        field_type = field["type"]
        if isinstance(field_type, list):
            if field_type[0] != "null":
                errors.append(
                    f"Field '{name}': union types must start with 'null', "
                    f'got {field_type}. Use ["null", "<type>"] format.'
                )
            if field.get("default") is not None:
                errors.append(
                    f"Field '{name}': nullable fields must have '\"default\": null', got default={field.get('default')}"
                )

    # 5. Parser-level validation (critical)
    schema_json = json.dumps(schema)
    parser = _load_avro_parser()
    if parser is None:
        errors.append("No Avro parser available (install avro or fastavro for structural parse validation).")
    else:
        try:
            parser(schema_json)
        except Exception as e:
            errors.append(f"Avro parser failed: {e}")

    # 6. Producer contract checks (critical + warning)
    try:
        transform_fields = _load_transform_field_names()
    except Exception as e:
        errors.append(f"Failed to load transform_polygon_to_avro output fields: {e}")
        return errors, warnings

    unknown_transform_fields = sorted(transform_fields - schema_field_names)
    if unknown_transform_fields:
        errors.append("Producer emits fields not present in schema: " + ", ".join(unknown_transform_fields))

    missing_required = sorted(required_field_names - transform_fields)
    if missing_required:
        errors.append("Schema required fields missing from producer transform output: " + ", ".join(missing_required))

    missing_optional = sorted(optional_field_names - transform_fields)
    if missing_optional:
        warnings.append(
            "Optional schema fields not emitted by producer transform (warn-only): " + ", ".join(missing_optional)
        )

    # 7. Informational Silver projection compatibility note (warn-only)
    try:
        silver_fields = _extract_silver_projected_fields()
        if silver_fields:
            missing_in_silver = sorted((required_field_names & transform_fields) - silver_fields - {"event_type"})
            if missing_in_silver:
                warnings.append(
                    "Producer required fields not projected by Silver _read_bronze_ohlcv_source "
                    "(informational): " + ", ".join(missing_in_silver)
                )
    except Exception as e:
        warnings.append(f"Could not evaluate Silver projection compatibility: {e}")

    return errors, warnings


def main():
    all_errors = {}
    all_warnings = {}

    # Check all required schemas exist
    for schema_name in REQUIRED_SCHEMAS:
        schema_path = SCHEMA_DIR / schema_name
        if not schema_path.exists():
            all_errors[schema_name] = [f"Schema file not found: {schema_path}"]
            continue

        errors, warnings = validate_schema_file(schema_path)
        if errors:
            all_errors[schema_name] = errors
        if warnings:
            all_warnings[schema_name] = warnings

    if all_warnings:
        print("Schema validation warnings (non-blocking):")
        for schema_name, warnings in all_warnings.items():
            print(f"\n  {schema_name}:")
            for warning in warnings:
                print(f"    - {warning}")

    # Report results
    if all_errors:
        print("Schema validation FAILED:")
        for schema_name, errors in all_errors.items():
            print(f"\n  {schema_name}:")
            for error in errors:
                print(f"    - {error}")
        sys.exit(1)
    else:
        print(f"Schema validation passed ({len(REQUIRED_SCHEMAS)} schema(s) validated)")
        sys.exit(0)


if __name__ == "__main__":
    main()
