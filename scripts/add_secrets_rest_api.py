#!/usr/bin/env python3
"""
Add Databricks Secrets via REST API

This script uses the Databricks REST API to add secrets directly.
Works with old or new CLI, doesn't require CLI at all!

Prerequisites:
    pip install requests python-dotenv

Usage:
    python scripts/add_secrets_rest_api.py
"""

import configparser
import os
import sys
from pathlib import Path

import requests

# Add config directory to path for BaseConfig import (repo root, not scripts/)
_repo_root = Path(__file__).resolve().parent.parent
_config_dir = str(_repo_root / "databricks" / "config")
if _config_dir not in sys.path:
    sys.path.insert(0, _config_dir)

# Fix Windows console encoding issues
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")

# Load environment variables from .env file
try:
    from dotenv import load_dotenv

    env_path = _repo_root / ".env"
    load_dotenv(env_path)
    print(f"✓ Loaded .env file from: {env_path}\n")
except ImportError:
    print("⚠️  python-dotenv not installed. Reading from environment variables.\n")

# Configuration — single source of truth for scope name
from base_config import BaseConfig

SCOPE_NAME = BaseConfig.SECRET_SCOPE

# Secrets to add (pulled from .env file)
SECRETS = {
    "polygon-api-key": os.getenv("POLYGON_API_KEY"),
    "kafka-bootstrap-servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
    "kafka-topic": os.getenv("KAFKA_TOPIC_STREAMING"),
    "kafka-group-id": os.getenv("KAFKA_GROUP_ID"),
    "kafka-sasl-username": os.getenv("KAFKA_SASL_USERNAME"),
    "kafka-sasl-password": os.getenv("KAFKA_SASL_PASSWORD"),
    "schema-registry-url": os.getenv("SCHEMA_REGISTRY_URL"),
    "schema-registry-api-key": os.getenv("SCHEMA_REGISTRY_API_KEY"),
    "schema-registry-api-secret": os.getenv("SCHEMA_REGISTRY_API_SECRET"),
    # Polygon Flat Files credentials (needed for historical ingestion)
    "polygon-flatfiles-endpoint": os.getenv("POLYGON_FLATFILES_ENDPOINT"),
    "polygon-flatfiles-bucket": os.getenv("POLYGON_FLATFILES_BUCKET"),
    "polygon-flatfiles-prefix": os.getenv("POLYGON_FLATFILES_PREFIX"),
    "polygon-flatfiles-access-key": os.getenv("POLYGON_AWS_ACCESS_KEY_ID"),
    "polygon-flatfiles-secret-key": os.getenv("POLYGON_AWS_SECRET_ACCESS_KEY"),
}


def load_databricks_config():
    """Load Databricks configuration from env (DATABRICKS_HOST, DATABRICKS_TOKEN) or ~/.databrickscfg"""
    host = os.getenv("DATABRICKS_HOST")
    token = os.getenv("DATABRICKS_TOKEN")

    if not host or not token:
        config_file = Path.home() / ".databrickscfg"
        if config_file.exists():
            config = configparser.ConfigParser()
            config.read(config_file)
            profile = "DEFAULT" if "DEFAULT" in config else list(config.sections())[0]
            host = host or config.get(profile, "host", fallback=None)
            token = token or config.get(profile, "token", fallback=None)

    # Clean host URL - remove query parameters and trailing slashes
    if host:
        if "?" in host:
            host = host.split("?")[0]
        host = host.rstrip("/")

    return host, token


def create_scope(host, token, scope_name):
    """Create secret scope via REST API"""
    url = f"{host}/api/2.0/secrets/scopes/create"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    # Omit initial_manage_principal so only the token owner has manage access.
    # Passing "users" grants all workspace users permission to modify this scope.
    payload = {"scope": scope_name}

    response = requests.post(url, headers=headers, json=payload, timeout=30)

    if response.status_code == 200:
        return True, "Created"
    elif response.status_code == 409 or "already exists" in response.text.lower():
        return True, "Already exists"
    else:
        return False, response.text


def list_scopes(host, token):
    """List all secret scopes"""
    url = f"{host}/api/2.0/secrets/scopes/list"
    headers = {"Authorization": f"Bearer {token}"}

    response = requests.get(url, headers=headers, timeout=30)

    if response.status_code == 200:
        scopes = response.json().get("scopes", [])
        return [s["name"] for s in scopes]
    return []


def add_secret(host, token, scope_name, key, value):
    """Add a secret via REST API"""
    url = f"{host}/api/2.0/secrets/put"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    payload = {"scope": scope_name, "key": key, "string_value": value}

    response = requests.post(url, headers=headers, json=payload, timeout=30)

    if response.status_code == 200:
        return True, "Success"
    else:
        return False, response.text


def list_secrets(host, token, scope_name):
    """List all secrets in a scope"""
    url = f"{host}/api/2.0/secrets/list"
    headers = {"Authorization": f"Bearer {token}"}
    params = {"scope": scope_name}

    response = requests.get(url, headers=headers, params=params, timeout=30)

    if response.status_code == 200:
        secrets = response.json().get("secrets", [])
        return [s["key"] for s in secrets]
    return []


def main():
    print("=" * 60)
    print("Databricks Secrets Setup via REST API")
    print("=" * 60)
    print()

    # Step 1: Load Databricks configuration
    print("Step 1: Loading Databricks configuration...")
    host, token = load_databricks_config()

    if not host or not token:
        print("❌ Databricks configuration not found or incomplete.")
        print()
        print("Update your .env file with:")
        print("  DATABRICKS_HOST=https://your-workspace.cloud.databricks.com")
        print("  DATABRICKS_TOKEN=your-personal-access-token")
        print()
        if not host:
            print("  Missing: DATABRICKS_HOST")
        if not token:
            print("  Missing: DATABRICKS_TOKEN")
        print()
        print("Create a token in Databricks: Settings → Developer → Access tokens")
        print()
        sys.exit(1)

    print("✓ Loaded configuration")
    print(f"  Host: {host}")
    print(f"  Token: {'*' * 20}{token[-4:]}")
    print()

    # Step 2: Check/Create scope
    print(f"Step 2: Checking if scope '{SCOPE_NAME}' exists...")
    scopes = list_scopes(host, token)

    if SCOPE_NAME in scopes:
        print(f"✓ Scope '{SCOPE_NAME}' exists")
    else:
        print(f"⚠️  Scope '{SCOPE_NAME}' not found, creating...")
        success, message = create_scope(host, token, SCOPE_NAME)

        if success:
            print(f"✓ Scope created: {message}")
        else:
            print(f"❌ Failed to create scope: {message}")
            sys.exit(1)
    print()

    # Step 3: Validate secrets
    print("Step 3: Validating secrets from .env file...")
    valid_secrets = {}
    missing_secrets = []

    for key, value in SECRETS.items():
        if value and value.strip():
            valid_secrets[key] = value
        else:
            missing_secrets.append(key)

    if missing_secrets:
        print(f"⚠️  Missing values for {len(missing_secrets)} secrets:")
        for key in missing_secrets:
            print(f"   - {key}")
        print()

    if not valid_secrets:
        print("❌ No valid secrets to add")
        sys.exit(1)

    print(f"✓ Found {len(valid_secrets)} valid secrets")
    print()

    # Step 4: Add secrets
    print(f"Step 4: Adding {len(valid_secrets)} secrets...")
    print()

    success_count = 0
    failed_secrets = []

    for key, value in valid_secrets.items():
        success, message = add_secret(host, token, SCOPE_NAME, key, value)

        if success:
            print(f"  ✓ {key}")
            success_count += 1
        else:
            print(f"  ✗ {key}: {message}")
            failed_secrets.append((key, message))

    # Summary
    print()
    print("=" * 60)
    print("Summary")
    print("=" * 60)
    print(f"✓ Successfully added: {success_count}/{len(valid_secrets)} secrets")

    if failed_secrets:
        print(f"✗ Failed: {len(failed_secrets)} secrets")
        print()
        print("Failed secrets:")
        for key, message in failed_secrets:
            print(f"  - {key}: {message[:100]}")
    else:
        print("🎉 All secrets added successfully!")

    # Step 5: Verify
    print()
    print("=" * 60)
    print("Verification")
    print("=" * 60)
    print()
    print(f"Secrets in scope '{SCOPE_NAME}':")

    all_secrets = list_secrets(host, token, SCOPE_NAME)
    for i, secret_key in enumerate(all_secrets, 1):
        print(f"  {i:2}. {secret_key}")

    print()
    print(f"Total: {len(all_secrets)} secrets")
    print()
    print("=" * 60)
    print()
    print("Use in Databricks notebooks:")
    print(f'  value = dbutils.secrets.get(scope="{SCOPE_NAME}", key="polygon-api-key")')
    print()
    print("=" * 60)


if __name__ == "__main__":
    main()
