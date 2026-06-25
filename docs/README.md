# Documentation Index

Welcome to the documentation for the **Market Data Streaming & Analytics Platform**. This folder contains detailed documentation on the architecture, data models, and interview preparation materials for the capstone project.

## 📚 Core Documentation

- [**Capstone Proposal**](capstone_proposal.md)  
  The main overview of the project, including the architecture, data flow, technology justification, and key features.
- [**Data Lineage**](DATA_LINEAGE.md)  
  End-to-end data flow through the Medallion architecture, table dependencies, architecture patterns, and storage locations.
- [**Data Dictionary**](DATA_DICTIONARY.md)  
  Complete schema reference for every Bronze, Silver, and Gold table — columns, types, nullability, quality checks, and partitioning.
- [**Daily Rollup Design Rationale**](DAILY_ROLLUP_DESIGN.md)  
  Why the daily OHLCV rollup is a materialized view — serverless incremental refresh, idempotency, and the aggregation shape that keeps it incrementally maintainable.

## 📸 Screenshots & Evidence

- [**Pipeline & Dashboard Screenshots**](screenshots/README.md)  
  Guided walkthrough of the platform running end-to-end — ingestion job runs, DLT pipeline DAGs with record counts, the WAP quality audit table, and the Streamlit dashboard (screener, deep dive, watchlist).

## 🗄️ Entity Relationship Diagrams (ERDs)

The project follows a Medallion Architecture. The data models for each layer are documented below:

- [**Bronze Layer ERD**](BRONZE_LAYER_ERD.md)  
  Raw ingestion layer capturing streaming data, batch data, and reference metadata.
- [**Silver Layer ERD**](SILVER_LAYER_ERD.md)  
  Cleaned, conformed, and enriched data, along with derived quality metrics and aggregations.
- [**Gold Layer ERD**](GOLD_LAYER_ERD.md)  
  Aggregated datasets structured as a Star Schema for analytics, dashboards, and ML applications.

## 📊 Analytics

- [**Gold Layer Analytics — Top 10 Business Questions**](ANALYTICS_QUESTIONS.md)  
  Investment-analyst business questions answerable from the Gold Star Schema, each with a runnable Spark SQL query grounded in the actual table columns.

## 🔍 Data Quality & Operations

- [**Data Quality Enforcement — Three Layers of Defense**](DATA_QUALITY_ENFORCEMENT.md)  
  How the OHLCV Silver pipeline decides whether a row passes, is quarantined, or halts the pipeline — row-level `expect_or_fail` (schema contract) vs. WAP quarantine vs. the aggregate `wap_audit_log_hc` quality gate.
- [**Quarantine Queries**](QUARANTINE_QUERIES.md)  
  How to inspect rejected/invalid records captured by the WAP pattern — Silver quarantine tables, Bronze dead-letter queue, and audit log queries.
- [**Orphan Symbol Check**](ORPHAN_SYMBOL_CHECK.md)  
  Read-only SQL checks to measure orphan symbols (fact rows with no matching `dim_ticker_hc` row) and track the rate against a baseline.
