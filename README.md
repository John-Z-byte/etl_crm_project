Project Overview

This project is a local Python ETL system that standardizes data from multiple CRMs used by Home Instead franchise offices.

Purpose

Normalize messy CRM exports into stable, analytics-ready CSVs

Enforce schemas, keys, and column order for safe downstream use (Power BI / Excel)

Architecture

datalake/

raw/ – untouched source files (WellSky, UKG, Salesforce, RingCentral)

processed/ – normalized, final CSV outputs (ETL contract)

rejected/ – invalid files with rejection reasons

src/

common/ – shared infrastructure (paths, IO, schema enforcement, franchise enrichment)

transforms/ – dataset-specific business logic (keys, dedupe, rules)

pipelines/ – orchestration entry points

ETL Flow

Base job reads raw/, applies YAML schema, normalizes headers, writes processed/

Transform scripts apply business rules, generate keys, deduplicate, reorder columns

Final CSVs in processed/ are the authoritative datasets

Design Principles

YAML defines schema (columns + types)

Transform scripts never touch raw data

Final column order is explicit and stable

Identity is scoped per franchise (no cross-franchise person dedupe)