<img width="1024" height="565" alt="image" src="https://github.com/user-attachments/assets/626c078c-d45a-4304-a28d-752adcf1c9c8" />

This repository contains SQL scripts to build a  Snowflake-based retail data warehouse.

Structure
- sql/00_setup.sql — create database, schemas, and warehouse
- sql/01_stage_formats.sql — file formats and stages
- sql/02_raw_tables.sql — raw tables and COPY commands
- sql/03_ref_transformations.sql — REF layer transformations (dedupe, typed tables)
- sql/04_streams_tasks.sql — streams, procedures, tasks for incremental refresh
- sql/05_governance_and_row_access.sql — roles, grants, tags, row access policies
- sql/06_final_tables.sql — FINAL layer: views and KPI tables

Usage
1. Open Snowflake UI or use SnowSQL.
2. Run scripts in order (00 → 01 → 02 → 03 → 04 → 05 → 06).
3. Review `sql/04_streams_tasks.sql` for the task schedule and adjust if needed.

Notes
- Scripts include `CREATE OR REPLACE` to be idempotent where appropriate.
- Validate file formats and stage paths before running COPY commands.
- Some example SELECTs and INSERTs in scripts are for testing/demo purposes; remove them in production.


```

Replace placeholders with your AWS values. See Snowflake docs for full IAM trust policy and encryption details.

License: MIT
 
