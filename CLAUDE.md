# OmniCart Intelligence Platform

E-commerce data intelligence platform. Raw order/customer/event data is ingested,
processed through a Databricks medallion architecture, transformed into
analytics-ready marts with dbt on Snowflake, validated with Great Expectations,
and surfaced through a Streamlit dashboard.

## Tech stack

- **Processing**: Databricks / PySpark (batch + Structured Streaming)
- **Warehouse**: Snowflake
- **Transformation**: dbt (`dbt-snowflake`)
- **Streaming ingestion**: Azure Event Hub
- **Storage / secrets**: Azure Blob Storage, Azure Key Vault
- **Data quality**: Great Expectations
- **Dashboard**: Streamlit
- **Synthetic data / testing**: Faker
- **CI/CD**: GitHub Actions (`.github/workflows/`)

## Folder structure

```
infra/                          IaC for Azure/Databricks/Snowflake resources
ingestion/                      Source connectors and one-off/batch loaders into bronze
databricks/
  bronze/                       Raw, append-only ingestion notebooks/jobs (schema-on-read)
  silver/                       Cleaned, deduped, conformed tables
  gold/                         Business-aggregated tables ready for consumption
  streaming/                    Spark Structured Streaming jobs
  workflows/                    Databricks Workflows/Jobs definitions (JSON/YAML)
dbt_project/
  models/staging/               1:1 source-conformed models (stg_*)
  models/intermediate/          Reusable joins/business logic (int_*)
  models/marts/                 Business-facing fact/dim models (fct_*, dim_*)
streaming/                      Non-Databricks streaming utilities (producers/consumers)
quality/                        Great Expectations suites and checkpoints
dashboard/                      Streamlit app
docs/                           Architecture notes and runbooks
.github/workflows/              CI/CD pipelines
```

## Conventions

- **Medallion layering**: bronze is raw and immutable; silver is cleaned/conformed;
  gold is aggregated for direct consumption. Never write business logic in bronze.
- **dbt layering**: staging models do light renaming/casting only, one staging model
  per source table; intermediate models hold shared joins/logic; marts are the only
  models dashboards/BI should query.
- **Naming**: `stg_<source>__<entity>`, `int_<entity>__<verb>`, `fct_<entity>`,
  `dim_<entity>`.
- **Secrets**: never commit credentials. Use Azure Key Vault or environment
  variables; `.env` files stay untracked.
- **Python**: dependencies are pinned in `requirements.txt`; use the local `venv/`
  (not committed).
- **Quality gates**: every new gold table or dbt mart should have a corresponding
  Great Expectations suite in `quality/`.
