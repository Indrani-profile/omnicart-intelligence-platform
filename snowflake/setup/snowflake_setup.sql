-- ============================================================================
-- OmniCart — Snowflake Cross-Cloud Setup (Weeks 5-6, Session 5.1-5.2)
-- ============================================================================
-- Snowflake account DDHCMGT-IY64615 runs on AWS (us-east-2), while the
-- lakehouse (ADLS Gen2, omnicartdatalake) lives on Azure. This script wires
-- Snowflake up to read Gold-layer data across that cloud boundary via a
-- storage integration + external stage + external tables.
--
-- Prerequisite one-time manual step (cannot be scripted, done via browser):
--   After running the CREATE STORAGE INTEGRATION statement below, run
--   DESCRIBE STORAGE INTEGRATION omnicart_azure_integration and open the
--   AZURE_CONSENT_URL it returns. Sign in as the Azure tenant admin and
--   click Accept on the app consent screen. This registers Snowflake's
--   multi-tenant app (name shown as AZURE_MULTI_TENANT_APP_NAME, e.g.
--   csivybsnowflakepacint_...) as an Enterprise Application/Service
--   Principal in the Azure tenant.
--
-- Then, in Azure Portal, on the omnicartdatalake storage account:
--   Access control (IAM) -> Add role assignment -> Storage Blob Data Reader
--   -> Members: search for the AZURE_MULTI_TENANT_APP_NAME app -> assign.
--   Role assignment can take a few minutes to propagate.
-- ============================================================================


-- ── 1. Database / schema ────────────────────────────────────────────────────
CREATE DATABASE IF NOT EXISTS OMNICART_DB;
CREATE SCHEMA IF NOT EXISTS OMNICART_DB.GOLD;

USE DATABASE OMNICART_DB;
USE SCHEMA GOLD;


-- ── 2. Storage integration (Snowflake -> Azure AD identity) ────────────────
CREATE STORAGE INTEGRATION IF NOT EXISTS omnicart_azure_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'AZURE'
  ENABLED = TRUE
  AZURE_TENANT_ID = 'b9cfc8d9-44e5-463a-84bc-f0f3af36a37f'
  STORAGE_ALLOWED_LOCATIONS = ('azure://omnicartdatalake.blob.core.windows.net/gold/');

-- Run this to retrieve AZURE_CONSENT_URL and AZURE_MULTI_TENANT_APP_NAME
-- for the manual consent + IAM step described above.
DESCRIBE STORAGE INTEGRATION omnicart_azure_integration;


-- ── 3. External stage ───────────────────────────────────────────────────────
-- NOTE: points at gold/exports/, NOT the bare gold/ container root. Gold
-- tables are Unity-Catalog-managed in Databricks, storing their actual
-- Parquet files under Databricks' own internal metastore storage account
-- (dbstoragedxmd4jtkt35ry, a UUID-keyed path under __unitystorage/...)
-- rather than in omnicartdatalake directly — not meant for external access.
-- Instead, each Gold table is exported (see export_gold_to_adls.py) as plain
-- Parquet into omnicartdatalake/gold/exports/<table_name>/, which is what
-- this stage and the external tables below actually read from.
CREATE OR REPLACE STAGE omnicart_gold_stage
  STORAGE_INTEGRATION = omnicart_azure_integration
  URL = 'azure://omnicartdatalake.blob.core.windows.net/gold/exports/'
  FILE_FORMAT = (TYPE = PARQUET);

-- Sanity check: should list real .parquet files once the export script has
-- been run at least once.
LIST @omnicart_gold_stage/daily_delivery_summary/;


-- ── 4. External tables ──────────────────────────────────────────────────────
-- Snowflake external tables on Parquet default to a single VARIANT column
-- (VALUE) unless columns are explicitly typed via VALUE:field::TYPE
-- expressions, as done below. All five tables below are VERIFIED against
-- real exported data (raw-VALUE-peek method: temporarily create without
-- typed columns, SELECT VALUE LIMIT 5, inspect actual field names, then
-- define the typed version to match).

CREATE OR REPLACE EXTERNAL TABLE ext_daily_delivery_summary (
    pickup_date                DATE   AS (VALUE:pickup_date::DATE),
    trip_count                 NUMBER AS (VALUE:trip_count::NUMBER),
    avg_fare_amount             FLOAT  AS (VALUE:avg_fare_amount::FLOAT),
    total_revenue               FLOAT  AS (VALUE:total_revenue::FLOAT),
    avg_trip_distance_miles     FLOAT  AS (VALUE:avg_trip_distance_miles::FLOAT),
    avg_trip_duration_minutes   FLOAT  AS (VALUE:avg_trip_duration_minutes::FLOAT),
    precipitation_mm             FLOAT  AS (VALUE:precipitation_mm::FLOAT),
    snowfall_cm                  FLOAT  AS (VALUE:snowfall_cm::FLOAT),
    temp_max_c                   FLOAT  AS (VALUE:temp_max_c::FLOAT),
    temp_min_c                   FLOAT  AS (VALUE:temp_min_c::FLOAT)
)
  WITH LOCATION = @omnicart_gold_stage/daily_delivery_summary/
  FILE_FORMAT = (TYPE = PARQUET)
  AUTO_REFRESH = FALSE;

-- CORRECTED from Session 5.1 guess: no distance_tier field exists; real
-- fields are total_trips/on_time_trips/delayed_trips/on_time_rate
-- (avg_delay_minutes only present on rows with delayed_trips > 0).
CREATE OR REPLACE EXTERNAL TABLE ext_delivery_sla (
    pickup_date          DATE   AS (VALUE:pickup_date::DATE),
    pickup_location_id   NUMBER AS (VALUE:pickup_location_id::NUMBER),
    total_trips          NUMBER AS (VALUE:total_trips::NUMBER),
    on_time_trips        NUMBER AS (VALUE:on_time_trips::NUMBER),
    delayed_trips        NUMBER AS (VALUE:delayed_trips::NUMBER),
    on_time_rate         FLOAT  AS (VALUE:on_time_rate::FLOAT),
    avg_delay_minutes    FLOAT  AS (VALUE:avg_delay_minutes::FLOAT)
)
  WITH LOCATION = @omnicart_gold_stage/delivery_sla/
  FILE_FORMAT = (TYPE = PARQUET)
  AUTO_REFRESH = FALSE;

-- CORRECTED from Session 5.1 guess: same field pattern as delivery_sla
-- (grouped by weather_severity instead of pickup_location_id), no
-- pct_delayed field.
CREATE OR REPLACE EXTERNAL TABLE ext_weather_delay_impact (
    pickup_date         DATE    AS (VALUE:pickup_date::DATE),
    weather_severity    VARCHAR AS (VALUE:weather_severity::VARCHAR),
    total_trips         NUMBER  AS (VALUE:total_trips::NUMBER),
    on_time_trips       NUMBER  AS (VALUE:on_time_trips::NUMBER),
    delayed_trips       NUMBER  AS (VALUE:delayed_trips::NUMBER),
    on_time_rate        FLOAT   AS (VALUE:on_time_rate::FLOAT),
    avg_delay_minutes   FLOAT   AS (VALUE:avg_delay_minutes::FLOAT)
)
  WITH LOCATION = @omnicart_gold_stage/weather_delay_impact/
  FILE_FORMAT = (TYPE = PARQUET)
  AUTO_REFRESH = FALSE;

-- CORRECTED from Session 5.1 guess: review_month is a VARCHAR in
-- "YYYY-MM" format (e.g. "1998-01"), not a DATE. Cast to a real date
-- downstream in a dbt model if needed, e.g. TO_DATE(review_month || '-01').
CREATE OR REPLACE EXTERNAL TABLE ext_review_summary (
    category                VARCHAR AS (VALUE:category::VARCHAR),
    review_month            VARCHAR AS (VALUE:review_month::VARCHAR),
    review_count            NUMBER  AS (VALUE:review_count::NUMBER),
    avg_rating              FLOAT   AS (VALUE:avg_rating::FLOAT),
    verified_purchase_pct   FLOAT   AS (VALUE:verified_purchase_pct::FLOAT)
)
  WITH LOCATION = @omnicart_gold_stage/review_summary/
  FILE_FORMAT = (TYPE = PARQUET)
  AUTO_REFRESH = FALSE;

-- Original Session 5.1 schema confirmed correct as-is.
CREATE OR REPLACE EXTERNAL TABLE ext_order_status_current (
    order_id               VARCHAR       AS (VALUE:order_id::VARCHAR),
    delivery_id            VARCHAR       AS (VALUE:delivery_id::VARCHAR),
    pickup_location_id     NUMBER        AS (VALUE:pickup_location_id::NUMBER),
    vendor_id              NUMBER        AS (VALUE:vendor_id::NUMBER),
    order_value            FLOAT         AS (VALUE:order_value::FLOAT),
    current_status         VARCHAR       AS (VALUE:current_status::VARCHAR),
    last_event_timestamp   TIMESTAMP_NTZ AS (VALUE:last_event_timestamp::TIMESTAMP_NTZ),
    updated_at             TIMESTAMP_NTZ AS (VALUE:updated_at::TIMESTAMP_NTZ)
)
  WITH LOCATION = @omnicart_gold_stage/order_status_current/
  FILE_FORMAT = (TYPE = PARQUET)
  AUTO_REFRESH = FALSE;


-- ── 5. Refresh pattern ───────────────────────────────────────────────────────
-- AUTO_REFRESH is FALSE (Azure auto-refresh needs Event Grid integration,
-- more setup than this project needs). After re-running the Databricks
-- export script, refresh each external table's file list manually:
--
--   ALTER EXTERNAL TABLE ext_daily_delivery_summary REFRESH;
--   ALTER EXTERNAL TABLE ext_delivery_sla REFRESH;
--   ALTER EXTERNAL TABLE ext_weather_delay_impact REFRESH;
--   ALTER EXTERNAL TABLE ext_review_summary REFRESH;
--   ALTER EXTERNAL TABLE ext_order_status_current REFRESH;
