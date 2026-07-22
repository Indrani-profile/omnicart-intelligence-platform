# OmniCart Intelligence Platform

An end-to-end data lakehouse project: NYC TLC taxi trips (reframed as delivery trips) and
Amazon Reviews 2023 data, ingested through Bronze/Silver/Gold Delta Lake layers on Databricks,
modeled with dbt into Snowflake across a cross-cloud (AWS Snowflake ↔ Azure ADLS) boundary,
tested with automated data contracts, and served via a live interactive dashboard.

**🔗 Live dashboard:** https://omnicart-intelligence-platform-vshrjnqhkps2arlhy89no4.streamlit.app/

**📊 Live dbt docs (lineage, sources, tests):** https://indrani-profile.github.io/omnicart-intelligence-platform/dbt/

## Architecture

NYC TLC + Amazon Reviews → Databricks (Bronze → Silver → Gold, Delta Lake)
→ ADLS Gen2 export → Snowflake external tables (cross-cloud, key-pair auth)
→ dbt (staging → intermediate → marts, 24 automated data contract tests)
→ Streamlit dashboard

## Key differentiators

- **Statistical data contracts**: 24 automated dbt tests (uniqueness, null checks, range
  bounds, accepted values) gating every change via GitHub Actions CI.
- **CI/CD**: every push runs the full test suite; passing tests on `main` auto-regenerate and
  redeploy the dbt docs site.
- **Cross-domain analysis**: does weather actually affect delivery delays? Answered with real
  correlation coefficients, not just a chart — see `mart_weather_delay_correlation`.
- **Cross-cloud**: Snowflake (AWS) reading Databricks (Azure) data via a storage integration
  authenticated through Azure AD, with key-pair auth throughout (no passwords in CI).

## Findings worth noting

- Weather has a near-zero correlation with delivery delays; severe-weather days show slightly
  *better* on-time rates than clear days — plausibly from proactive operational adjustments on
  known-bad-weather days.
- Amazon's Verified Purchase badge visibly rolls out ~2011-2015 in the review data, confirmed
  against real historical context.

## Stack

Azure (ADLS Gen2, Event Hubs, Databricks) · Snowflake · dbt · Streamlit · GitHub Actions ·
Terraform
