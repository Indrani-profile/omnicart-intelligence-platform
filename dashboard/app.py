import streamlit as st
import pandas as pd
import plotly.express as px
import snowflake.connector
from cryptography.hazmat.primitives import serialization

st.set_page_config(page_title="OmniCart Intelligence Dashboard", layout="wide")

@st.cache_resource
def get_connection():
    private_key_pem = st.secrets["snowflake"]["private_key"].encode()
    p_key = serialization.load_pem_private_key(
        private_key_pem,
        password=None,
    )
    private_key_der = p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    return snowflake.connector.connect(
        account=st.secrets["snowflake"]["account"],
        user=st.secrets["snowflake"]["user"],
        private_key=private_key_der,
        role="ACCOUNTADMIN",
        warehouse="COMPUTE_WH",
        database="OMNICART_DB",
        schema="dbt_dev",
    )

@st.cache_data(ttl=3600)
def run_query(query):
    conn = get_connection()
    df = pd.read_sql(query, conn)
    df.columns = [c.lower() for c in df.columns]
    return df

st.title("🛒 OmniCart Intelligence Dashboard")
st.caption("Cross-cloud data lakehouse: Databricks → ADLS → Snowflake → dbt")

tab1, tab2, tab3 = st.tabs(["🌦️ Weather & Delays", "⭐ Review Analytics", "🚚 Vendor Performance"])

with tab1:
    st.header("Does weather affect delivery delays?")
    df = run_query("SELECT * FROM mart_weather_delay_correlation ORDER BY avg_delay_minutes DESC")

    col1, col2 = st.columns(2)
    with col1:
        fig = px.bar(df, x="weather_severity", y="avg_delay_minutes",
                      title="Average Delay by Weather Severity",
                      color="weather_severity")
        st.plotly_chart(fig, use_container_width=True)
    with col2:
        fig2 = px.bar(df, x="weather_severity", y="on_time_rate",
                       title="On-Time Rate by Weather Severity",
                       color="weather_severity")
        st.plotly_chart(fig2, use_container_width=True)

    st.subheader("Full breakdown")
    st.dataframe(df, use_container_width=True)

    st.info(
        "**Finding:** Correlation coefficients between weather metrics and delay outcomes "
        "are all near zero. Severe-weather days actually show slightly BETTER on-time rates "
        "than Clear days — plausibly because operations proactively adjust staffing/routing "
        "on known-bad-weather days. Extreme Cold's 3-day sample is too small for strong conclusions."
    )

with tab2:
    st.header("Review Trends Over Time (1998-2023)")
    df = run_query("SELECT * FROM mart_review_analytics ORDER BY category, review_year")

    categories = df["category"].unique()
    selected_category = st.selectbox("Select category", categories)
    filtered = df[df["category"] == selected_category]

    col1, col2 = st.columns(2)
    with col1:
        fig = px.line(filtered, x="review_year", y="review_count",
                       title=f"Review Volume Over Time — {selected_category}")
        st.plotly_chart(fig, use_container_width=True)
    with col2:
        fig2 = px.line(filtered, x="review_year", y="avg_rating",
                        title=f"Average Rating Over Time — {selected_category}")
        st.plotly_chart(fig2, use_container_width=True)

    fig3 = px.line(filtered, x="review_year", y="avg_verified_purchase_pct",
                    title=f"Verified Purchase % Over Time — {selected_category}")
    st.plotly_chart(fig3, use_container_width=True)

    st.dataframe(filtered, use_container_width=True)

with tab3:
    st.header("Vendor Performance")
    df = run_query("SELECT * FROM mart_vendor_performance ORDER BY total_orders DESC")

    col1, col2, col3 = st.columns(3)
    col1.metric("Total Vendors", len(df))
    col2.metric("Total Orders", int(df["total_orders"].sum()))
    col3.metric("Avg Delivered Rate", f"{df['delivered_rate'].mean():.1%}")

    fig = px.bar(df, x="vendor_id", y=["delivered_orders", "cancelled_orders"],
                  title="Orders by Vendor: Delivered vs Cancelled", barmode="stack")
    st.plotly_chart(fig, use_container_width=True)

    st.dataframe(df, use_container_width=True)
    st.caption(
        "Note: only 2 vendors exist in this dataset (synthetic Session 4.3 data), "
        "reflecting the small fixed producer set rather than a real multi-vendor marketplace."
    )
