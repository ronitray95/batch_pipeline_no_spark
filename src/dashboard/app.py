import streamlit as st
import pandas as pd

from loaders import load_table
from charts import (
    monthly_revenue_chart,
    region_bar_chart,
    top_products_chart
)

st.set_page_config(
    page_title="Sales Analytics Dashboard",
    layout="wide"
)

st.title("📊 Sales Analytics Dashboard")

# -------------------------
# Input
# -------------------------
output_path = st.text_input(
    "Pipeline Output Path",
    value="./processed"
)

if not output_path:
    st.stop()

# -------------------------
# Load Data
# -------------------------
try:
    monthly_df = load_table(output_path, "monthly_sales_summary")

    monthly_df = monthly_df[monthly_df["sale_month"] > "1970-01-01"]
    top_products_df = load_table(output_path, "top_products")
    region_df = load_table(output_path, "region_wise_performance")
    anomaly_df = load_table(output_path, "anomaly_records")
except Exception as e:
    st.error(str(e))
    st.stop()

# -------------------------
# KPIs
# -------------------------
st.subheader("Key Metrics")

total_revenue = monthly_df["total_revenue"].sum()
total_orders = monthly_df["total_quantity"].sum()

col1, col2 = st.columns(2)
col1.metric("Total Revenue", f"{total_revenue:,.2f}")
col2.metric("Total Orders", int(total_orders))

# -------------------------
# Charts
# -------------------------
st.subheader("Trends")

st.plotly_chart(
    monthly_revenue_chart(monthly_df),
    use_container_width=True
)

st.subheader("Regional Performance")

st.plotly_chart(
    region_bar_chart(region_df),
    use_container_width=True
)

st.subheader("Top Products")

st.altair_chart(
    top_products_chart(top_products_df),
    use_container_width=True
)

# -------------------------
# Anomalies
# -------------------------
st.subheader("Anomalous Transactions")

st.dataframe(
    anomaly_df.sort_values("revenue", ascending=False),
    use_container_width=True
)
