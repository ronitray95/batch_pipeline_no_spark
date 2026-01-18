import plotly.express as px
import altair as alt


def monthly_revenue_chart(df):
    return px.line(
        df.sort_values("sale_month"),
        x="sale_month",
        y="total_revenue",
        markers=True,
        title="Monthly Revenue Trend"
    )


def region_bar_chart(df):
    return px.bar(
        df,
        x="region",
        y="total_revenue",
        title="Revenue by Region"
    )


def top_products_chart(df):
    chart = alt.Chart(df).mark_bar().encode(
        x=alt.X("product_key:N", sort="-y"),
        y="revenue:Q",
        tooltip=["product_key", "revenue"]
    ).properties(
        title="Top Products by Revenue"
    )

    return chart
