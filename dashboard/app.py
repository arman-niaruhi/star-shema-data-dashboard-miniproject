from pathlib import Path

import pandas as pd
import streamlit as st

st.set_page_config(page_title="ERP Star Schema Dashboard", layout="wide")

BASE = Path(__file__).resolve().parents[1] / "data" / "starschema"

METRIC_OPTIONS = {
    "Revenue": "amount",
    "Profit": "profit",
    "Quantity": "quantity",
    "Discount Amount": "discount_amount",
}
DIMENSION_OPTIONS = {
    "Product Category": "product_category",
    "Brand": "brand",
    "Material": "material_description",
    "Country": "customer_country",
    "City": "customer_city",
    "Customer Segment": "customer_segment",
    "Distribution Channel": "distribution_channel",
    "Sales Organization": "sales_organization",
    "Payment Terms": "payment_terms",
}


def load_data():
    fact_sales = pd.read_csv(BASE / "fact_sales.csv")
    dim_customer = pd.read_csv(BASE / "dim_customer.csv")
    dim_product = pd.read_csv(BASE / "dim_product.csv")
    dim_date = pd.read_csv(BASE / "dim_date.csv")
    dim_date["full_date"] = pd.to_datetime(dim_date["full_date"], errors="coerce")
    return fact_sales, dim_customer, dim_product, dim_date


def aggregate_metric(dataframe: pd.DataFrame, dimension: str, metric_name: str) -> pd.DataFrame:
    metric_column = METRIC_OPTIONS[metric_name]
    aggregated = dataframe.groupby(dimension, as_index=False)[metric_column].sum()
    return aggregated.sort_values(metric_column, ascending=False)


fact_sales, dim_customer, dim_product, dim_date = load_data()

df = (
    fact_sales
    .merge(dim_customer, on="customer_key", how="left", suffixes=("", "_customer"))
    .merge(dim_product, on="product_key", how="left")
    .merge(dim_date, on="date_key", how="left")
    .sort_values("full_date")
    .reset_index(drop=True)
)

st.title("ERP Sales Performance Dashboard")
st.caption("Business view generated from an ERP sales extract with customer, material, organization, and fulfillment dimensions.")

with st.sidebar:
    st.header("Filters")

    year_options = sorted(df["year"].dropna().unique().tolist())
    selected_years = st.multiselect("Year", year_options, default=year_options)

    category_options = sorted(df["product_category"].dropna().unique().tolist())
    selected_categories = st.multiselect("Product Category", category_options, default=category_options)

    segment_options = sorted(df["customer_segment"].dropna().unique().tolist())
    selected_segments = st.multiselect("Customer Segment", segment_options, default=segment_options)

    channel_options = sorted(df["distribution_channel"].dropna().unique().tolist())
    selected_channels = st.multiselect("Distribution Channel", channel_options, default=channel_options)

    chart_metric_name = st.selectbox("Metric", list(METRIC_OPTIONS.keys()))
    chart_dimension_name = st.selectbox("Compare By", list(DIMENSION_OPTIONS.keys()))
    top_n = st.slider("Top Results", min_value=5, max_value=20, value=10)

filtered_df = df.copy()

if selected_years:
    filtered_df = filtered_df[filtered_df["year"].isin(selected_years)]
if selected_categories:
    filtered_df = filtered_df[filtered_df["product_category"].isin(selected_categories)]
if selected_segments:
    filtered_df = filtered_df[filtered_df["customer_segment"].isin(selected_segments)]
if selected_channels:
    filtered_df = filtered_df[filtered_df["distribution_channel"].isin(selected_channels)]

if filtered_df.empty:
    st.warning("No data matches the selected filters.")
    st.stop()

chart_dimension = DIMENSION_OPTIONS[chart_dimension_name]

total_revenue = filtered_df["amount"].sum()
total_profit = filtered_df["profit"].sum()
total_discount = filtered_df["discount_amount"].sum()
total_orders = filtered_df["sales_id"].nunique()
profit_margin = (total_profit / total_revenue * 100) if total_revenue else 0
avg_shipping_days = filtered_df["shipping_days"].mean()

st.header("Executive Overview")
metric_cols = st.columns(3)
metric_cols[0].metric("Revenue", f"{total_revenue:,.2f}")
metric_cols[1].metric("Profit", f"{total_profit:,.2f}")
metric_cols[2].metric("Margin %", f"{profit_margin:,.1f}%")
st.caption(
    f"Sales Documents: {total_orders:,} | Discounts: {total_discount:,.2f} | Avg Shipping Days: {avg_shipping_days:,.1f}"
)

st.header("Commercial Breakdown")
left_col, right_col = st.columns((1.2, 1))

with left_col:
    st.subheader(f"{chart_metric_name} by {chart_dimension_name}")
    breakdown = aggregate_metric(filtered_df, chart_dimension, chart_metric_name).head(top_n)
    st.bar_chart(breakdown.set_index(chart_dimension))

with right_col:
    st.subheader("Top Materials by Revenue")
    top_products = (
        filtered_df.groupby(["material_description", "brand"], as_index=False)["amount"]
        .sum()
        .sort_values("amount", ascending=False)
        .head(top_n)
        .rename(columns={"amount": "revenue"})
    )
    st.dataframe(top_products, use_container_width=True, hide_index=True)

st.header("Trend Analysis")
trend_cols = st.columns(2)

with trend_cols[0]:
    st.subheader("Monthly Revenue Trend")
    monthly_revenue = (
        filtered_df.groupby(["year", "month", "month_name"], as_index=False)["amount"]
        .sum()
        .sort_values(["year", "month"])
    )
    monthly_revenue["period"] = monthly_revenue["month_name"].str[:3] + " " + monthly_revenue["year"].astype(str)
    st.line_chart(monthly_revenue.set_index("period")[["amount"]])

with trend_cols[1]:
    st.subheader("Monthly Profit Trend")
    monthly_profit = (
        filtered_df.groupby(["year", "month", "month_name"], as_index=False)["profit"]
        .sum()
        .sort_values(["year", "month"])
    )
    monthly_profit["period"] = monthly_profit["month_name"].str[:3] + " " + monthly_profit["year"].astype(str)
    st.line_chart(monthly_profit.set_index("period")[["profit"]])

st.header("Customer And Organization Insights")
insight_cols = st.columns(3)

with insight_cols[0]:
    st.subheader("Revenue by Customer Segment")
    segment_mix = (
        filtered_df.groupby("customer_segment", as_index=False)["amount"]
        .sum()
        .sort_values("amount", ascending=False)
        .rename(columns={"amount": "revenue"})
    )
    st.dataframe(segment_mix, use_container_width=True, hide_index=True)

with insight_cols[1]:
    st.subheader("Revenue by Distribution Channel")
    channel_mix = (
        filtered_df.groupby("distribution_channel", as_index=False)["amount"]
        .sum()
        .sort_values("amount", ascending=False)
        .rename(columns={"amount": "revenue"})
    )
    st.dataframe(channel_mix, use_container_width=True, hide_index=True)

with insight_cols[2]:
    st.subheader("Revenue by Sales Organization")
    org_mix = (
        filtered_df.groupby("sales_organization", as_index=False)["amount"]
        .sum()
        .sort_values("amount", ascending=False)
        .rename(columns={"amount": "revenue"})
    )
    st.dataframe(org_mix, use_container_width=True, hide_index=True)

st.header("Fulfillment View")
ops_cols = st.columns(2)

with ops_cols[0]:
    st.subheader("Shipping Performance")
    shipping_view = (
        filtered_df.groupby("shipping_mode", as_index=False)
        .agg(avg_shipping_days=("shipping_days", "mean"), revenue=("amount", "sum"))
        .sort_values("revenue", ascending=False)
    )
    shipping_view["avg_shipping_days"] = shipping_view["avg_shipping_days"].round(1)
    st.dataframe(shipping_view, use_container_width=True, hide_index=True)

with ops_cols[1]:
    st.subheader("Payment Terms")
    payment_view = (
        filtered_df.groupby("payment_terms", as_index=False)["amount"]
        .sum()
        .sort_values("amount", ascending=False)
        .rename(columns={"amount": "revenue"})
    )
    st.dataframe(payment_view, use_container_width=True, hide_index=True)

st.header("Detailed ERP Records")
display_columns = [
    "sales_id",
    "sales_document_item",
    "full_date",
    "company_code",
    "sales_organization",
    "distribution_channel",
    "customer_name",
    "customer_segment",
    "customer_country",
    "material_description",
    "product_category",
    "brand",
    "plant",
    "storage_location",
    "payment_terms",
    "incoterms",
    "quantity",
    "gross_amount",
    "discount_amount",
    "amount",
    "profit",
]
st.dataframe(
    filtered_df[display_columns].rename(columns={"full_date": "order_date"}),
    use_container_width=True,
    hide_index=True,
)

csv_data = filtered_df[display_columns].to_csv(index=False).encode("utf-8")
st.download_button(
    "Download filtered data",
    data=csv_data,
    file_name="filtered_erp_sales_data.csv",
    mime="text/csv",
)
