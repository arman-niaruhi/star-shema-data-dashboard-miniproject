from pathlib import Path

import pandas as pd
import streamlit as st

st.set_page_config(page_title="Star Schema Dashboard", layout="wide")

BASE = Path(__file__).resolve().parents[1] / "data" / "starschema"

METRIC_OPTIONS = {
    "Revenue": "amount",
    "Quantity": "quantity",
    "Average Unit Price": "unit_price",
}
DIMENSION_OPTIONS = {
    "Category": "category",
    "Product": "product_name",
    "Country": "country",
    "City": "city",
    "Customer": "customer_name",
    "Day": "day_name",
}


def load_data():
    fact_sales = pd.read_csv(BASE / "fact_sales.csv")
    dim_customer = pd.read_csv(BASE / "dim_customer.csv")
    dim_product = pd.read_csv(BASE / "dim_product.csv")
    dim_date = pd.read_csv(BASE / "dim_date.csv")
    dim_date["full_date"] = pd.to_datetime(dim_date["full_date"], errors="coerce")
    return fact_sales, dim_customer, dim_product, dim_date


def format_metric(value: float, metric_name: str) -> str:
    if metric_name == "Revenue":
        return f"{value:,.2f}"
    if metric_name == "Average Unit Price":
        return f"{value:,.2f}"
    return f"{value:,.0f}"


def aggregate_metric(dataframe: pd.DataFrame, dimension: str, metric_name: str) -> pd.DataFrame:
    metric_column = METRIC_OPTIONS[metric_name]
    if metric_name == "Average Unit Price":
        aggregated = dataframe.groupby(dimension, as_index=False)[metric_column].mean()
    else:
        aggregated = dataframe.groupby(dimension, as_index=False)[metric_column].sum()
    return aggregated.sort_values(metric_column, ascending=False)


fact_sales, dim_customer, dim_product, dim_date = load_data()

df = (
    fact_sales
    .merge(dim_customer, on="customer_key", how="left")
    .merge(dim_product, on="product_key", how="left")
    .merge(dim_date, on="date_key", how="left")
    .sort_values("full_date")
    .reset_index(drop=True)
)

st.title("Sales Performance Dashboard")
st.caption("Interactive view of the business star schema for revenue, volume, pricing, customers, and products.")

with st.sidebar:
    st.header("Controllers")

    year_options = sorted(df["year"].dropna().unique().tolist())
    selected_years = st.multiselect("Year", year_options, default=year_options)

    category_options = sorted(df["category"].dropna().unique().tolist())
    selected_categories = st.multiselect("Category", category_options, default=category_options)

    country_options = sorted(df["country"].dropna().unique().tolist())
    selected_countries = st.multiselect("Country", country_options, default=country_options)

    chart_metric_name = st.selectbox("Chart Variable", list(METRIC_OPTIONS.keys()))
    chart_dimension_name = st.selectbox("Compare By", list(DIMENSION_OPTIONS.keys()))
    trend_metric_name = st.selectbox("Trend Variable", list(METRIC_OPTIONS.keys()), index=0)
    top_n = st.slider("Top Results", min_value=3, max_value=15, value=8)

filtered_df = df.copy()

if selected_years:
    filtered_df = filtered_df[filtered_df["year"].isin(selected_years)]
if selected_categories:
    filtered_df = filtered_df[filtered_df["category"].isin(selected_categories)]
if selected_countries:
    filtered_df = filtered_df[filtered_df["country"].isin(selected_countries)]

if filtered_df.empty:
    st.warning("No data matches the selected filters.")
    st.stop()

chart_dimension = DIMENSION_OPTIONS[chart_dimension_name]
chart_metric_column = METRIC_OPTIONS[chart_metric_name]
trend_metric_column = METRIC_OPTIONS[trend_metric_name]

total_revenue = filtered_df["amount"].sum()
total_orders = filtered_df["sales_id"].nunique()
total_customers = filtered_df["customer_id"].nunique()
avg_order_value = total_revenue / total_orders if total_orders else 0

metric_cols = st.columns(4)
metric_cols[0].metric("Revenue", f"{total_revenue:,.2f}")
metric_cols[1].metric("Orders", f"{total_orders:,}")
metric_cols[2].metric("Customers", f"{total_customers:,}")
metric_cols[3].metric("Avg Order Value", f"{avg_order_value:,.2f}")

left_col, right_col = st.columns((1.2, 1))

with left_col:
    st.subheader(f"{chart_metric_name} by {chart_dimension_name}")
    breakdown = aggregate_metric(filtered_df, chart_dimension, chart_metric_name).head(top_n)
    st.bar_chart(breakdown.set_index(chart_dimension))

with right_col:
    st.subheader("Top Products")
    top_products = (
        filtered_df.groupby("product_name", as_index=False)["amount"]
        .sum()
        .sort_values("amount", ascending=False)
        .head(top_n)
        .rename(columns={"amount": "revenue"})
    )
    st.dataframe(top_products, use_container_width=True, hide_index=True)

st.subheader(f"{trend_metric_name} Trend")
monthly_trend = (
    filtered_df
    .groupby(["year", "month", "month_name"], as_index=False)[trend_metric_column]
    .sum()
    .sort_values(["year", "month"])
)
monthly_trend["period"] = monthly_trend["month_name"].str[:3] + " " + monthly_trend["year"].astype(str)
st.line_chart(monthly_trend.set_index("period")[[trend_metric_column]])

col_a, col_b = st.columns(2)

with col_a:
    st.subheader("Customer Geography")
    geography = (
        filtered_df.groupby(["country", "city"], as_index=False)["amount"]
        .sum()
        .sort_values("amount", ascending=False)
        .head(top_n)
        .rename(columns={"amount": "revenue"})
    )
    st.dataframe(geography, use_container_width=True, hide_index=True)

with col_b:
    st.subheader("Category Mix")
    category_mix = (
        filtered_df.groupby("category", as_index=False)["amount"]
        .sum()
        .sort_values("amount", ascending=False)
        .rename(columns={"amount": "revenue"})
    )
    st.dataframe(category_mix, use_container_width=True, hide_index=True)

st.subheader("Detailed Data")
display_columns = [
    "sales_id",
    "full_date",
    "customer_name",
    "country",
    "city",
    "product_name",
    "category",
    "quantity",
    "unit_price",
    "amount",
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
    file_name="filtered_sales_data.csv",
    mime="text/csv",
)
