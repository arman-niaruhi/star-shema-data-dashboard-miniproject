from pathlib import Path

import pandas as pd

ROOT_DIR = Path(__file__).resolve().parents[1]
STAGING_DIR = ROOT_DIR / "data" / "staging"
STARSCHEMA_DIR = ROOT_DIR / "data" / "starschema"


def load_staging() -> pd.DataFrame:
    customers_file = STAGING_DIR / "erp_customers.csv"
    materials_file = STAGING_DIR / "erp_materials.csv"
    orders_file = STAGING_DIR / "erp_orders.csv"
    fulfillment_file = STAGING_DIR / "erp_fulfillment.csv"

    required_files = [customers_file, materials_file, orders_file, fulfillment_file]
    missing = [str(path) for path in required_files if not path.exists()]
    if missing:
        raise FileNotFoundError(f"Missing staging files: {missing}")

    customers = pd.read_csv(customers_file)
    materials = pd.read_csv(materials_file)
    orders = pd.read_csv(orders_file)
    fulfillment = pd.read_csv(fulfillment_file)

    return (
        orders
        .merge(customers, on="sold_to_customer", how="left")
        .merge(materials, on="material_id", how="left")
        .merge(fulfillment, on=["sales_document", "sales_document_item"], how="left")
    )


def transform_staging_data(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    required_columns = [
        "sales_document",
        "document_type",
        "sales_document_item",
        "order_date",
        "requested_delivery_date",
        "company_code",
        "sales_organization",
        "distribution_channel",
        "division",
        "plant",
        "storage_location",
        "sold_to_customer",
        "customer_name",
        "customer_city",
        "customer_country",
        "customer_segment",
        "customer_group",
        "material_id",
        "material_description",
        "product_category",
        "brand",
        "material_group",
        "shipping_condition",
        "order_priority",
        "payment_terms",
        "incoterms",
        "order_quantity",
        "net_unit_price",
        "discount_pct",
    ]
    missing = [c for c in required_columns if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    text_cols = [c for c in required_columns if c not in {"sales_document", "sales_document_item", "order_quantity", "net_unit_price", "discount_pct"} and c not in {"order_date", "requested_delivery_date"}]
    for col in text_cols:
        df[col] = df[col].astype(str).str.strip()

    df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")
    df["requested_delivery_date"] = pd.to_datetime(df["requested_delivery_date"], errors="coerce")
    df["sales_document"] = pd.to_numeric(df["sales_document"], errors="coerce")
    df["sales_document_item"] = pd.to_numeric(df["sales_document_item"], errors="coerce")
    df["order_quantity"] = pd.to_numeric(df["order_quantity"], errors="coerce")
    df["net_unit_price"] = pd.to_numeric(df["net_unit_price"], errors="coerce")
    df["discount_pct"] = pd.to_numeric(df["discount_pct"], errors="coerce").fillna(0)

    df = df.dropna(subset=["sales_document", "order_date", "sold_to_customer", "material_id", "order_quantity", "net_unit_price"])
    df = df[(df["order_quantity"] > 0) & (df["net_unit_price"] >= 0) & (df["discount_pct"] >= 0) & (df["discount_pct"] <= 1)]
    df = df.drop_duplicates()

    df["sales_document"] = df["sales_document"].astype(int)
    df["sales_document_item"] = df["sales_document_item"].astype(int)
    df["date_key"] = df["order_date"].dt.strftime("%Y%m%d").astype(int)
    df["gross_amount"] = df["order_quantity"] * df["net_unit_price"]
    df["discount_amount"] = df["gross_amount"] * df["discount_pct"]
    df["amount"] = df["gross_amount"] - df["discount_amount"]
    df["unit_cost"] = (df["net_unit_price"] * 0.58).round(2)
    df["cost_amount"] = df["order_quantity"] * df["unit_cost"]
    df["profit"] = df["amount"] - df["cost_amount"]
    df["shipping_days"] = (df["requested_delivery_date"] - df["order_date"]).dt.days.clip(lower=0)
    return df.sort_values(["order_date", "sales_document", "sales_document_item"]).reset_index(drop=True)


def build_dim_customer(df: pd.DataFrame) -> pd.DataFrame:
    dim_customer = df[["sold_to_customer", "customer_name", "customer_city", "customer_country", "customer_segment", "customer_group", "sales_organization", "distribution_channel"]].drop_duplicates().sort_values("sold_to_customer").reset_index(drop=True)
    dim_customer["customer_key"] = range(1, len(dim_customer) + 1)
    return dim_customer[["customer_key", "sold_to_customer", "customer_name", "customer_city", "customer_country", "customer_segment", "customer_group", "sales_organization", "distribution_channel"]]


def build_dim_product(df: pd.DataFrame) -> pd.DataFrame:
    dim_product = df[["material_id", "material_description", "product_category", "brand", "material_group", "division"]].drop_duplicates().sort_values("material_id").reset_index(drop=True)
    dim_product["product_key"] = range(1, len(dim_product) + 1)
    return dim_product[["product_key", "material_id", "material_description", "product_category", "brand", "material_group", "division"]]


def build_dim_date(df: pd.DataFrame) -> pd.DataFrame:
    dim_date = pd.DataFrame({"full_date": sorted(df["order_date"].dropna().drop_duplicates())}).reset_index(drop=True)
    dim_date["date_key"] = dim_date["full_date"].dt.strftime("%Y%m%d").astype(int)
    dim_date["year"] = dim_date["full_date"].dt.year
    dim_date["quarter"] = dim_date["full_date"].dt.quarter
    dim_date["month"] = dim_date["full_date"].dt.month
    dim_date["month_name"] = dim_date["full_date"].dt.month_name()
    dim_date["day"] = dim_date["full_date"].dt.day
    dim_date["day_name"] = dim_date["full_date"].dt.day_name()
    dim_date["week_of_year"] = dim_date["full_date"].dt.isocalendar().week.astype(int)
    return dim_date[["date_key", "full_date", "year", "quarter", "month", "month_name", "day", "day_name", "week_of_year"]]


def build_fact_sales(df: pd.DataFrame, dim_customer: pd.DataFrame, dim_product: pd.DataFrame) -> pd.DataFrame:
    fact = df.merge(dim_customer[["sold_to_customer", "customer_key"]], on="sold_to_customer", how="left")
    fact = fact.merge(dim_product[["material_id", "product_key"]], on="material_id", how="left")
    fact_sales = fact[["sales_document", "sales_document_item", "customer_key", "product_key", "date_key", "company_code", "sales_organization", "distribution_channel", "plant", "storage_location", "shipping_condition", "order_priority", "payment_terms", "incoterms", "order_quantity", "net_unit_price", "discount_pct", "gross_amount", "discount_amount", "amount", "cost_amount", "profit", "shipping_days"]].copy()
    fact_sales = fact_sales.rename(columns={"sales_document": "sales_id", "order_quantity": "quantity", "net_unit_price": "unit_price", "shipping_condition": "shipping_mode"})
    return fact_sales.sort_values(["sales_id", "sales_document_item"]).reset_index(drop=True)


def save_starschema(dim_customer: pd.DataFrame, dim_product: pd.DataFrame, dim_date: pd.DataFrame, fact_sales: pd.DataFrame) -> None:
    STARSCHEMA_DIR.mkdir(parents=True, exist_ok=True)
    dim_customer.to_csv(STARSCHEMA_DIR / "dim_customer.csv", index=False)
    dim_product.to_csv(STARSCHEMA_DIR / "dim_product.csv", index=False)
    dim_date.to_csv(STARSCHEMA_DIR / "dim_date.csv", index=False)
    fact_sales.to_csv(STARSCHEMA_DIR / "fact_sales.csv", index=False)
    print(f"Starschema files written in: {STARSCHEMA_DIR}")


if __name__ == "__main__":
    staging_df = load_staging()
    transformed_df = transform_staging_data(staging_df)
    dim_customer = build_dim_customer(transformed_df)
    dim_product = build_dim_product(transformed_df)
    dim_date = build_dim_date(transformed_df)
    fact_sales = build_fact_sales(transformed_df, dim_customer, dim_product)
    save_starschema(dim_customer, dim_product, dim_date, fact_sales)
