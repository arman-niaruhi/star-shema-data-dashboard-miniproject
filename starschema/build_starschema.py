from pathlib import Path
import pandas as pd

ROOT_DIR = Path(__file__).resolve().parents[1]
STAGING_FILE = ROOT_DIR / "data" / "staging" / "source_sales.csv"
STARSCHEMA_DIR = ROOT_DIR / "data" / "starschema"


def load_staging() -> pd.DataFrame:
    if not STAGING_FILE.exists():
        raise FileNotFoundError(f"Missing staging file: {STAGING_FILE}")
    return pd.read_csv(STAGING_FILE)


def transform_staging_data(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    required_columns = [
        "order_id",
        "order_date",
        "customer_id",
        "customer_name",
        "city",
        "country",
        "product_id",
        "product_name",
        "category",
        "quantity",
        "unit_price",
    ]
    missing = [c for c in required_columns if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    text_cols = [
        "customer_id",
        "customer_name",
        "city",
        "country",
        "product_id",
        "product_name",
        "category",
    ]
    for col in text_cols:
        df[col] = df[col].astype(str).str.strip()

    df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")
    df["order_id"] = pd.to_numeric(df["order_id"], errors="coerce")
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")
    df["unit_price"] = pd.to_numeric(df["unit_price"], errors="coerce")

    df = df.dropna(subset=[
        "order_id",
        "order_date",
        "customer_id",
        "customer_name",
        "product_id",
        "product_name",
        "quantity",
        "unit_price",
    ])

    df = df[(df["quantity"] > 0) & (df["unit_price"] >= 0)]
    df = df.drop_duplicates()

    df["order_id"] = df["order_id"].astype(int)
    df["amount"] = df["quantity"] * df["unit_price"]
    df["date_key"] = df["order_date"].dt.strftime("%Y%m%d").astype(int)

    return df.sort_values(["order_date", "order_id"]).reset_index(drop=True)


def build_dim_customer(df: pd.DataFrame) -> pd.DataFrame:
    dim_customer = (
        df[["customer_id", "customer_name", "city", "country"]]
        .drop_duplicates()
        .sort_values("customer_id")
        .reset_index(drop=True)
    )
    dim_customer["customer_key"] = range(1, len(dim_customer) + 1)
    return dim_customer[["customer_key", "customer_id", "customer_name", "city", "country"]]


def build_dim_product(df: pd.DataFrame) -> pd.DataFrame:
    dim_product = (
        df[["product_id", "product_name", "category"]]
        .drop_duplicates()
        .sort_values("product_id")
        .reset_index(drop=True)
    )
    dim_product["product_key"] = range(1, len(dim_product) + 1)
    return dim_product[["product_key", "product_id", "product_name", "category"]]


def build_dim_date(df: pd.DataFrame) -> pd.DataFrame:
    dim_date = pd.DataFrame({
        "full_date": sorted(df["order_date"].dropna().drop_duplicates())
    }).reset_index(drop=True)

    dim_date["date_key"] = dim_date["full_date"].dt.strftime("%Y%m%d").astype(int)
    dim_date["year"] = dim_date["full_date"].dt.year
    dim_date["quarter"] = dim_date["full_date"].dt.quarter
    dim_date["month"] = dim_date["full_date"].dt.month
    dim_date["month_name"] = dim_date["full_date"].dt.month_name()
    dim_date["day"] = dim_date["full_date"].dt.day
    dim_date["day_name"] = dim_date["full_date"].dt.day_name()
    dim_date["week_of_year"] = dim_date["full_date"].dt.isocalendar().week.astype(int)

    return dim_date[
        [
            "date_key",
            "full_date",
            "year",
            "quarter",
            "month",
            "month_name",
            "day",
            "day_name",
            "week_of_year",
        ]
    ]


def build_fact_sales(
    df: pd.DataFrame,
    dim_customer: pd.DataFrame,
    dim_product: pd.DataFrame,
) -> pd.DataFrame:
    fact = df.copy()

    fact = fact.merge(
        dim_customer[["customer_id", "customer_key"]],
        on="customer_id",
        how="left",
    )

    fact = fact.merge(
        dim_product[["product_id", "product_key"]],
        on="product_id",
        how="left",
    )

    fact_sales = fact[
        [
            "order_id",
            "customer_key",
            "product_key",
            "date_key",
            "quantity",
            "unit_price",
            "amount",
        ]
    ].copy()

    fact_sales = fact_sales.rename(columns={"order_id": "sales_id"})
    fact_sales = fact_sales.sort_values("sales_id").reset_index(drop=True)
    return fact_sales


def save_starschema(
    dim_customer: pd.DataFrame,
    dim_product: pd.DataFrame,
    dim_date: pd.DataFrame,
    fact_sales: pd.DataFrame,
) -> None:
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
