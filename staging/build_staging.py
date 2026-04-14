from pathlib import Path
import pandas as pd
from datetime import datetime, timedelta
import random

ROOT_DIR = Path(__file__).resolve().parents[1]
STAGING_DIR = ROOT_DIR / "data" / "staging"
STAGING_FILE = STAGING_DIR / "source_sales.csv"


def import_source_data(row_count: int = 10000) -> pd.DataFrame:
    rng = random.Random(42)

    customers = [
        ("C001", "Anna Schmidt", "Berlin", "Germany"),
        ("C002", "John Miller", "Hamburg", "Germany"),
        ("C003", "Sofia Rossi", "Milan", "Italy"),
        ("C004", "Liam Dupont", "Paris", "France"),
        ("C005", "Maya Chen", "Amsterdam", "Netherlands"),
    ]
    products = [
        ("P001", "Laptop Pro 14", "Electronics"),
        ("P002", "Wireless Mouse", "Accessories"),
        ("P003", "Office Chair", "Furniture"),
        ("P004", "Standing Desk", "Furniture"),
        ("P005", "4K Monitor", "Electronics"),
    ]
    start_date = datetime(2023, 1, 1)

    rows = []
    for order_id in range(1, row_count + 1):
        customer_id, customer_name, city, country = rng.choice(customers)
        product_id, product_name, category = rng.choice(products)
        quantity = rng.randint(1, 8)
        unit_price = round(rng.uniform(25, 1500), 2)
        order_date = start_date + timedelta(days=rng.randint(0, 729))

        rows.append(
            {
                "order_id": order_id,
                "order_date": order_date.strftime("%Y-%m-%d"),
                "customer_id": customer_id,
                "customer_name": customer_name,
                "city": city,
                "country": country,
                "product_id": product_id,
                "product_name": product_name,
                "category": category,
                "quantity": quantity,
                "unit_price": unit_price,
            }
        )

    return pd.DataFrame(rows)


def save_staging(df: pd.DataFrame) -> None:
    STAGING_DIR.mkdir(parents=True, exist_ok=True)
    df.to_csv(STAGING_FILE, index=False)
    print(f"Imported source data written to staging: {STAGING_FILE} ({len(df)} rows)")
    


if __name__ == "__main__":
    source_df = import_source_data()
    save_staging(source_df)
