from datetime import datetime, timedelta
from pathlib import Path
import random
from typing import Tuple

import pandas as pd

ROOT_DIR = Path(__file__).resolve().parents[1]
STAGING_DIR = ROOT_DIR / "data" / "staging"

CUSTOMERS_FILE = STAGING_DIR / "erp_customers.csv"
MATERIALS_FILE = STAGING_DIR / "erp_materials.csv"
ORDERS_FILE = STAGING_DIR / "erp_orders.csv"
FULFILLMENT_FILE = STAGING_DIR / "erp_fulfillment.csv"


def build_master_data():
    customers = pd.DataFrame(
        [
            ("C001", "Anna Schmidt", "Berlin", "Germany", "Enterprise", "B2B"),
            ("C002", "John Miller", "Hamburg", "Germany", "SMB", "B2B"),
            ("C003", "Sofia Rossi", "Milan", "Italy", "Consumer", "Retail"),
            ("C004", "Liam Dupont", "Paris", "France", "Enterprise", "B2B"),
            ("C005", "Maya Chen", "Amsterdam", "Netherlands", "SMB", "Wholesale"),
            ("C006", "Noah Jensen", "Copenhagen", "Denmark", "Consumer", "Retail"),
            ("C007", "Elena Novak", "Prague", "Czech Republic", "SMB", "Wholesale"),
            ("C008", "Lucas Silva", "Lisbon", "Portugal", "Enterprise", "B2B"),
            ("C009", "Amelia Brown", "Dublin", "Ireland", "Consumer", "Retail"),
            ("C010", "Yara Haddad", "Brussels", "Belgium", "SMB", "B2B"),
        ],
        columns=[
            "sold_to_customer",
            "customer_name",
            "customer_city",
            "customer_country",
            "customer_segment",
            "customer_group",
        ],
    )

    materials = pd.DataFrame(
        [
            ("MAT001", "Laptop Pro 14", "Electronics", "Northwind", "Hardware", "Technology"),
            ("MAT002", "Wireless Mouse", "Accessories", "Northwind", "Peripherals", "Technology"),
            ("MAT003", "Office Chair", "Furniture", "UrbanWorks", "Office", "Workspace"),
            ("MAT004", "Standing Desk", "Furniture", "UrbanWorks", "Office", "Workspace"),
            ("MAT005", "4K Monitor", "Electronics", "Visionix", "Displays", "Technology"),
            ("MAT006", "USB-C Dock", "Accessories", "Visionix", "Peripherals", "Technology"),
            ("MAT007", "Noise-Cancel Headset", "Audio", "SoundFlow", "Collaboration", "Collaboration"),
            ("MAT008", "Conference Camera", "Audio", "SoundFlow", "Collaboration", "Collaboration"),
        ],
        columns=[
            "material_id",
            "material_description",
            "product_category",
            "brand",
            "material_group",
            "division",
        ],
    )
    return customers, materials


def build_transactions(row_count: int = 15000) -> Tuple[pd.DataFrame, pd.DataFrame]:
    rng = random.Random(42)
    sales_orgs = ["DE01", "EU01", "EU02"]
    distribution_channels = ["Online", "Retail", "Partner", "Inside Sales"]
    company_codes = ["1000", "2000"]
    plants = ["BER1", "AMS1", "PAR1"]
    storage_locations = ["WH-A", "WH-B", "WH-C"]
    payment_terms = ["NET30", "NET45", "PREPAID", "COD"]
    incoterms = ["EXW", "DAP", "FCA"]
    document_types = ["OR", "ZOR"]
    order_priorities = ["Low", "Medium", "High", "Critical"]
    shipping_modes = ["Standard", "Express", "Pickup"]
    customer_ids = [f"C{i:03d}" for i in range(1, 11)]
    material_ids = [f"MAT{i:03d}" for i in range(1, 9)]
    start_date = datetime(2023, 1, 1)

    orders = []
    fulfillment = []
    for sales_doc in range(1, row_count + 1):
        shipping_mode = rng.choice(shipping_modes)
        order_date = start_date + timedelta(days=rng.randint(0, 729))
        requested_days = {"Standard": 6, "Express": 2, "Pickup": 0}[shipping_mode]
        requested_delivery_date = order_date + timedelta(days=max(0, requested_days + rng.randint(-1, 2)))

        orders.append(
            {
                "sales_document": sales_doc,
                "document_type": rng.choice(document_types),
                "sales_document_item": 10,
                "order_date": order_date.strftime("%Y-%m-%d"),
                "company_code": rng.choice(company_codes),
                "sales_organization": rng.choice(sales_orgs),
                "distribution_channel": rng.choice(distribution_channels),
                "sold_to_customer": rng.choice(customer_ids),
                "material_id": rng.choice(material_ids),
                "order_priority": rng.choice(order_priorities),
                "payment_terms": rng.choice(payment_terms),
                "incoterms": rng.choice(incoterms),
                "order_quantity": rng.randint(1, 12),
                "net_unit_price": round(rng.uniform(35, 1800), 2),
                "discount_pct": rng.choice([0, 0, 0, 5, 10, 15, 20]) / 100,
            }
        )

        fulfillment.append(
            {
                "sales_document": sales_doc,
                "sales_document_item": 10,
                "requested_delivery_date": requested_delivery_date.strftime("%Y-%m-%d"),
                "plant": rng.choice(plants),
                "storage_location": rng.choice(storage_locations),
                "shipping_condition": shipping_mode,
            }
        )

    return pd.DataFrame(orders), pd.DataFrame(fulfillment)


def save_staging(customers: pd.DataFrame, materials: pd.DataFrame, orders: pd.DataFrame, fulfillment: pd.DataFrame) -> None:
    STAGING_DIR.mkdir(parents=True, exist_ok=True)
    customers.to_csv(CUSTOMERS_FILE, index=False)
    materials.to_csv(MATERIALS_FILE, index=False)
    orders.to_csv(ORDERS_FILE, index=False)
    fulfillment.to_csv(FULFILLMENT_FILE, index=False)
    print(f"ERP source files written to staging: {STAGING_DIR}")


if __name__ == "__main__":
    customers_df, materials_df = build_master_data()
    orders_df, fulfillment_df = build_transactions()
    save_staging(customers_df, materials_df, orders_df, fulfillment_df)
