# ERP Star Schema Dashboard

This project is a small data engineering and analytics pipeline built around an ERP-style sales dataset.

It simulates source-system CSV files, loads them into a staging layer, transforms them into a business-facing star schema, and exposes the result in a Streamlit dashboard.

The dashboard is available online at: https://starschema-dashboard.streamlit.app/

## What This Project Is

This project is an end-to-end mini data platform for sales analytics:

- `staging/` creates ERP source CSV files
- `starschema/` transforms those source files into dimensional model tables
- `dashboard/` reads the star schema and shows KPIs, trends, and business breakdowns
- `.github/workflows/update-starschema.yml` automates the refresh pipeline in GitHub Actions

In practice, it demonstrates a simple warehouse-style flow:

1. ERP source data is represented as separate CSV extracts.
2. Those CSVs are stored in the staging layer.
3. The staging data is transformed into dimension and fact tables.
4. The dashboard reads the star schema for analysis.

## Current Data Flow

The staging layer writes these ERP source files into `data/staging/`:

- `erp_customers.csv`
- `erp_materials.csv`
- `erp_orders.csv`
- `erp_fulfillment.csv`

The star-schema layer reads those files and produces:

- `data/starschema/dim_customer.csv`
- `data/starschema/dim_product.csv`
- `data/starschema/dim_date.csv`
- `data/starschema/fact_sales.csv`

## Project Structure

```text
.
├── compose.yml
├── dashboard/
│   ├── app.py
│   └── requirements.txt
├── staging/
│   ├── build_staging.py
│   └── Dockerfile
├── starschema/
│   ├── build_starschema.py
│   └── Dockerfile
└── .github/workflows/
    └── update-starschema.yml
```

## How To Run

### 1. Generate staging source files

```bash
python staging/build_staging.py
```

### 2. Build the star schema

```bash
python starschema/build_starschema.py
```

### 3. Run the dashboard

```bash
pip install -r dashboard/requirements.txt
streamlit run dashboard/app.py
```

Hosted dashboard:

https://starschema-dashboard.streamlit.app/

### 4. Or run the pipeline with Docker Compose

```bash
docker compose up --build
```

## Dashboard Content

The dashboard shows:

- executive KPIs such as revenue, profit, discounts, and shipping days
- commercial breakdowns by product category, brand, material, customer segment, and channel
- monthly revenue and profit trends
- ERP-oriented views such as sales organization, payment terms, plant, and fulfillment performance
- detailed transactional records from the star schema

## Automation

The GitHub Actions workflow at `.github/workflows/update-starschema.yml`:

- builds the staging ERP CSV source files
- rebuilds the star schema
- validates the generated outputs
- smoke-tests the Streamlit app import
- commits refreshed data artifacts back to `main` when they change

## Summary

This is a mini data engineering project that models ERP sales data into a star schema and presents it in a dashboard.

It is useful as a portfolio project for:

- data engineering pipelines
- dimensional modeling
- CSV-based ETL/ELT workflows
- dashboarding with Streamlit
- CI/CD automation for data refreshes
