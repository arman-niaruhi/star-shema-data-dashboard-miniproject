# CI/CD

This project now includes a GitHub Actions workflow at `.github/workflows/ci-cd.yml`.

What it does:
- installs Python dependencies from `dashboard/requirements.txt`
- imports source data into staging and rebuilds the starschema business datasets
- validates that the expected CSV outputs exist
- smoke-tests the Streamlit app import
- commits refreshed `data/` artifacts back to `main` when they change

To make the online Streamlit app update reliably:
- deploy the app from this same repository
- point the hosting platform to the `main` branch
- use `dashboard/app.py` as the app entrypoint

Important note:
- the hosted app only updates after the platform redeploys from the new commit
- if you use Streamlit Community Cloud, make sure the app is connected to `main`
