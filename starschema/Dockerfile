FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir pandas==2.2.3

COPY build_starschema.py /app/build_starschema.py

CMD ["python", "/app/build_starschema.py"]
