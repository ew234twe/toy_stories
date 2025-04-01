# Set up environment
## Set up with `uv`

Run following commands
```bash
uv sync --dev
uv pip install "apache-airflow==2.10.5" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.12.txt" && \
  uv run airflow db migrate
```

Verify local environments
```bash
uv run ruff check
uv run pytest
```

# Pandas problem
My `simple_etl.py` doesn't work and I don't know why.

# Rewrite transformation to SQL
Historical data is now stored in Redshift, but logic is still written in `pandas`. Need to translate it to SQL.

# Bring it into Airflow
We will need to run the task from Airflow server and not from local machine

# Connect to database
Database is not ready but we know it will be of Postgres-flavored (so Redshift, Postgres .etc.). Production database will be locked down anyway so we should find a way to develop and test without over-relying on access to production