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

# Collect new data
We will also need to collect and analyze Azure data.

# Bring it into Airflow
We will need to run the task from Airflow server and not from local machine.

# Refactor for cloud
Describe (in code or in documentation) how would you refactor those 2 jobs on production server.