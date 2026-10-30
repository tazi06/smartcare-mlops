FROM apache/airflow:3.2.0
RUN pip install --no-cache-dir duckdb pyarrow
RUN python -c "import duckdb; duckdb.install_extension('httpfs')"