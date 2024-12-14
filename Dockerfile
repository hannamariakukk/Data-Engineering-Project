FROM apache/airflow:2.6.3

# Install Kaggle API and any additional Python packages
RUN pip install --no-cache-dir kaggle

# Install DuckDB module
RUN pip install duckdb

# Install Geopy
RUN pip install geopy

# Optionally copy the Kaggle configuration file if needed
COPY ./kaggle/kaggle.json /opt/airflow/kaggle.json

ENV KAGGLE_CONFIG_DIR=/opt/airflow

ENV KAGGLE_USERNAME=xubanarrieta

ENV KAGGLE_KEY=fef8a534c80f0da6a075e20cefbacca2
