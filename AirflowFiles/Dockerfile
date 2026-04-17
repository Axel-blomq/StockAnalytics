FROM apache/airflow:2.8.1-python3.10

USER root
RUN apt-get update && apt-get install -y build-essential

USER airflow
RUN pip install --no-cache-dir \
    yfinance \
    pandas \
    requests \
    cassandra-driver \
    prophet