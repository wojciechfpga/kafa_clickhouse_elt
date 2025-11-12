FROM apache/airflow:2.10.0

USER root

# Zainstaluj systemowe zależności potrzebne do confluent-kafka (librdkafka)
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Skopiuj i zainstaluj zależności Pythona
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
