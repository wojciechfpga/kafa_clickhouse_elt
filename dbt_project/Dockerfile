FROM python:3.11-slim

# Opcjonalne: zainstaluj podstawowe narzędzia
RUN apt-get update && apt-get install -y build-essential curl vim && rm -rf /var/lib/apt/lists/*
RUN apt-get update && apt-get install -y git && rm -rf /var/lib/apt/lists/*
# Instalacja dbt + adaptera ClickHouse
RUN pip install --no-cache-dir dbt-core dbt-clickhouse fastapi uvicorn[standard]

WORKDIR /usr/app
COPY . /usr/app

# W razie potrzeby — zmienna środowiskowa dla dbt
ENV DBT_PROFILES_DIR=/usr/app/.dbt

# Domyślny start: uruchamia Uvicorn z API
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
