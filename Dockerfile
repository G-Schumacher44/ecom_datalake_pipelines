# =============================================================================
# Production-ready Airflow image with e-commerce pipeline dependencies
# =============================================================================
# Base: Official Airflow 2.9.3 with Python 3.12
FROM apache/airflow:2.9.3-python3.12

# Switch to root for system package installation
USER root

# Install system dependencies
RUN apt-get update && apt-get install -y \
    # Google Cloud SDK (for bq, gsutil)
    curl \
    gnupg \
    lsb-release \
    && echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" \
    | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list \
    && curl https://packages.cloud.google.com/apt/doc/apt-key.gpg \
    | apt-key --keyring /usr/share/keyrings/cloud.google.gpg add - \
    && apt-get update \
    && apt-get install -y google-cloud-sdk \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Switch back to airflow user
USER airflow

# Set working directory
WORKDIR /opt/airflow

# Copy dependency files first (for layer caching)
COPY --chown=airflow:root pyproject.toml ./

# Install Python dependencies
RUN pip install --no-cache-dir --upgrade pip setuptools wheel && \
    # Install core dependencies
    pip install --no-cache-dir -e ".[airflow]" && \
    # Install dbt packages for BigQuery (prod) and DuckDB (local)
    pip install --no-cache-dir dbt-bigquery>=1.7.0

# Copy application code
COPY --chown=airflow:root src/ ./src/
COPY --chown=airflow:root config/ ./config/
COPY --chown=airflow:root dbt_duckdb/ ./dbt_duckdb/
COPY --chown=airflow:root dbt_bigquery/ ./dbt_bigquery/

# Install dbt dependencies (dbt-utils, etc.)
RUN cd dbt_duckdb && dbt deps --project-dir . --profiles-dir . && \
    cd ../dbt_bigquery && dbt deps --project-dir . --profiles-dir .

# Create necessary directories for local runs
RUN mkdir -p \
    data/silver/base \
    data/silver/enriched \
    data/metrics \
    data/logs \
    docs/validation_reports

# Health check: Verify Python package is importable
RUN python -c "from src.settings import load_settings; print('✅ Package installed successfully')"

# Set environment defaults (can be overridden in docker-compose or Cloud Composer)
ENV PIPELINE_ENV=local \
    AIRFLOW__CORE__LOAD_EXAMPLES=False \
    AIRFLOW__WEBSERVER__EXPOSE_CONFIG=True

# Default command (overridden by docker-compose)
CMD ["airflow", "webserver"]
