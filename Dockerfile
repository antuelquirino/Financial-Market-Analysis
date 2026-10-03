# The Financial Market API for Cloud Run. Only the API and its runtime
# dependencies; extraction, dbt and the web app stay out of the image.

# --- Build stage: install dependencies into an isolated prefix --------------
FROM python:3.11-slim AS build

ENV PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

COPY api/requirements.txt /tmp/requirements.txt
RUN pip install --prefix=/install -r /tmp/requirements.txt

# --- Runtime stage: the slim base, the installed packages and the code ------
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PORT=8080

COPY --from=build /install /usr/local
WORKDIR /app
COPY api/ api/

# No credentials in the image: on Cloud Run the API runs as its own service
# account and the BigQuery client finds it through the metadata server.
RUN useradd --create-home market
USER market

CMD exec uvicorn api.main:app --host 0.0.0.0 --port "${PORT}" --proxy-headers --forwarded-allow-ips="*"
