"""Configuration from environment variables (and `.env` in development)."""
from __future__ import annotations

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    gcp_project: str = "financial-market-analysis"
    bq_location: str = "US"
    marts_dataset: str = "analytics_finance"
    # BigQuery bills at least 10 MB per table read; the marts are a few MB.
    max_bytes_billed: int = 200_000_000
    # The pipeline loads once per weekday, so an hour-old answer is still current.
    cache_ttl_seconds: int = 3600
    query_cache_size: int = 512

    # Comma-separated list of frontend origins allowed by CORS.
    allowed_origins: str = "http://localhost:3000"

    @property
    def cors_origins(self) -> list[str]:
        return [origin.strip() for origin in self.allowed_origins.split(",") if origin.strip()]

    @property
    def marts(self) -> str:
        """Fully qualified marts dataset, ready to prefix table names in SQL."""
        return f"`{self.gcp_project}.{self.marts_dataset}`"


@lru_cache
def get_settings() -> Settings:
    return Settings()
