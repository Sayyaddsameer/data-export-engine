"""
config.py — Application settings loaded from environment variables.
All configuration is via env vars (12-factor); no hardcoded values.
"""
from pydantic_settings import BaseSettings, SettingsConfigDict
from functools import lru_cache


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # ── Database ───────────────────────────────────────────────────────────────
    database_url: str                         # e.g. postgresql://user:pw@db:5432/exports_db
    db_pool_min_size: int = 2
    db_pool_max_size: int = 10
    db_chunk_size: int = 1000                 # rows per cursor fetch; controls memory

    # ── Application ───────────────────────────────────────────────────────────
    port: int = 8080
    log_level: str = "INFO"

    # ── Parquet ───────────────────────────────────────────────────────────────
    parquet_row_group_size: int = 50_000      # rows per Parquet row group before flush


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return a cached singleton Settings instance."""
    return Settings()
