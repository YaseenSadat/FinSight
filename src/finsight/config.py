"""Runtime configuration.

All settings are read from environment variables (optionally loaded from a
``.env`` file in the working directory). Defaults are chosen so that FinSight
works out of the box with no configuration at all: data is written to
``./data`` as Parquet and queried through DuckDB.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal, cast

from finsight.errors import ConfigError

StorageBackend = Literal["local", "s3"]


def _get(env: Mapping[str, str], name: str, default: str | None = None) -> str | None:
    value = env.get(name)
    if value is None or value.strip() == "":
        return default
    return value.strip()


def _get_int(env: Mapping[str, str], name: str, default: int, minimum: int = 1) -> int:
    raw = _get(env, name)
    if raw is None:
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise ConfigError(f"{name} must be an integer, got {raw!r}") from exc
    if value < minimum:
        raise ConfigError(f"{name} must be >= {minimum}, got {value}")
    return value


@dataclass(frozen=True)
class SnowflakeSettings:
    """Connection settings for the optional Snowflake sink."""

    account: str | None = None
    user: str | None = None
    password: str | None = None
    warehouse: str | None = None
    database: str = "FINSIGHT"
    schema: str = "MARKET_DATA"
    role: str | None = None

    @property
    def configured(self) -> bool:
        """True when enough settings are present to open a connection."""
        return bool(self.account and self.user and self.password)


@dataclass(frozen=True)
class Settings:
    """Resolved FinSight configuration.

    Construct directly for programmatic use, or call :meth:`from_env` to read
    the standard ``FINSIGHT_*`` environment variables.
    """

    storage: StorageBackend = "local"
    data_dir: Path = Path("data")
    s3_bucket: str | None = None
    s3_prefix: str = ""
    s3_endpoint_url: str | None = None
    s3_region: str = "us-east-1"
    aws_access_key_id: str | None = None
    aws_secret_access_key: str | None = None
    max_workers: int = 4
    max_retries: int = 3
    spark_master: str = "local[*]"
    spark_driver_host: str | None = None
    spark_jars_dir: str | None = None
    snowflake: SnowflakeSettings = field(default_factory=SnowflakeSettings)

    def __post_init__(self) -> None:
        if self.storage not in ("local", "s3"):
            raise ConfigError(f"FINSIGHT_STORAGE must be 'local' or 's3', got {self.storage!r}")
        if self.storage == "s3" and not self.s3_bucket:
            raise ConfigError("FINSIGHT_S3_BUCKET is required when FINSIGHT_STORAGE=s3")

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None, *, dotenv: bool = True) -> Settings:
        """Build settings from environment variables.

        Args:
            env: Mapping to read from. Defaults to ``os.environ``.
            dotenv: When reading ``os.environ``, first load a ``.env`` file
                from the current directory (existing variables win).
        """
        if env is None:
            if dotenv:
                from dotenv import find_dotenv, load_dotenv

                load_dotenv(find_dotenv(usecwd=True), override=False)
            env = os.environ

        storage = (_get(env, "FINSIGHT_STORAGE", "local") or "local").lower()
        return cls(
            storage=cast(StorageBackend, storage),
            data_dir=Path(_get(env, "FINSIGHT_DATA_DIR", "data") or "data").expanduser(),
            s3_bucket=_get(env, "FINSIGHT_S3_BUCKET"),
            s3_prefix=(_get(env, "FINSIGHT_S3_PREFIX", "") or "").strip("/"),
            s3_endpoint_url=_get(env, "FINSIGHT_S3_ENDPOINT_URL"),
            s3_region=_get(env, "FINSIGHT_S3_REGION", "us-east-1") or "us-east-1",
            aws_access_key_id=_get(env, "AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=_get(env, "AWS_SECRET_ACCESS_KEY"),
            max_workers=_get_int(env, "FINSIGHT_MAX_WORKERS", 4),
            max_retries=_get_int(env, "FINSIGHT_MAX_RETRIES", 3),
            spark_master=_get(env, "FINSIGHT_SPARK_MASTER", "local[*]") or "local[*]",
            spark_driver_host=_get(env, "FINSIGHT_SPARK_DRIVER_HOST"),
            spark_jars_dir=_get(env, "FINSIGHT_SPARK_JARS_DIR"),
            snowflake=SnowflakeSettings(
                account=_get(env, "SNOWFLAKE_ACCOUNT"),
                user=_get(env, "SNOWFLAKE_USER"),
                password=_get(env, "SNOWFLAKE_PASSWORD"),
                warehouse=_get(env, "SNOWFLAKE_WAREHOUSE"),
                database=_get(env, "SNOWFLAKE_DATABASE", "FINSIGHT") or "FINSIGHT",
                schema=_get(env, "SNOWFLAKE_SCHEMA", "MARKET_DATA") or "MARKET_DATA",
                role=_get(env, "SNOWFLAKE_ROLE"),
            ),
        )

    def describe(self) -> dict[str, Any]:
        """Return a printable view of the settings with secrets masked."""

        def mask(value: str | None) -> str | None:
            return "********" if value else None

        info: dict[str, Any] = {"storage": self.storage}
        if self.storage == "local":
            info["data_dir"] = str(self.data_dir.resolve())
        else:
            info.update(
                s3_bucket=self.s3_bucket,
                s3_prefix=self.s3_prefix or None,
                s3_endpoint_url=self.s3_endpoint_url,
                s3_region=self.s3_region,
                aws_access_key_id=mask(self.aws_access_key_id),
                aws_secret_access_key=mask(self.aws_secret_access_key),
            )
        info.update(
            max_workers=self.max_workers,
            max_retries=self.max_retries,
            spark_master=self.spark_master,
            snowflake_configured=self.snowflake.configured,
        )
        return info
