from __future__ import annotations

from pathlib import Path

import pytest

from finsight.config import Settings
from finsight.errors import ConfigError


def test_defaults_work_without_configuration() -> None:
    settings = Settings.from_env({})
    assert settings.storage == "local"
    assert settings.data_dir == Path("data")
    assert settings.max_workers == 4
    assert not settings.snowflake.configured


def test_reads_environment() -> None:
    settings = Settings.from_env(
        {
            "FINSIGHT_STORAGE": "S3",
            "FINSIGHT_S3_BUCKET": "market",
            "FINSIGHT_S3_PREFIX": "/team/finsight/",
            "FINSIGHT_S3_ENDPOINT_URL": "http://localhost:9000",
            "AWS_ACCESS_KEY_ID": "key",
            "AWS_SECRET_ACCESS_KEY": "s3cr3t-value",
            "FINSIGHT_MAX_WORKERS": "8",
            "SNOWFLAKE_ACCOUNT": "acct",
            "SNOWFLAKE_USER": "me",
            "SNOWFLAKE_PASSWORD": "pw",
        }
    )
    assert settings.storage == "s3"
    assert settings.s3_prefix == "team/finsight"
    assert settings.max_workers == 8
    assert settings.snowflake.configured
    described = settings.describe()
    assert described["aws_secret_access_key"] == "********"
    assert "s3cr3t-value" not in str(described)


def test_s3_requires_bucket() -> None:
    with pytest.raises(ConfigError, match="FINSIGHT_S3_BUCKET"):
        Settings.from_env({"FINSIGHT_STORAGE": "s3"})


@pytest.mark.parametrize(
    ("env", "message"),
    [
        ({"FINSIGHT_STORAGE": "ftp"}, "must be 'local' or 's3'"),
        ({"FINSIGHT_MAX_WORKERS": "lots"}, "must be an integer"),
        ({"FINSIGHT_MAX_WORKERS": "0"}, "must be >= 1"),
    ],
)
def test_invalid_values(env: dict[str, str], message: str) -> None:
    with pytest.raises(ConfigError, match=message):
        Settings.from_env(env)


def test_dotenv_is_loaded(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    (tmp_path / ".env").write_text("FINSIGHT_MAX_WORKERS=7\n")
    monkeypatch.delenv("FINSIGHT_MAX_WORKERS", raising=False)
    assert Settings.from_env().max_workers == 7
