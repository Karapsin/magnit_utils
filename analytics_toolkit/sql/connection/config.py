from __future__ import annotations

import os
from dataclasses import dataclass

from .errors import SqlConfigError, UnsupportedConnectionTypeError


@dataclass(frozen=True)
class TrinoConfig:
    host: str
    port: int
    user: str
    password: str | None
    catalog: str | None
    schema: str | None
    auth_mode: str
    http_scheme: str
    verify_value: str
    use_keychain_certs: bool


@dataclass(frozen=True)
class GpConfig:
    host: str
    port: int
    user: str
    password: str
    database: str


@dataclass(frozen=True)
class ChConfig:
    host: str
    port: int
    user: str
    password: str
    database: str | None
    secure: bool


def get_connection_config(connection_type: str) -> TrinoConfig | GpConfig | ChConfig:
    normalized_type = connection_type.strip().lower()
    if normalized_type == "trino":
        return TrinoConfig(
            host=_require_env("TRINO_HOST"),
            port=int(os.getenv("TRINO_PORT", "8080")),
            user=_require_env("TRINO_USER"),
            password=os.getenv("TRINO_PASSWORD"),
            catalog=os.getenv("TRINO_CATALOG"),
            schema=os.getenv("TRINO_SCHEMA"),
            auth_mode=os.getenv("TRINO_AUTH_MODE", "basic").strip().lower(),
            http_scheme=os.getenv("TRINO_HTTP_SCHEME", "http"),
            verify_value=os.getenv("TRINO_VERIFY", "true"),
            use_keychain_certs=os.getenv("TRINO_USE_KEYCHAIN_CERTS", "false").strip().lower() == "true",
        )
    if normalized_type == "gp":
        return GpConfig(
            host=_require_env("GP_HOST"),
            port=int(os.getenv("GP_PORT", "5432")),
            user=_require_env("GP_USER"),
            password=_require_env("GP_PASSWORD"),
            database=_require_env("GP_DATABASE"),
        )
    if normalized_type == "ch":
        return ChConfig(
            host=_require_env("CH_HOST"),
            port=int(os.getenv("CH_PORT", "8123")),
            user=_require_env("CH_USER"),
            password=_require_env("CH_PASSWORD"),
            database=os.getenv("CH_DATABASE"),
            secure=os.getenv("CH_SECURE", "false").lower() == "true",
        )
    raise UnsupportedConnectionTypeError(
        "Unsupported connection type. Expected one of: 'trino', 'gp', 'ch'."
    )


def _require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise SqlConfigError(f"Missing required environment variable: {name}")
    return value
