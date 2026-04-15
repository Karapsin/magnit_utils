from __future__ import annotations

import os
import subprocess
from functools import wraps
from pathlib import Path
from typing import Any, Callable

from dotenv import load_dotenv

from .config import ChConfig, GpConfig, TrinoConfig, get_connection_config
from .errors import SqlConfigError, UnsupportedConnectionTypeError
from ..general.logging import time_print


def get_sql_connection(connection_type: str) -> Any:
    _load_dotenv()
    normalized_type = connection_type.strip().lower()
    config = get_connection_config(normalized_type)
    time_print(f"Opening {normalized_type} connection")

    if isinstance(config, TrinoConfig):
        return _get_trino_connection(config)
    if isinstance(config, GpConfig):
        return _get_gp_connection(config)
    if isinstance(config, ChConfig):
        return _get_ch_connection(config)

    raise UnsupportedConnectionTypeError(
        "Unsupported connection type. Expected one of: 'trino', 'gp', 'ch'."
    )


def with_sql_connection(connection_type: str) -> Callable[..., Any]:
    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            connection = get_sql_connection(connection_type)
            try:
                return func(connection, *args, **kwargs)
            finally:
                time_print(f"Closing {connection_type} connection")
                connection.close()

        return wrapper

    return decorator


def _get_trino_connection(config: TrinoConfig) -> Any:
    try:
        import trino
        from trino.auth import BasicAuthentication
    except ImportError as exc:
        raise ImportError(
            "The 'trino' package is required for Trino connections."
        ) from exc

    verify_value = config.verify_value
    if config.use_keychain_certs:
        verify_value = str(_build_trino_keychain_bundle())

    if config.auth_mode == "oauth2":
        auth = trino.auth.OAuth2Authentication()
    elif config.auth_mode == "basic":
        auth = BasicAuthentication(config.user, config.password) if config.password else None
    else:
        raise SqlConfigError(
            "Unsupported TRINO_AUTH_MODE. Expected 'basic' or 'oauth2'."
        )

    connect_kwargs = {
        "host": config.host,
        "port": config.port,
        "user": config.user,
        "http_scheme": config.http_scheme,
        "auth": auth,
        "verify": _parse_verify_value(verify_value),
    }
    if config.catalog:
        connect_kwargs["catalog"] = config.catalog
    if config.schema:
        connect_kwargs["schema"] = config.schema

    return trino.dbapi.connect(**connect_kwargs)


def _get_gp_connection(config: GpConfig) -> Any:
    try:
        import psycopg2
    except ImportError as exc:
        raise ImportError(
            "The 'psycopg2' package is required for Greenplum connections."
        ) from exc

    return psycopg2.connect(
        host=config.host,
        port=config.port,
        user=config.user,
        password=config.password,
        dbname=config.database,
    )


def _get_ch_connection(config: ChConfig) -> Any:
    try:
        import clickhouse_connect
    except ImportError as exc:
        raise ImportError(
            "The 'clickhouse-connect' package is required for ClickHouse connections."
        ) from exc

    client_kwargs = {
        "host": config.host,
        "port": config.port,
        "username": config.user,
        "password": config.password,
        "secure": config.secure,
    }
    if config.database:
        client_kwargs["database"] = config.database

    return clickhouse_connect.get_client(**client_kwargs)


def _parse_verify_value(value: str) -> bool | str:
    normalized = value.strip()
    lowered = normalized.lower()
    if lowered == "true":
        return True
    if lowered == "false":
        return False
    return normalized


def _build_trino_keychain_bundle() -> Path:
    certs_dir = _state_dir() / "certs"
    certs_dir.mkdir(parents=True, exist_ok=True)
    bundle_path = certs_dir / "trino-keychain-ca.pem"
    keychains = [
        str(Path.home() / "Library/Keychains/login.keychain-db"),
        "/Library/Keychains/System.keychain",
    ]
    cert_names = _trino_keychain_cert_names()

    certificates: list[str] = []
    for cert_name in cert_names:
        certificate = _export_keychain_certificate(cert_name, keychains)
        if not certificate:
            raise SqlConfigError(
                f"Could not export '{cert_name}' from macOS Keychain."
            )
        certificates.append(certificate.strip())
    bundle_contents = "\n".join(certificates) + "\n"
    if not bundle_path.exists() or bundle_path.read_text(encoding="utf-8") != bundle_contents:
        bundle_path.write_text(bundle_contents, encoding="utf-8")
    return bundle_path


def _export_keychain_certificate(cert_name: str, keychains: list[str]) -> str:
    for keychain in keychains:
        if not Path(keychain).exists():
            continue

        result = subprocess.run(
            [
                "security",
                "find-certificate",
                "-a",
                "-c",
                cert_name,
                "-p",
                keychain,
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode == 0 and "BEGIN CERTIFICATE" in result.stdout:
            return result.stdout

    return ""


def _load_dotenv() -> None:
    env_path = _dotenv_path()
    if env_path is not None:
        load_dotenv(dotenv_path=env_path, override=True)


def _trino_keychain_cert_names() -> list[str]:
    raw_value = os.getenv("TRINO_KEYCHAIN_CERT_NAMES", "")
    cert_names = [name.strip() for name in raw_value.split("|") if name.strip()]
    if not cert_names:
        raise SqlConfigError(
            "Missing required environment variable: TRINO_KEYCHAIN_CERT_NAMES"
        )
    return cert_names


def _dotenv_path() -> Path | None:
    env_override = os.getenv("MAGNIT_UTILS_ENV_FILE")
    if env_override:
        env_path = Path(env_override).expanduser()
        if env_path.exists():
            return env_path

    current_dir = Path.cwd().resolve()
    for directory in (current_dir, *current_dir.parents):
        env_path = directory / ".env"
        if env_path.exists():
            return env_path

    package_env = _package_root_dir() / ".env"
    if package_env.exists():
        return package_env

    return None


def _state_dir() -> Path:
    state_override = os.getenv("MAGNIT_UTILS_HOME")
    if state_override:
        return Path(state_override).expanduser()

    env_path = _dotenv_path()
    if env_path is not None:
        return env_path.parent

    return Path.cwd().resolve()


def _package_root_dir() -> Path:
    return Path(__file__).resolve().parents[2]
