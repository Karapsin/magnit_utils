from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, cast

from .errors import SqlConfigError, UnsupportedConnectionTypeError


BackendName = Literal["trino", "gp", "ch"]
SUPPORTED_BACKENDS: set[str] = {"trino", "gp", "ch"}
CONNECTIONS_FILE_NAME = ".connections"


@dataclass(frozen=True)
class TrinoConfig:
    connection_key: str
    backend: BackendName
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
    keychain_cert_names: list[str]
    insert_chunk_size: int | None


@dataclass(frozen=True)
class GpConfig:
    connection_key: str
    backend: BackendName
    host: str
    port: int
    user: str
    password: str
    database: str


@dataclass(frozen=True)
class ChConfig:
    connection_key: str
    backend: BackendName
    host: str
    port: int
    user: str
    password: str
    database: str | None
    secure: bool


ConnectionConfig = TrinoConfig | GpConfig | ChConfig


def get_connection_config(connection_key: str) -> ConnectionConfig:
    normalized_key = normalize_connection_key(connection_key)
    raw_config = _get_raw_connection_config(normalized_key)
    backend = _require_backend(normalized_key, raw_config)

    if backend == "trino":
        return TrinoConfig(
            connection_key=normalized_key,
            backend=backend,
            host=_require_string(raw_config, normalized_key, "host"),
            port=_optional_int(raw_config, normalized_key, "port", 8080),
            user=_require_string(raw_config, normalized_key, "user"),
            password=_optional_string(raw_config, normalized_key, "password"),
            catalog=_optional_string(raw_config, normalized_key, "catalog"),
            schema=_optional_string(raw_config, normalized_key, "schema"),
            auth_mode=_optional_string(
                raw_config,
                normalized_key,
                "auth_mode",
                "basic",
            ).lower(),
            http_scheme=_optional_string(
                raw_config,
                normalized_key,
                "http_scheme",
                "http",
            ),
            verify_value=_optional_string(raw_config, normalized_key, "verify", "true"),
            use_keychain_certs=_optional_bool(
                raw_config,
                normalized_key,
                "use_keychain_certs",
                False,
            ),
            keychain_cert_names=_optional_string_list(
                raw_config,
                normalized_key,
                "keychain_cert_names",
            ),
            insert_chunk_size=_optional_positive_int(
                raw_config,
                normalized_key,
                "insert_chunk_size",
            ),
        )
    if backend == "gp":
        return GpConfig(
            connection_key=normalized_key,
            backend=backend,
            host=_require_string(raw_config, normalized_key, "host"),
            port=_optional_int(raw_config, normalized_key, "port", 5432),
            user=_require_string(raw_config, normalized_key, "user"),
            password=_require_string(raw_config, normalized_key, "password"),
            database=_require_string(raw_config, normalized_key, "database"),
        )
    if backend == "ch":
        return ChConfig(
            connection_key=normalized_key,
            backend=backend,
            host=_require_string(raw_config, normalized_key, "host"),
            port=_optional_int(raw_config, normalized_key, "port", 8123),
            user=_require_string(raw_config, normalized_key, "user"),
            password=_require_string(raw_config, normalized_key, "password"),
            database=_optional_string(raw_config, normalized_key, "database"),
            secure=_optional_bool(raw_config, normalized_key, "secure", False),
        )

    raise UnsupportedConnectionTypeError(
        f"Unsupported backend for SQL connection '{normalized_key}': {backend!r}."
    )


def get_connection_backend(connection_key: str) -> BackendName:
    config = get_connection_config(connection_key)
    return config.backend


def resolve_connection_backend(connection_type_or_key: str) -> BackendName:
    normalized = normalize_connection_key(connection_type_or_key)
    if normalized in SUPPORTED_BACKENDS:
        return cast(BackendName, normalized)
    return get_connection_backend(normalized)


def normalize_connection_key(connection_key: str) -> str:
    normalized = connection_key.strip().lower()
    if not normalized:
        raise SqlConfigError("Connection key must not be empty.")
    return normalized


def load_sql_connections() -> dict[str, dict[str, Any]]:
    connections_path = get_connections_file_path()

    try:
        parsed = json.loads(connections_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise SqlConfigError(
            f"{connections_path} must contain valid JSON."
        ) from exc

    if not isinstance(parsed, dict):
        raise SqlConfigError(f"{connections_path} must contain a JSON object.")

    connections: dict[str, dict[str, Any]] = {}
    for raw_key, raw_config in parsed.items():
        if not isinstance(raw_key, str):
            raise SqlConfigError(f"{connections_path} keys must be strings.")
        normalized_key = normalize_connection_key(raw_key)
        if normalized_key in connections:
            raise SqlConfigError(
                f"Duplicate SQL connection key after normalization: {normalized_key}"
            )
        if not isinstance(raw_config, dict):
            raise SqlConfigError(
                f"{connections_path}['{normalized_key}'] must be a JSON object."
            )
        connections[normalized_key] = raw_config

    return connections


def get_connections_file_path() -> Path:
    connections_path = _find_connections_file_path()
    if connections_path is None:
        raise SqlConfigError(
            f"Missing SQL connections file: {CONNECTIONS_FILE_NAME}. "
            "Place it in the current working directory or one of its parents."
        )
    return connections_path


def _find_connections_file_path() -> Path | None:
    current_dir = Path.cwd().resolve()
    for directory in (current_dir, *current_dir.parents):
        connections_path = directory / CONNECTIONS_FILE_NAME
        if connections_path.is_file():
            return connections_path
    return None


def _get_raw_connection_config(connection_key: str) -> dict[str, Any]:
    connections = load_sql_connections()
    try:
        return connections[connection_key]
    except KeyError as exc:
        available = ", ".join(sorted(connections)) or "<none>"
        raise UnsupportedConnectionTypeError(
            f"Unknown SQL connection key: {connection_key}. "
            f"Available keys: {available}"
        ) from exc


def _require_backend(connection_key: str, config: dict[str, Any]) -> BackendName:
    raw_backend = _require_string(config, connection_key, "type").lower()
    if raw_backend not in SUPPORTED_BACKENDS:
        expected = ", ".join(sorted(SUPPORTED_BACKENDS))
        raise UnsupportedConnectionTypeError(
            f"SQL connection '{connection_key}' has unsupported type {raw_backend!r}. "
            f"Expected one of: {expected}."
        )
    return cast(BackendName, raw_backend)


def _require_string(
    config: dict[str, Any],
    connection_key: str,
    field_name: str,
) -> str:
    value = _optional_string(config, connection_key, field_name)
    if value is None:
        raise SqlConfigError(
            f"SQL connection '{connection_key}' is missing required field: {field_name}"
        )
    return value


def _optional_string(
    config: dict[str, Any],
    connection_key: str,
    field_name: str,
    default: str | None = None,
) -> str | None:
    if field_name not in config:
        return default

    value = config[field_name]
    if value is None:
        return default
    if not isinstance(value, str):
        raise SqlConfigError(
            f"SQL connection '{connection_key}' field '{field_name}' must be a string."
        )

    normalized = value.strip()
    return normalized if normalized else default


def _optional_int(
    config: dict[str, Any],
    connection_key: str,
    field_name: str,
    default: int,
) -> int:
    if field_name not in config or config[field_name] is None:
        return default

    value = config[field_name]
    if isinstance(value, bool):
        raise SqlConfigError(
            f"SQL connection '{connection_key}' field '{field_name}' must be an integer."
        )
    if isinstance(value, int):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = int(value.strip())
        except ValueError as exc:
            raise SqlConfigError(
                f"SQL connection '{connection_key}' field '{field_name}' must be an integer."
            ) from exc
    else:
        raise SqlConfigError(
            f"SQL connection '{connection_key}' field '{field_name}' must be an integer."
        )

    if parsed <= 0:
        raise SqlConfigError(
            f"SQL connection '{connection_key}' field '{field_name}' must be positive."
        )
    return parsed


def _optional_positive_int(
    config: dict[str, Any],
    connection_key: str,
    field_name: str,
) -> int | None:
    if field_name not in config or config[field_name] is None:
        return None

    return _optional_int(config, connection_key, field_name, 1)


def _optional_bool(
    config: dict[str, Any],
    connection_key: str,
    field_name: str,
    default: bool,
) -> bool:
    if field_name not in config or config[field_name] is None:
        return default

    value = config[field_name]
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "1", "yes"}:
            return True
        if normalized in {"false", "0", "no"}:
            return False

    raise SqlConfigError(
        f"SQL connection '{connection_key}' field '{field_name}' must be a boolean."
    )


def _optional_string_list(
    config: dict[str, Any],
    connection_key: str,
    field_name: str,
) -> list[str]:
    value = config.get(field_name)
    if value is None:
        return []
    if isinstance(value, str):
        names = [name.strip() for name in value.split("|")]
    elif isinstance(value, list):
        names = []
        for item in value:
            if not isinstance(item, str):
                raise SqlConfigError(
                    f"SQL connection '{connection_key}' field '{field_name}' "
                    "must contain only strings."
                )
            names.append(item.strip())
    else:
        raise SqlConfigError(
            f"SQL connection '{connection_key}' field '{field_name}' "
            "must be a string or list of strings."
        )

    return [name for name in names if name]
