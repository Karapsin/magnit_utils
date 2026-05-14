from __future__ import annotations

from ..connection.config import BackendName, resolve_connection_backend
from ..connection.errors import UnsupportedConnectionTypeError
from .base import BackendAdapter, UNSUPPORTED_BACKEND_MESSAGE
from .clickhouse import ClickHouseAdapter
from .gp import GreenplumAdapter
from .trino import TrinoAdapter


BACKEND_ADAPTERS: dict[BackendName, BackendAdapter] = {
    "gp": GreenplumAdapter(),
    "trino": TrinoAdapter(),
    "ch": ClickHouseAdapter(),
}


def get_backend_adapter(connection_type_or_key: str) -> BackendAdapter:
    backend = resolve_connection_backend(connection_type_or_key)
    try:
        return BACKEND_ADAPTERS[backend]
    except KeyError as exc:
        raise UnsupportedConnectionTypeError(UNSUPPORTED_BACKEND_MESSAGE) from exc
