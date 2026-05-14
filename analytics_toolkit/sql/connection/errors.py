from __future__ import annotations

from dataclasses import dataclass


class SqlUtilsError(Exception):
    pass


class UnsupportedConnectionTypeError(SqlUtilsError):
    pass


class InvalidSqlInputError(SqlUtilsError):
    pass


class SqlConfigError(SqlUtilsError):
    pass


@dataclass(frozen=True)
class SqlOperationContext:
    operation: str
    alias: str | None = None
    backend: str | None = None
    phase: str | None = None
    target_table: str | None = None
    source_table: str | None = None
    retry_attempt: int | None = None
    sql_preview: str | None = None


class SqlOperationError(SqlUtilsError):
    def __init__(self, message: str, context: SqlOperationContext) -> None:
        super().__init__(message)
        self.context = context


def sql_preview(sql: str | None, max_chars: int = 500) -> str | None:
    if sql is None:
        return None
    normalized = " ".join(sql.strip().split())
    if len(normalized) <= max_chars:
        return normalized
    return normalized[: max_chars - 3] + "..."


def annotate_sql_exception(exc: Exception, context: SqlOperationContext) -> Exception:
    setattr(exc, "sql_context", context)
    try:
        exc.add_note(_format_context_note(context))
    except AttributeError:
        pass
    return exc


def operation_error(exc: Exception, context: SqlOperationContext) -> SqlOperationError:
    details = [context.operation]
    if context.alias:
        details.append(f"alias={context.alias}")
    if context.backend:
        details.append(f"backend={context.backend}")
    if context.phase:
        details.append(f"phase={context.phase}")
    message = f"SQL operation failed ({', '.join(details)}): {type(exc).__name__}: {exc}"
    return SqlOperationError(message, context)


def _format_context_note(context: SqlOperationContext) -> str:
    parts = [f"operation={context.operation}"]
    if context.alias:
        parts.append(f"alias={context.alias}")
    if context.backend:
        parts.append(f"backend={context.backend}")
    if context.phase:
        parts.append(f"phase={context.phase}")
    if context.target_table:
        parts.append(f"target_table={context.target_table}")
    if context.source_table:
        parts.append(f"source_table={context.source_table}")
    if context.retry_attempt is not None:
        parts.append(f"retry_attempt={context.retry_attempt}")
    if context.sql_preview:
        parts.append(f"sql_preview={context.sql_preview}")
    return "SQL context: " + ", ".join(parts)
