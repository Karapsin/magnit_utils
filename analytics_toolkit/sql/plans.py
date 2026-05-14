from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .labels import apply_query_label


@dataclass(frozen=True)
class SqlStatement:
    sql: str
    alias: str | None = None
    backend: str | None = None
    phase: str | None = None
    target_table: str | None = None
    source_table: str | None = None


@dataclass
class SqlOperationMetadata:
    source_rows: int | None = None
    staged_rows: int | None = None
    inserted_rows: int | None = None
    affected_rows: int | None = None
    final_target_rows: int | None = None
    stage_table: str | None = None

    def as_dict(self) -> dict[str, int | str | None]:
        return {
            "source_rows": self.source_rows,
            "staged_rows": self.staged_rows,
            "inserted_rows": self.inserted_rows,
            "affected_rows": self.affected_rows,
            "final_target_rows": self.final_target_rows,
            "stage_table": self.stage_table,
        }


@dataclass
class SqlPlan:
    operation: str
    statements: list[SqlStatement] = field(default_factory=list)
    source_alias: str | None = None
    target_alias: str | None = None
    source_backend: str | None = None
    target_backend: str | None = None
    source_table: str | None = None
    target_table: str | None = None
    options: dict[str, Any] = field(default_factory=dict)
    metadata: SqlOperationMetadata = field(default_factory=SqlOperationMetadata)

    @property
    def sqls(self) -> list[str]:
        return [statement.sql for statement in self.statements]

    def add(
        self,
        sql: str,
        *,
        alias: str | None = None,
        backend: str | None = None,
        phase: str | None = None,
        target_table: str | None = None,
        source_table: str | None = None,
        query_label: str | None = None,
    ) -> None:
        self.statements.append(
            SqlStatement(
                sql=apply_query_label(sql, query_label),
                alias=alias,
                backend=backend,
                phase=phase,
                target_table=target_table,
                source_table=source_table,
            )
        )

    def extend(
        self,
        statements: list[str],
        *,
        alias: str | None = None,
        backend: str | None = None,
        phase: str | None = None,
        target_table: str | None = None,
        source_table: str | None = None,
        query_label: str | None = None,
    ) -> None:
        for statement in statements:
            self.add(
                statement,
                alias=alias,
                backend=backend,
                phase=phase,
                target_table=target_table,
                source_table=source_table,
                query_label=query_label,
            )


@dataclass
class SqlOperationResult:
    rows: int | None
    metadata: SqlOperationMetadata
    plan: SqlPlan | None = None

    @property
    def inserted_rows(self) -> int | None:
        return self.metadata.inserted_rows

    @property
    def affected_rows(self) -> int | None:
        return self.metadata.affected_rows
