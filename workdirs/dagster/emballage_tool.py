"""Dagster migration of the EmballageTool staging-to-history ADF pipeline."""
from __future__ import annotations

import os

from dagster import job, op
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

CONNECTION_URI_ENV = "EMBALLAGETOOL_DWH_CONNECTION_URI"
SOURCE_SCHEMA_ENV = "EMBALLAGETOOL_SOURCE_SCHEMA"
DEFAULT_SOURCE_SCHEMA = "EmballageTool"

_LOOKUP_SOURCE_TABLES_SQL = """
SELECT DISTINCT
    [SourceSchema],
    [SourceTable]
FROM [generate].[SourceColumns]
WHERE [SourceSchema] = :source_schema
"""

_REMOVE_DUPLICATES_SQL = """
WITH CTE AS (
    SELECT
        [Verzender],
        [Datum],
        [Ontvanger],
        [Type],
        [BonNR],
        [Product],
        [PalletID],
        [AantalPallets],
        [FustID],
        [AantalFust],
        [GewichtPerFust],
        [TotaalGewicht],
        [Gebruiker],
        [Zendingsnummer],
        RN = ROW_NUMBER() OVER (
            PARTITION BY [Datum], [Gebruiker], [Verzender], [Ontvanger]
            ORDER BY [Datum], [Gebruiker], [Verzender], [Ontvanger]
        )
    FROM [STG].[tbl_EmballageTool_Data]
)
DELETE FROM CTE WHERE RN > 1
"""


def _create_sql_server_engine() -> Engine:
    connection_uri = os.environ.get(CONNECTION_URI_ENV)
    if not connection_uri:
        raise RuntimeError(
            f"Set {CONNECTION_URI_ENV} to a SQLAlchemy SQL Server connection URI."
        )
    if not connection_uri.partition(":")[0].lower().startswith("mssql"):
        raise ValueError(
            f"{CONNECTION_URI_ENV} must use a SQLAlchemy SQL Server URI "
            "(for example, mssql+pyodbc://...)."
        )
    return create_engine(connection_uri, hide_parameters=True, pool_pre_ping=True)


def _source_schema() -> str:
    source_schema = os.environ.get(SOURCE_SCHEMA_ENV, DEFAULT_SOURCE_SCHEMA)
    if not source_schema:
        raise ValueError(f"{SOURCE_SCHEMA_ENV} must not be empty.")
    return source_schema


def _quote_sql_server_identifier(identifier: str) -> str:
    if not identifier or "\x00" in identifier or len(identifier) > 128:
        raise ValueError(f"Invalid SQL Server identifier: {identifier!r}.")
    return f"[{identifier.replace(']', ']]')}]"


@op
def lookup_emballage_source_tables(context) -> list[str]:
    source_schema = _source_schema()
    engine = _create_sql_server_engine()
    try:
        with engine.connect() as connection:
            rows = connection.execute(
                text(_LOOKUP_SOURCE_TABLES_SQL),
                {"source_schema": source_schema},
            ).mappings()
            tables: list[str] = []
            for row in rows:
                row_schema = row["SourceSchema"]
                table_name = row["SourceTable"]
                if not isinstance(row_schema, str) or not isinstance(table_name, str):
                    raise ValueError(
                        "SourceColumns returned a null or non-string SourceSchema/SourceTable."
                    )
                if row_schema != source_schema:
                    raise ValueError(
                        "SourceColumns returned a schema different from the requested "
                        f"schema {source_schema!r}."
                    )
                tables.append(table_name)
    finally:
        engine.dispose()

    context.log.info("Found %s source table(s) in schema %s", len(tables), source_schema)
    return tables


@op
def remove_emballage_staging_duplicates(context) -> int:
    engine = _create_sql_server_engine()
    try:
        with engine.begin() as connection:
            result = connection.execute(text(_REMOVE_DUPLICATES_SQL))
            deleted_rows = result.rowcount
    finally:
        engine.dispose()

    context.log.info("Removed %s duplicate staging row(s)", deleted_rows)
    return deleted_rows


@op
def merge_emballage_staging_to_history(
    context,
    source_tables: list[str],
    deleted_rows: int,
) -> str:
    source_schema = _source_schema()
    context.log.info(
        "Merging %s table(s) into history after removing %s duplicate row(s)",
        len(source_tables),
        deleted_rows,
    )

    engine = _create_sql_server_engine()
    try:
        for table_name in source_tables:
            procedure_name = f"sp_{source_schema}_{table_name}_Merge"
            statement = text(
                f"EXEC [HIS].{_quote_sql_server_identifier(procedure_name)} "
                "@IsDeltaload = :is_deltaload"
            )
            with engine.begin() as connection:
                connection.execute(statement, {"is_deltaload": True})
            context.log.info("Merged source table %s.%s", source_schema, table_name)
    finally:
        engine.dispose()

    return source_schema


@op
def empty_emballage_staging_tables(context, source_schema: str) -> None:
    engine = _create_sql_server_engine()
    try:
        with engine.begin() as connection:
            connection.execute(
                text(
                    "EXEC [STG].[sp_Empty_STG_Tabels] "
                    "@SourceSchema = :source_schema"
                ),
                {"source_schema": source_schema},
            )
    finally:
        engine.dispose()

    context.log.info("Emptied staging tables for schema %s", source_schema)


@job(name="load_emballagetool_stg_to_his")
def load_emballagetool_stg_to_his():
    source_tables = lookup_emballage_source_tables()
    deleted_rows = remove_emballage_staging_duplicates()
    source_schema = merge_emballage_staging_to_history(source_tables, deleted_rows)
    empty_emballage_staging_tables(source_schema)
