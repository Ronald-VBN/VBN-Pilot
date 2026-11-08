"""Dagster migration of the EmballageTool staging-to-history ADF pipeline."""
from __future__ import annotations

import os

from dagster import job, op
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

CONNECTION_URI_ENV = "CDS_ANALYTICS_DB_CONNECTION_URI"

_COLUMNS = (
    "verzender",
    "datum",
    "ontvanger",
    "type",
    "bonnr",
    "product",
    "palletid",
    "aantalpallets",
    "fustid",
    "aantalfust",
    "gewichtperfust",
    "totaalgewicht",
    "gebruiker",
    "zendingsnummer",
    "opmerking",
    "loaddatetime",
    "pipelinerunid",
)
_KEY_COLUMNS = ("verzender", "datum", "ontvanger", "gebruiker")
_UPDATE_COLUMNS = tuple(column for column in _COLUMNS if column not in _KEY_COLUMNS)
_COLUMN_LIST = ", ".join(f'"{column}"' for column in _COLUMNS)
_SELECT_COLUMNS = ", ".join(f's."{column}"' for column in _COLUMNS)
_CONFLICT_COLUMNS = ", ".join(f'"{column}"' for column in _KEY_COLUMNS)
_UPDATE_SET = ", ".join(
    f'"{column}" = EXCLUDED."{column}"' for column in _UPDATE_COLUMNS
)

_DEDUPLICATE_STAGING_SQL = """
WITH ranked AS (
    SELECT ctid,
           row_number() OVER (
               PARTITION BY verzender, datum, ontvanger, gebruiker
               ORDER BY ctid
           ) AS row_number
    FROM stg.tbl_emballagetool_data
)
DELETE FROM stg.tbl_emballagetool_data AS staging
USING ranked
WHERE staging.ctid = ranked.ctid
  AND ranked.row_number > 1
"""

_CREATE_HISTORY_SCHEMA_SQL = "CREATE SCHEMA IF NOT EXISTS his"

_CREATE_HISTORY_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS his.tbl_emballagetool_data (
    verzender text NOT NULL,
    datum timestamp(6) without time zone NOT NULL,
    ontvanger text NOT NULL,
    "type" text,
    bonnr text,
    product text,
    palletid text,
    aantalpallets integer,
    fustid text,
    aantalfust integer,
    gewichtperfust double precision,
    totaalgewicht double precision,
    gebruiker text NOT NULL,
    zendingsnummer text,
    opmerking text,
    loaddatetime timestamp(6) without time zone,
    pipelinerunid text,
    PRIMARY KEY (verzender, datum, ontvanger, gebruiker)
)
"""

_UPSERT_HISTORY_SQL = f"""
INSERT INTO his.tbl_emballagetool_data ({_COLUMN_LIST})
SELECT {_SELECT_COLUMNS}
FROM stg.tbl_emballagetool_data AS s
ON CONFLICT ({_CONFLICT_COLUMNS}) DO UPDATE
SET {_UPDATE_SET}
"""


def _create_postgres_engine() -> Engine:
    connection_uri = os.environ.get(CONNECTION_URI_ENV)
    if not connection_uri:
        raise RuntimeError(
            f"Set {CONNECTION_URI_ENV} to the Postgres analytics database URI."
        )
    if not connection_uri.partition(":")[0].lower().startswith("postgres"):
        raise ValueError(f"{CONNECTION_URI_ENV} must be a PostgreSQL connection URI.")
    return create_engine(connection_uri, hide_parameters=True, pool_pre_ping=True)


@op
def remove_emballage_staging_duplicates(context) -> int:
    engine = _create_postgres_engine()
    try:
        with engine.begin() as connection:
            result = connection.execute(text(_DEDUPLICATE_STAGING_SQL))
            deleted_rows = result.rowcount
    finally:
        engine.dispose()

    context.log.info("Removed %s duplicate staging row(s)", deleted_rows)
    return deleted_rows


@op
def merge_emballage_staging_to_history(context, deleted_rows: int) -> int:
    engine = _create_postgres_engine()
    try:
        with engine.begin() as connection:
            connection.execute(text(_CREATE_HISTORY_SCHEMA_SQL))
            connection.execute(text(_CREATE_HISTORY_TABLE_SQL))
            result = connection.execute(text(_UPSERT_HISTORY_SQL))
            merged_rows = result.rowcount
    finally:
        engine.dispose()

    context.log.info(
        "Upserted %s EmballageTool row(s) into history after removing %s duplicate row(s)",
        merged_rows,
        deleted_rows,
    )
    return merged_rows


@op
def empty_emballage_staging_tables(context, merged_rows: int) -> None:
    engine = _create_postgres_engine()
    try:
        with engine.begin() as connection:
            connection.execute(text("TRUNCATE TABLE stg.tbl_emballagetool_data"))
    finally:
        engine.dispose()

    context.log.info(
        "Emptied EmballageTool staging table after merging %s row(s)", merged_rows
    )


@job(name="load_emballagetool_stg_to_his")
def load_emballagetool_stg_to_his():
    deleted_rows = remove_emballage_staging_duplicates()
    merged_rows = merge_emballage_staging_to_history(deleted_rows)
    empty_emballage_staging_tables(merged_rows)
