"""
Asyncio implementation of db.
"""
import asyncio, psycopg
from typing import Any
from psycopg import sql
from . import logging
logger = logging.Logger("db")

__all__ = ["SCHEMA", "start", "run", "single", "multiple", "insert", "delete", "get", "table", "logger"]

SCHEMA: sql.Identifier = None
cursor: psycopg.Cursor = None
connection: psycopg.Connection = None

async def table(name: str, columns: list[str]):
    """
    Create a table. Columns should be formatted like `column_name TYPE` e.g. `server_id BIGINT PRIMARY KEY` or `data JSONB`
    """
    await run(f"CREATE TABLE IF NOT EXISTS {SCHEMA.as_string()}.{name} ({", ".join(columns)});")
    logger.info(f"Created table {name} if it didn't already exist")

async def check_connection():
    await cursor.execute("SELECT version();")
    db_version = await cursor.fetchone()
    
    logger.info("Connection Successful")
    logger.info(f"PostgreSQL version: {db_version[0]}")

async def close_connection():
    await cursor.close()
    await connection.close()
    logger.info("Database connection closed.")

async def run(*args):
    await cursor.execute(*args)
    await connection.commit()

async def single(*args):
    await cursor.execute(*args)
    result = await cursor.fetchone()
    if isinstance(result, tuple) and len(result) == 1:
        return result[0]
    return result

async def multiple(*args):
    await cursor.execute(*args)
    return await cursor.fetchall()

async def insert(table: str, key: tuple[str, ...], field: tuple[str, ...], value: tuple[Any, ...]):
    """Actually an upsert function."""
    if not isinstance(key, tuple):
        raise ValueError("Argument `key` must be a tuple.")
    if not isinstance(field, tuple):
        raise ValueError("Argument `field` must be a tuple.")
    if not isinstance(value, tuple):
        raise ValueError("Argument `value` must be a tuple.")
    if len(key) + len(field) != len(value):
        raise ValueError("len(key) + len(field) must equal len(value).")
    if not key:
        raise ValueError("Arguments `key`, `field`, and `value` cannot be empty.")
    
    query = "INSERT INTO {schema}.{table} ({fields}) VALUES ({values}) ON CONFLICT ({keys}) DO"
    if field:
        query += " UPDATE SET {assignments}"
    else:
        query += " NOTHING"

    query = sql.SQL(query).format(
        schema = SCHEMA,
        table = sql.Identifier(table),
        fields = sql.SQL(", ").join(sql.Identifier(f) for f in (key + field)),
        values = sql.SQL(", ").join(sql.Placeholder() for _ in value),
        keys = sql.SQL(", ").join(sql.Identifier(k) for k in key),
        assignments = sql.SQL(", ").join(sql.SQL("{f} = EXCLUDED.{f}").format(
            f=sql.Identifier(f)) for f in field if f not in key
        )
    )
    await run(query, value)

async def delete(table: str, key: tuple[str, ...], value: tuple[Any, ...]):
    if not isinstance(key, tuple):
        raise ValueError("Argument `key` must be a tuple")
    if not isinstance(value, tuple):
        raise ValueError("Argument `value` must be a tuple")
    if len(key) != len(value):
        raise ValueError("Arguments `key` and `value` must be equal in length")
    if not key:
        raise ValueError("Arguments `key` and `value` cannot be empty")
    
    query = sql.SQL("DELETE FROM {schema}.{table} WHERE {key}").format(
        schema = SCHEMA,
        table = sql.Identifier(table),
        key = sql.SQL(" AND ").join(sql.SQL("{k} = %s").format(
            k = sql.Identifier(k)) for k in key
        )
    )
    await run(query, value)

async def get(table: str, value: tuple[Any], key: tuple[str], column: tuple[str]):
    if not isinstance(key, tuple):
        raise ValueError("Argument `key` must be a tuple")
    if not isinstance(value, tuple):
        raise ValueError("Argument `value` must be a tuple")
    if len(key) != len(value):
        raise ValueError("Arguments `key` and `value` must be equal in length")
    if not key:
        raise ValueError("Arguments `key` and `value` cannot be empty")
    
    query = sql.SQL("SELECT {column} FROM {schema}.{table} WHERE {key}").format(
        schema = SCHEMA,
        table = sql.Identifier(table),
        column = sql.SQL(", ").join(sql.Identifier(c) for c in column), # CANNOT PUT "*" IN HERE
        key = sql.SQL(" AND ").join(sql.SQL("{k} = %s").format(
            k = sql.Identifier(k)) for k in key
        )
    )
    
    return await single(query, value)

async def start(schema: str, host: str, name: str, user: str, password: str, port: int):
    global SCHEMA, connection, cursor
    SCHEMA = sql.Identifier(schema)
    logger.info("Connecting to database...")
    connection = await psycopg.AsyncConnection.connect(
        host=host,
        dbname=name,
        user=user,
        password=password,
        port=port
    )
    cursor = connection.cursor()
    await check_connection()
    logger.info("Connected to database.")
    await run(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA.as_string()};")
    logger.info(f"Created schema {schema} if it didn't already exist")

asyncio.run(start("test", "localhost", "postgres", "postgres", "asdfjkl", "5432"), loop_factory=asyncio.SelectorEventLoop)