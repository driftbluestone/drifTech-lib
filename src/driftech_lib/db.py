"""
Module for interacting with PostgreSQL
"""
import psycopg
from typing import Any
from psycopg import sql
from . import logging
logger = logging.Logger("db")

__all__ = ["SCHEMA", "start", "run", "single", "multiple", "insert", "delete", "get", "table", "logger"]

SCHEMA: sql.Identifier = None
cursor: psycopg.Cursor = None
connection: psycopg.Connection = None

def table(name: str, columns: list[str]):
    """
    Create a table. Columns should be formatted like `column_name TYPE` e.g. `server_id BIGINT PRIMARY KEY` or `data JSONB`
    """
    run(f"CREATE TABLE IF NOT EXISTS {SCHEMA.as_string()}.{name} ({", ".join(columns)});")
    logger.info(f"Created table {name} if it didn't already exist")

def check_connection():
    cursor.execute("SELECT version();")
    db_version = cursor.fetchone()
    
    logger.info("Connection Successful")
    logger.info(f"PostgreSQL version: {db_version[0]}")

def close_connection():
    cursor.close()
    connection.close()
    logger.info("Database connection closed.")

def run(*args):
    cursor.execute(*args)
    connection.commit()

def single(*args):
    cursor.execute(*args)
    result = cursor.fetchone()
    if isinstance(result, tuple) and len(result) == 1:
        return result[0]
    return result

def multiple(*args):
    cursor.execute(*args)
    return cursor.fetchall()

def insert(table: str, key: tuple[str, ...], field: tuple[str, ...], value: tuple[Any, ...]):
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
    run(query, value)

def delete(table: str, key: tuple[str, ...], value: tuple[Any, ...]):
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
    run(query, value)

def get(table: str, value: tuple[Any], key: tuple[str], column: tuple[str]):
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
    
    return single(query, value)

def start(schema: str, host: str, name: str, user: str, password: str, port: int):
    global SCHEMA, connection, cursor
    SCHEMA = sql.Identifier(schema)
    logger.info("Connecting to database...")
    connection = psycopg.connect(
        host=host,
        dbname=name,
        user=user,
        password=password,
        port=port
    )
    cursor = connection.cursor()
    check_connection()
    logger.info("Connected to database.")
    run(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA.as_string()};")
    logger.info(f"Created schema {schema} if it didn't already exist")
