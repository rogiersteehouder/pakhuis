"""Pakhuis database initialization."""

# ruff: noqa: E501

import sqlite3

def init_db(conn: sqlite3.Connection, version: tuple[int, int]) -> None:
    """Pakhuis database initialization."""
    if version < (1, 0):
        conn.executescript(
            """
            begin;
            create table CONFIG ("VERSION" text);
            create table PAKHUIS ("BIN" text, "ID" text, "DTTM" text default CURRENT_TIMESTAMP, "STATUS" text default "A", "CONTENT" text);
            create table SEARCH_KEYS ("BIN" text, "KEY" text, "PATH" text, "TYPE" text);
            create table SEARCH_VALUES ("BIN" text, "KEY" text, "ID" text, "VALUE" text);
            insert into CONFIG values ('1.0');
            end;
            """
        )
    if version < (1, 1):
        conn.executescript(
            """
            begin;
            create table BIN_CONFIG ("BIN" text, "INCLUDE_ID" text default "N");
            update CONFIG set VERSION = '1.1';
            commit;
            """
        )
    if version < (1, 2):
        conn.executescript(
            """
            begin;
            create table PAKHUIS_OLD as select * from PAKHUIS;
            drop table PAKHUIS;
            create table PAKHUIS ("BIN" text, "ID" text, "DTTM" datetime default CURRENT_TIMESTAMP, "STATUS" text default "A", "CONTENT" text);
            insert into PAKHUIS select BIN, ID, DTTM, STATUS, CONTENT from PAKHUIS_OLD;
            drop table PAKHUIS_OLD;
            update CONFIG set VERSION = '1.2';
            commit;
            """
        )
