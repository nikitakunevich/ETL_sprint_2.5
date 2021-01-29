import argparse
import contextlib
import sqlite3

import psycopg2
from psycopg2.extras import DictCursor

from etl import (fetch_sqlite_data, migrate_data_to_new_schema,
                 sqlite_dict_connection_factory, write_data_to_postgres)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Migration script for getting movie data from sqlite db "
                    "and storing it in PostgreSQL in different scheme."
    )

    parser.add_argument("--from", dest="sqlite_db_path", default="db.sqlite",
                        help="Путь к файлу SQLite с данными для миграции.", required=True)
    parser.add_argument("--to", dest="postgres_dsn", default="dbname=postgres user=postgres",
                        help="DSN в postgreSQL формате для подключения к базе.", required=True)
    parser.add_argument("--init", dest="postgres_init_sql", default="postgres_init.sql",
                        help="Скрипт с инициализацией новой схемы таблиц.", required=True)
    return parser.parse_args()


if __name__ == "__main__":
    print("Start migrating data from SQLite to PostgreSQL")
    args = parse_args()

    # noinspection PyTypeChecker
    with contextlib.closing(sqlite3.connect(args.sqlite_db_path,
                                            factory=sqlite_dict_connection_factory)) as sqlite_connection:
        old_schema_data = fetch_sqlite_data(sqlite_connection)

    processed_data = migrate_data_to_new_schema(old_schema_data)

    psycopg2.extras.register_uuid()
    with psycopg2.connect(args.postgres_dsn) as postgres_connection:
        # run init script
        with postgres_connection.cursor() as curs:
            init_sql = open(args.postgres_init_sql, encoding="utf-8").read()
            curs.execute(init_sql)
        write_data_to_postgres(processed_data, postgres_connection)

    print(f"Migrated {len(processed_data.film_works)} movies, {len(processed_data.persons)} persons, {len(processed_data.genres)} genres from SQLite to PostgreSQL")
