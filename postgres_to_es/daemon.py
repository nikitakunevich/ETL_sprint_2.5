import sys
import os
import argparse
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import wraps
from typing import List, Optional, Dict, Any, Union, Callable, Sequence

import psycopg2
from elasticsearch import Elasticsearch, helpers
from psycopg2.extras import DictCursor
from psycopg2 import sql
from loguru import logger
from pydantic import BaseModel
from redis import Redis

from postgres_to_es.state import State, RedisState

logger.remove()
logger.add(sys.stderr, level=os.environ.get("LOG_LEVEL", "INFO"))

from postgres_to_es.utils import datetime_to_iso_string, backoff


def coroutine(func):
    """Декоратор для инициализации(priming) корутины."""

    @wraps(func)
    def inner(*args, **kwargs):
        fn = func(*args, **kwargs)
        next(fn)
        return fn

    return inner


ObjectId = str
ObjectName = str


class MovieElastic(BaseModel):
    """Схема для ES документа с фильмами."""
    id: str
    imdb_rating: Optional[float]
    title: str
    description: Optional[str]
    actors_names: List[str]
    writers_names: List[str]
    directors_names: List[str]
    genres_names: List[str]
    actors: List[Dict[ObjectId, ObjectName]]
    writers: List[Dict[ObjectId, ObjectName]]
    directors: List[Dict[ObjectId, ObjectName]]
    genres: List[Dict[ObjectId, ObjectName]]


@backoff()
def query_postgresql(pg_url: str, template: Union[str, sql.Composable], params: Dict[str, Any]) -> List[dict]:
    """Функция для запросов к postgresql."""
    with psycopg2.connect(pg_url) as connection:
        with connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
            cursor.execute(template, params)
            results = [dict(r) for r in cursor.fetchall()]
    return results


def get_updated_postgres_entries(table: str, pg_url: str, target, state: State, batch_size: int = 1000,
                                 timestamp_field: str = 'updated_at',
                                 columns: List[str] = None) -> None:
    """Producer, отправляющий в корутину обновленные записи из таблицы.

    :param table: PostgreSQL таблица, в которой ищутся обновленные записи.
    :param pg_url: URL к PostgreSQL.
    :param target: Корутина-получатель.
    :param state: Объект для сохранения состояния ETL.
    :param batch_size: Размер батча для получения записей из бд.
    :param timestamp_field: Поле, по которому определяются обновленные записи.
    :param columns: столбцы, которые должны быть в ответе.
    """
    # read state
    updated_at = datetime.fromisoformat(
        state.state_get_key(f'{table}.updated_at', datetime_to_iso_string(datetime.fromtimestamp(0, tz=timezone.utc))))
    last_id = state.state_get_key(f'{table}.last_id', str(uuid.UUID(int=0)))

    column_names = ','.join(columns) if columns else '*'
    query = sql.SQL(f"""
            select {column_names}, {timestamp_field}
            from {table}
            where ({timestamp_field} = %(timestamp)s and id > %(last_id)s)
                  or {timestamp_field} > %(timestamp)s
            order by {timestamp_field}, id
            limit {batch_size}
        """)
    rows = query_postgresql(pg_url, query, {'timestamp': updated_at, 'last_id': last_id})

    if rows:
        logger.info("Fetched {} updated rows from table {}", len(rows), table)

        target.send(rows)
        current_last_timestamp = datetime_to_iso_string(rows[-1][timestamp_field])
        current_last_id = str(rows[-1]['id'])
        state.state_set_key(f'{table}.updated_at', current_last_timestamp)
        state.state_set_key(f'{table}.last_id', current_last_id)
        logger.debug("Updated state with updated_at: {}, last_id: {}", current_last_timestamp, current_last_id)
    else:
        logger.debug("No updated rows in table {}", table)


@coroutine
def table_with_fwkey_get_film_ids(film_work_id_field: str, target):
    """Отправляет в target id фильмов из поля film_work_id_field."""
    while rows := (yield):
        target.send([row[film_work_id_field] for row in rows])


@coroutine
def get_films_ids_by_join(pg_url: str, join_table: str, join_field: str, target):
    """Отправляет в target id фильмов, полученных пересечением входящих id с записями в join_table по join_field."""
    while rows := (yield):
        ids = [row['id'] for row in rows]
        query = f"""
        SELECT t.film_work_id as id
        FROM {join_table} t
        WHERE t.{join_field} = ANY(%(ids)s::uuid[])
        """
        rows = query_postgresql(pg_url, query, {'ids': ids})
        if rows:
            target.send([row['id'] for row in rows])


@coroutine
def denormalize_film_data(pg_url: str, target):
    """Отправляет в target информацию о фильме из нескольких таблиц для ElasticSearch."""
    while film_ids := (yield):
        logger.debug("Denormalizing data.")
        query = """
                SELECT 
                    fw.id AS id,
                    fw.title,
                    fw.description,
                    fw.rating,
                    fw.type,
                    fwp.persons,
                    fwg.genres
                FROM "public".film_work fw
                LEFT JOIN LATERAL ( 
                    SELECT 
                        pfw.film_work_id,
                        array_agg(jsonb_build_object(
                            'id', p.id, 
                            'full_name', p.full_name, 
                            'role', pfw.role
                        )) AS persons
                    FROM "public".person_film_work pfw
                    JOIN "public".person p ON p.id = pfw.person_id
                    WHERE pfw.film_work_id = fw.id
                    GROUP BY 1 
                    ) fwp ON TRUE
                LEFT JOIN LATERAL ( 
                    SELECT 
                        gfw.film_work_id,
                        array_agg(jsonb_build_object(
                            'id', g.id, 
                            'name', g.name
                        )) AS genres
                    FROM "public".genre_film_work gfw
                    JOIN "public".genre g ON g.id = gfw.genre_id
                    WHERE gfw.film_work_id = fw.id
                    GROUP BY 1
                    ) fwg ON TRUE
                WHERE fw.id = ANY(%(film_ids)s::uuid[]);
                """

        films = query_postgresql(pg_url, query, {'film_ids': film_ids})
        logger.debug("Extracted {} film works from database", len(films))
        target.send(films)


@coroutine
def transform(target):
    """Преобразует входящие записи в схему ElasticSearch."""
    while film_works := (yield):
        logger.debug('transforming')
        batch = []
        for film_work in film_works:
            if not film_work['genres']:
                film_work['genres'] = []
            if not film_work['persons']:
                film_work['persons'] = []
            actors = [{'id': person['id'], 'name': person['full_name']}
                      for person in film_work['persons']
                      if person['role'] == 'actor']
            writers = [{'id': person['id'], 'name': person['full_name']}
                       for person in film_work['persons']
                       if person['role'] == 'writer']
            directors = [{'id': person['id'], 'name': person['full_name']}
                         for person in film_work['persons']
                         if person['role'] == 'director']

            genres = [{'id': genre['id'], 'name': genre['name']}
                      for genre in film_work['genres']]

            directors_names = [person['full_name'] for person in film_work['persons'] if person['role'] == 'director']
            actors_names = [person['full_name'] for person in film_work['persons'] if person['role'] == 'actor']
            writers_names = [person['full_name'] for person in film_work['persons'] if person['role'] == 'writer']
            genres_names = [genre['name'] for genre in film_work['genres']]

            movie = MovieElastic(id=str(film_work['id']),
                                 imdb_rating=film_work['rating'],
                                 genres_names=genres_names,
                                 title=film_work['title'],
                                 description=film_work['description'],
                                 actors_names=actors_names,
                                 writers_names=writers_names,
                                 directors_names=directors_names,
                                 actors=actors,
                                 writers=writers,
                                 directors=directors,
                                 genres=genres)

            batch.append(movie.dict())
        target.send(batch)


@coroutine
def batcher(batch_size, target):
    """Группирует входящие данные по батчам."""
    while data := (yield):
        data: List
        batch_num = 0
        while True:
            start, end = min(batch_size * batch_num, len(data)), min(batch_size * (batch_num + 1), len(data))
            batch = data[start:end]
            if not batch:
                break
            target.send(batch)
            batch_num += 1


@coroutine
def load_to_elastic(elastic_host):
    """Сохраняет входящие данные в ElasticSearch."""
    while docs := (yield):
        logger.debug('writing to ES')

        elastic_settings = dict(
            hosts=elastic_host,
        )

        def generate_doc(docs):
            for doc in docs:
                yield {
                    '_index': 'movies',
                    '_id': doc['id'],
                    '_source': doc
                }

        with Elasticsearch(**elastic_settings) as es:
            count, _ = helpers.bulk(
                es,
                generate_doc(docs)
            )
            logger.info("Updated {} documents in Elastic", count)


if __name__ == '__main__':

    parser = argparse.ArgumentParser(
        description="Daemon for getting movie db updates from Postgre and storing them in ElasticSearch")
    parser.add_argument("--postgres-url", dest="postgres_url",
                        default="postgresql://localhost:5432/",
                        help="URL в postgreSQL формате для подключения к базе.", required=False)
    parser.add_argument("--elastic-url", dest="elastic_host", default="http://localhost:9200",
                        help="URL ElasticSearch.", required=False)
    parser.add_argument("--redis-host", dest="redis_host", default='localhost',
                        help="Хост Redis.", required=False)
    parser.add_argument("--poll-period", dest="poll_period", default=2,
                        help="Пауза между обновлением данных в секундах.", required=False)
    parser.add_argument("--pg-batch", dest="pg_batch_size", default=1000,
                        help="Размер батча для загрузки из PosgreSQL.", required=False)
    parser.add_argument("--es-batch", dest="es_batch_size", default=1000,
                        help="Размер батча для загрузки в ElasticSearch.", required=False)
    args = parser.parse_args()

    logger.info("Starting ETL runner.")

    redis = Redis(host=args.redis_host)
    state = RedisState(redis_adapter=redis)

    psycopg2.extras.register_uuid()


    @dataclass(frozen=True)
    class ETLProcessConfig:
        table: str
        postgres_url: str
        elastic_host: str

        state: State
        film_id_function: Callable
        get_film_id_args: Sequence

        timestamp_field: str = 'updated_at'
        pg_batch_size: int = 1000
        es_batch_size: int = 1000

        def run(self):
            logger.debug(f"Running process for table: {self.table}")
            get_updated_postgres_entries(
                self.table,
                self.postgres_url,
                self.film_id_function(
                    *self.get_film_id_args,
                    denormalize_film_data(
                        self.postgres_url,
                        transform(
                            batcher(self.es_batch_size, load_to_elastic(self.elastic_host))
                        )
                    )
                ),
                self.state,
                self.pg_batch_size,
                self.timestamp_field
            )


    etl_processes = [
        ETLProcessConfig(table="public.film_work", postgres_url=args.postgres_url, elastic_host=args.elastic_host,
                         film_id_function=table_with_fwkey_get_film_ids, get_film_id_args=('id',), state=state),

        ETLProcessConfig(table="public.person", postgres_url=args.postgres_url, elastic_host=args.elastic_host,
                         film_id_function=get_films_ids_by_join,
                         get_film_id_args=(args.postgres_url, "public.person_film_work", "person_id"),
                         state=state),

        ETLProcessConfig(table="public.genre", postgres_url=args.postgres_url, elastic_host=args.elastic_host,
                         film_id_function=get_films_ids_by_join,
                         get_film_id_args=(args.postgres_url, "public.genre_film_work", "genre_id"),
                         state=state),

        ETLProcessConfig(table="public.person_film_work", postgres_url=args.postgres_url,
                         elastic_host=args.elastic_host, film_id_function=table_with_fwkey_get_film_ids,
                         get_film_id_args=("film_work_id",), timestamp_field='created_at',
                         state=state),

        ETLProcessConfig(table="public.genre_film_work", postgres_url=args.postgres_url,
                         elastic_host=args.elastic_host, film_id_function=table_with_fwkey_get_film_ids,
                         get_film_id_args=("film_work_id",), timestamp_field='created_at',
                         state=state),
    ]

    while True:
        logger.debug("Checking if any updated entries.")
        for etl_process in etl_processes:
            etl_process.run()

        time.sleep(args.poll_period)
