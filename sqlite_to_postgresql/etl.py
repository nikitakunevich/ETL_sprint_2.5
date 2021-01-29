"""
Модуль мигрирует данные о фильмах из SQLite в PostgreSQL в новую схему.
"""

import json
import sqlite3
from dataclasses import astuple
from typing import List, Sequence, Callable, Any
from uuid import uuid4

import psycopg2
import psycopg2.extras

from models import (
    OriginalMovie, OriginalMovieActors, OriginalActors, OriginalWriters,
    OriginalData, TransformedFilmWork, TransformedPerson, TransformedFilmWorkPerson,
    TransformedGenre, TransformedFilmWorkGenre,
    TransformedData)


def sqlite_dict_factory(cursor, row):
    """Фабрика для создания словарей из срок таблицы, где ключи — названия столбцов."""
    row_dict = {}
    for idx, column in enumerate(cursor.description):
        row_dict[column[0]] = row[idx]
    return row_dict


def sqlite_dict_connection_factory(*args, **kwargs):
    con = sqlite3.Connection(*args, **kwargs)
    con.row_factory = sqlite_dict_factory
    return con


EMPTY_VALUES = ["N/A", ""]
INVALID_WRITERS_IDS = []


def to_none_if_empty(value):
    if value in EMPTY_VALUES:
        return None
    else:
        return value


def clean_original_movie_fields(movie):
    return OriginalMovie(
        id=movie.id,
        genre=to_none_if_empty(movie.genre),
        director=to_none_if_empty(movie.director),
        title=movie.title,
        plot=to_none_if_empty(movie.plot),
        imdb_rating=to_none_if_empty(movie.imdb_rating),
        writers=to_none_if_empty(movie.writers),
    )


def fetch_sqlite_data(connection) -> OriginalData:
    """Считываем все данные из старой таблицы, убирая невалидные данные (N/A, '')."""

    # noinspection PyTypeChecker
    cursor = connection.cursor()

    cursor.execute("select DISTINCT * from actors")
    invalid_actors_ids = []
    actor_names: OriginalActors = {}

    for id_name in cursor.fetchall():
        if id_name["name"] in EMPTY_VALUES:
            invalid_actors_ids.append(id_name["id"])
        else:
            actor_names[id_name["id"]] = id_name["name"]

    cursor.execute("select DISTINCT * from writers")
    writer_names: OriginalWriters = {}

    for id_name in cursor.fetchall():
        if id_name["name"] in EMPTY_VALUES:
            INVALID_WRITERS_IDS.append(id_name["id"])
        else:
            writer_names[id_name["id"]] = id_name["name"]

    cursor.execute("select DISTINCT * from movie_actors")
    movie_actors: OriginalMovieActors = {}
    for movie_actor in cursor.fetchall():
        actors = movie_actors.setdefault(movie_actor["movie_id"], [])
        actor_id = int(movie_actor["actor_id"])
        if actor_id not in invalid_actors_ids:
            actors.append(actor_id)

    movies: List[OriginalMovie] = []
    cursor.execute("select DISTINCT * from movies")
    for movie in cursor.fetchall():
        if movie["writers"]:
            writers = [item["id"] for item in json.loads(movie["writers"])]
        else:
            writers = [movie["writer"]]
        writers = [writer for writer in writers if writer not in INVALID_WRITERS_IDS]
        unique_writers = list(set(writers))
        processed_movie = OriginalMovie(id=movie["id"], genre=movie["genre"],
                                        director=movie["director"], title=movie["title"],
                                        plot=movie["plot"], imdb_rating=movie["imdb_rating"],
                                        writers=unique_writers)
        movies.append(processed_movie)

    cursor.close()

    return OriginalData(
        movies=movies,
        movie_actors=movie_actors,
        actor_names=actor_names,
        writer_names=writer_names
    )

def update_transformed_persons(original_persons, transformed_persons,
                               persons_cache, name_getter: Callable[[Any], str]):
    for person in original_persons:
        if person not in persons_cache:
            transformed_person = TransformedPerson(id=uuid4(), full_name=name_getter(person))
            persons_cache[person] = transformed_person.id
            transformed_persons.append(transformed_person)


def update_transformed_genres(original_movie, transformed_genres, genres_name_to_new_id):
    for genre in original_movie.get_genres():
        if genre not in genres_name_to_new_id:
            transformed_genre = TransformedGenre(id=uuid4(), name=genre)
            genres_name_to_new_id[genre] = transformed_genre.id
            transformed_genres.append(transformed_genre)


def get_transformed_movie_persons(transformed_movie, original_persons, persons_cache, role):
    transformed_movie_persons = []
    for original_id in original_persons:
        movie_person = TransformedFilmWorkPerson(uuid4(), transformed_movie.id,
                                                 persons_cache[original_id], role)
        transformed_movie_persons.append(movie_person)
    return transformed_movie_persons


def migrate_data_to_new_schema(original_data: OriginalData) -> TransformedData:
    """Трансформируем данные из старой схемы в новую схему."""

    cleaned_movies = [clean_original_movie_fields(movie) for movie in original_data.movies]

    # Кэш old_id -> new_id уже созданных объектов.
    directors_name_to_new_id = dict()  # name -> id
    genres_name_to_new_id = dict()  # name -> id
    actors_old_id_to_new_id = dict()  # old_id -> new_id
    writers_old_id_to_new_id = dict()  # old_id -> new_id

    transformed_movie_persons: List[TransformedFilmWorkPerson] = []
    transformed_movie_genres: List[TransformedFilmWorkGenre] = []
    transformed_movies: List[TransformedFilmWork] = []
    transformed_persons: List[TransformedPerson] = []
    transformed_genres: List[TransformedGenre] = []

    for original_movie in cleaned_movies:
        # Преобразуем объект фильма из старой схемы в новую.
        transformed_movie = original_movie.to_transformed_movie()
        transformed_movies.append(transformed_movie)

        # Создание объектов таблиц genre и movie_genre.
        update_transformed_genres(original_movie,
                                  transformed_genres,
                                  genres_name_to_new_id)

        for genre in original_movie.get_genres():
            movie_genre = TransformedFilmWorkGenre(uuid4(),
                                                   transformed_movie.id,
                                                   genres_name_to_new_id[genre])
            transformed_movie_genres.append(movie_genre)

        # ------------ Создание объектов таблиц person и movie_person. --------------------

        # Обновляем список режиссеров
        update_transformed_persons(original_movie.get_directors(),
                                   transformed_persons,
                                   directors_name_to_new_id,
                                   lambda id_: id_)

        movie_directors = get_transformed_movie_persons(
            transformed_movie, original_movie.get_directors(),
            directors_name_to_new_id, 'director'
        )
        transformed_movie_persons.extend(movie_directors)

        # Обновляем список актеров
        update_transformed_persons(original_data.movie_actors.get(original_movie.id, []),
                                   transformed_persons,
                                   actors_old_id_to_new_id,
                                   lambda old_id: original_data.actor_names[old_id])

        movie_actors = get_transformed_movie_persons(
            transformed_movie, original_data.movie_actors[original_movie.id],
            actors_old_id_to_new_id, 'actor'
        )
        transformed_movie_persons.extend(movie_actors)

        # Обновляем список писателей
        update_transformed_persons(original_movie.writers,
                                   transformed_persons,
                                   writers_old_id_to_new_id,
                                   lambda old_id: original_data.writer_names[old_id])

        movie_writers = get_transformed_movie_persons(
            transformed_movie, original_movie.writers,
            writers_old_id_to_new_id, 'writer'
        )
        transformed_movie_persons.extend(movie_writers)

    return TransformedData(
        film_works=transformed_movies,
        film_work_persons=transformed_movie_persons,
        persons=transformed_persons,
        film_work_genres=transformed_movie_genres,
        genres=transformed_genres,
    )


def insert_rows_into_table(cursor, table_name: str, rows: Sequence[Sequence]):
    """
    Генерирует одну длинную строку с значениями для insert с правильным кол-вом параметров в зависимости от количества
    параметров в строке, и исполняет INSERT с этими значениями для заданной таблицы.
    """
    column_count = len(rows[0])
    values_template = "(" + ",".join(("%s",) * column_count) + ")"
    prepared_values = ",".join(cursor.mogrify(values_template, row).decode() for row in rows)
    cursor.execute("insert into %s values %s" % (table_name, prepared_values))


def write_data_to_postgres(transformed_data: TransformedData, connection):
    """Записываем трансформированные данные в PostgreSQL."""

    psycopg2.extras.register_uuid()  # Для конвертации python UUID в psql ::uuid.

    with connection.cursor() as curs:
        insert_rows_into_table(curs, "film_work", [astuple(movie) for movie in transformed_data.film_works])
        insert_rows_into_table(curs, "genre", [astuple(genre) for genre in transformed_data.genres])
        insert_rows_into_table(
            curs, "genre_film_work", [astuple(movie_genre) for movie_genre in transformed_data.film_work_genres]
        )
        insert_rows_into_table(curs, "person", [astuple(person) for person in transformed_data.persons])
        insert_rows_into_table(
            curs,
            "person_film_work",
            [astuple(movie_person) for movie_person in transformed_data.film_work_persons],
        )
