"""API которое ищет в elastic фильмы."""
import logging
import os
from typing import List, Literal, Optional

from flask import Flask, abort, jsonify, request
import requests
from pydantic import BaseModel, ValidationError, validator


class MovieModel(BaseModel):
    class Actor(BaseModel):
        id: str
        name: str

    class Writer(BaseModel):
        id: str
        name: str

    id: str
    title: str
    description: Optional[str]
    imdb_rating: Optional[float]
    writers: List[Writer]
    actors: List[Actor]
    genres: Optional[List[str]]
    directors: Optional[List[str]]


class MovieListItemModel(BaseModel):
    id: str
    title: str
    imdb_rating: Optional[float]


app = Flask(__name__)
ESHOST = os.environ.get('ES_URL', "http://localhost:9200")
httpclient = requests.Session()


@app.route("/api/movies/<string:movie_id>", methods=["GET"])
def movie_info(movie_id: str) -> str:
    movie = None
    query = {
        "query": {
            "match": {
                "id": movie_id
            }
        }
    }
    response = httpclient.get(f"{ESHOST}/movies/_doc/_search", json=query)
    response.raise_for_status()

    # hits.hits - array
    # hits.total.value - count
    result = response.json()
    hits = result["hits"]["hits"]
    if len(hits) == 0:
        abort(404)
    movie = MovieModel(**hits[0]["_source"])
    return jsonify(movie.dict())


@app.route("/api/movies/", methods=["GET"])
def movies_list() -> str:
    class Query(BaseModel):
        limit = 50
        page = 1
        sort: Literal["id", "title", "imdb_rating"] = "id"
        sort_order: Literal["asc", "desc"] = "asc"
        search: Optional[str]

        @validator("limit", "page", allow_reuse=True)
        def is_positive_int(cls, v):
            assert v > 0
            return v

    try:
        query_params = Query(**request.args.to_dict())
    except ValidationError as ve:
        response = jsonify(ve.errors())
        response.status_code = 422
        return response

    query = {
        "from": (query_params.page - 1) * query_params.limit,
        "size": query_params.limit,
        "sort": {
            query_params.sort: {
                "order": query_params.sort_order
            }
        },
    }
    if query_params.search:
        query["query"] = {
            "multi_match": {
                "query": query_params.search,
                "fields": ["title^4", "description^3", "genres_names^2", "actors_names^4", "writers_names",
                           "directors^3"]
            }
        }
    response = httpclient.get(f"{ESHOST}/movies/_doc/_search/", json=query)
    try:
        response.raise_for_status()
    except Exception as e:
        app.logger.exception("Couldn't connect to ES", exc_info=e)
        abort(400)

    result = response.json()
    hits = result["hits"]["hits"]
    movies = [MovieModel(**hit["_source"], genres=hit['_source']['genres_names']) for hit in hits]
    return jsonify([movie.dict() for movie in movies])


if __name__ == '__main__':
    app.run(port=8000, debug=True)
else:
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
