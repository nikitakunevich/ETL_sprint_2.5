FROM python:3.9-slim

WORKDIR /app/
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt
COPY . postgres_to_es/

CMD python -m postgres_to_es.daemon --postgres-url postgresql://${PG_USER}:${PG_PASS}@${PG_HOST}/${PG_DB} --elastic-url ${ES_URL} --redis-host ${REDIS_HOST}