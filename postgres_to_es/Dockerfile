FROM python:3.9-slim

WORKDIR /app/
COPY . postgres_to_es/
RUN pip install -r postgres_to_es/requirements.txt

CMD python -m postgres_to_es.daemon --postgres-url postgresql://${PG_USER}:${PG_PASS}@${PG_HOST}/${PG_DB} --elastic-url ${ES_URL}