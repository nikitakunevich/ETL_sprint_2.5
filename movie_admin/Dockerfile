FROM python:3.9-slim

WORKDIR /app/
COPY . .
RUN pip install -r requirements-prod.txt

EXPOSE 8000
CMD gunicorn -b 0.0.0.0 config.wsgi --log-level $LOG_LEVEL
