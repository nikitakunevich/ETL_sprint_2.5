FROM python:3.9-slim

WORKDIR /app/
COPY . .
RUN pip install -r requirements.txt

EXPOSE 4000
CMD gunicorn -b 0.0.0.0:4000 api:app --log-level=${LOG_LEVEL} --access-logfile=-
