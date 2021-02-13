set -a
. deploys/prod.env
set +a

case $1 in
  rebuild)
    docker-compose build movies_admin etl search_api
  ;;

  start)
    ./run.sh stop
    ./run.sh rebuild
    docker-compose up -d movies_admin postgres nginx
    docker-compose run -v $(pwd)/sqlite_to_postgresql:/sqlite_to_postgresql -w /sqlite_to_postgresql movies_admin python load_data.py --from db.sqlite --to "dbname=${PG_DB} user=${PG_USER} host=${PG_HOST} password=${PG_PASS}" --init postgres_init.sql
    docker-compose exec movies_admin ./manage.py migrate
    docker-compose exec movies_admin ./manage.py collectstatic --no-input
    docker-compose exec movies_admin /bin/sh -c 'echo "creating admin user"'
    docker-compose exec movies_admin ./manage.py shell -c "from django.contrib.auth.models import User; User.objects.create_superuser('${ADMIN}', 'admin@example.com', '${ADMIN_PASS}')"
    # etl process
    docker-compose up -d elasticsearch

    docker-compose up -d search_api
    docker-compose logs -f
  ;;
  load_es_index)
    curl  -XPUT http://localhost:9200/movies -H 'Content-Type: application/json' -d @movies.es.schema.json
    curl  -XPUT http://localhost:9200/persons -H 'Content-Type: application/json' -d @persons.es.schema.json
    curl  -XPUT http://localhost:9200/genres -H 'Content-Type: application/json' -d @genres.es.schema.json
  ;;
  start_etl)
    docker-compose up etl
  ;;
  stop)
    docker-compose down -v --remove-orphans
  ;;
  *)
    echo "Use 'start' command"
esac
