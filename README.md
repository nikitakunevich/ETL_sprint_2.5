Чтобы запустить проект в docker-compose:

```./run.sh start```

Далее надо загрузить индекс в Elastic:

```./run.sh load_es_index```

Осталось запустить etl-демона:

```./run.sh start_etl```

Теперь на порту **8081** работает django-админка вместе с API к PostgreSQL.
А на порту **8082** работает API к elasticsearch.

При запуске команды `start`:
* будут подняты postgresql, nginx, gunicorn с django приложением, search API на flask.
* созданы вольюмы для базы, статики.
* ETL достанет данные из sqlite и положит в postgresql.
* будут применены django-миграции к данным, чтобы они соответствовали моделям.
* Статические файлы будут сохранены в volume для отдачи nginx-ом.
* Будет создан админ с логин: admin, паролем: admin.

При запуске команды `start_etl`:
* поднимется redis
* запустится etl-демон, который будет периодически смотреть за изменениями в базе и реплицировать их в ES.

Чтобы удалить все контейнеры и volumes:

```./run.sh stop```

Чтобы запустить проект без docker, а на django dev-server, можно после запуска скрипта ./run.sh start выполнить команду:
```
set -a;source ../deploys/local.env; set +a; ./manage.py runserver
```
(В такой конфигурации работает debug-toolbar).

Сервер подключится к postgresql базе в docker.
