Проект мигрирует данные о фильмах из SQLite в PostgreSQL в новую схему.

# Как использовать
load_data.py использует argparse, поэтому запускайте `python load_data.py`, а `help` вам все объяснит.

# Пример исспользования
```python load_data.py --from db.sqlite --to "dbname=testdb user=nikitak" --init postgres_init.sql```