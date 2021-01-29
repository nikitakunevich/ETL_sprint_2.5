import uuid

import django.core.validators
import django.utils.timezone
import model_utils.fields
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('movie_admin', '0002_make_models_managed'),
    ]

    operations = [
        migrations.AddField(
            model_name='filmwork',
            name='genres',
            field=models.ManyToManyField(blank=True, through='movie_admin.GenreFilmWork', to='movie_admin.Genre'),
        ),
        migrations.AddField(
            model_name='filmwork',
            name='mpaa_age_rating',
            field=models.TextField(blank=True, choices=[('general', 'без ограничений'), ('parental_guidance', 'рекомендовано смотреть с родителями'), ('parental_guidance_strong', 'просмотр не желателен детям до 13 лет'), ('restricted', 'до 17 в сопровождении родителей'), ('no_one_17_under', 'только с 18')], null=True, verbose_name='возрастной рейтинг'),
        ),
        migrations.AddField(
            model_name='filmwork',
            name='persons',
            field=models.ManyToManyField(blank=True, through='movie_admin.PersonFilmWork', to='movie_admin.Person'),
        ),
        migrations.AlterField(
            model_name='genrefilmwork',
            name='film_work',
            field=models.ForeignKey(default=None, on_delete=django.db.models.deletion.CASCADE, to='movie_admin.filmwork', verbose_name='фильм'),
            preserve_default=False,
        ),
        migrations.AlterField(
            model_name='genrefilmwork',
            name='genre',
            field=models.ForeignKey(default=None, on_delete=django.db.models.deletion.CASCADE, to='movie_admin.genre', verbose_name='жанр'),
            preserve_default=False,
        ),
        migrations.AlterField(
            model_name='personfilmwork',
            name='film_work',
            field=models.ForeignKey(default=None, on_delete=django.db.models.deletion.CASCADE, to='movie_admin.filmwork', verbose_name='фильм'),
            preserve_default=False,
        ),
        migrations.AlterField(
            model_name='personfilmwork',
            name='person',
            field=models.ForeignKey(default=None, on_delete=django.db.models.deletion.CASCADE, to='movie_admin.person', verbose_name='человек'),
            preserve_default=False,
        ),
        migrations.AlterField(
            model_name='filmwork',
            name='certificate',
            field=models.TextField(blank=True, null=True, verbose_name='сертификат'),
        ),
        migrations.AlterField(
            model_name='filmwork',
            name='created_at',
            field=model_utils.fields.AutoCreatedField(blank=True, default=django.utils.timezone.now, editable=False, null=True, verbose_name='время создания'),
        ),
        migrations.AlterField(
            model_name='filmwork',
            name='creation_date',
            field=models.DateField(blank=True, null=True, verbose_name='дата создания фильма'),
        ),
        migrations.AlterField(
            model_name='filmwork',
            name='description',
            field=models.TextField(blank=True, null=True, verbose_name='описание'),
        ),
        migrations.AlterField(
            model_name='filmwork',
            name='file_path',
            field=models.FileField(blank=True, null=True, upload_to='film_works/', verbose_name='файл'),
        ),
        migrations.AlterField(
            model_name='filmwork',
            name='id',
            field=models.UUIDField(default=uuid.uuid4, primary_key=True, serialize=False),
        ),
        migrations.AlterField(
            model_name='filmwork',
            name='rating',
            field=models.FloatField(blank=True, null=True, validators=[django.core.validators.MinValueValidator(0)], verbose_name='рейтинг'),
        ),
        migrations.AlterField(
            model_name='filmwork',
            name='title',
            field=models.TextField(verbose_name='название'),
        ),
        migrations.AlterField(
            model_name='filmwork',
            name='type',
            field=models.TextField(blank=True, choices=[('movie', 'фильм'), ('series', 'сериал'), ('tv_show', 'шоу')], null=True, verbose_name='тип'),
        ),
        migrations.AlterField(
            model_name='filmwork',
            name='updated_at',
            field=model_utils.fields.AutoLastModifiedField(blank=True, default=django.utils.timezone.now, editable=False, null=True, verbose_name='время последнего изменения'),
        ),
        migrations.AlterField(
            model_name='genre',
            name='created_at',
            field=model_utils.fields.AutoCreatedField(blank=True, default=django.utils.timezone.now, editable=False, null=True, verbose_name='время создания'),
        ),
        migrations.AlterField(
            model_name='genre',
            name='description',
            field=models.TextField(blank=True, null=True, verbose_name='описание'),
        ),
        migrations.AlterField(
            model_name='genre',
            name='id',
            field=models.UUIDField(default=uuid.uuid4, primary_key=True, serialize=False),
        ),
        migrations.AlterField(
            model_name='genre',
            name='name',
            field=models.TextField(verbose_name='название'),
        ),
        migrations.AlterField(
            model_name='genre',
            name='updated_at',
            field=model_utils.fields.AutoLastModifiedField(blank=True, default=django.utils.timezone.now, editable=False, null=True, verbose_name='время последнего изменения'),
        ),
        migrations.AlterField(
            model_name='genrefilmwork',
            name='created_at',
            field=model_utils.fields.AutoCreatedField(blank=True, default=django.utils.timezone.now, editable=False, null=True, verbose_name='время создания'),
        ),
        migrations.AlterField(
            model_name='person',
            name='birth_date',
            field=models.DateField(blank=True, null=True, verbose_name='дата рождения'),
        ),
        migrations.AlterField(
            model_name='person',
            name='created_at',
            field=model_utils.fields.AutoCreatedField(blank=True, default=django.utils.timezone.now, editable=False, null=True, verbose_name='время создания'),
        ),
        migrations.AlterField(
            model_name='person',
            name='full_name',
            field=models.TextField(verbose_name='полное имя'),
        ),
        migrations.AlterField(
            model_name='person',
            name='id',
            field=models.UUIDField(default=uuid.uuid4, primary_key=True, serialize=False),
        ),
        migrations.AlterField(
            model_name='person',
            name='updated_at',
            field=model_utils.fields.AutoLastModifiedField(blank=True, default=django.utils.timezone.now, editable=False, null=True, verbose_name='время последнего изменения'),
        ),
        migrations.AlterField(
            model_name='personfilmwork',
            name='created_at',
            field=model_utils.fields.AutoCreatedField(blank=True, default=django.utils.timezone.now, editable=False, null=True, verbose_name='время создания'),
        ),
        migrations.AlterField(
            model_name='personfilmwork',
            name='id',
            field=models.UUIDField(default=uuid.uuid4, primary_key=True, serialize=False),
        ),
        migrations.AlterField(
            model_name='personfilmwork',
            name='role',
            field=models.TextField(choices=[('actor', 'актер'), ('director', 'режиссер'), ('writer', 'сценарист')], verbose_name='кем работал на фильме'),
        ),
        migrations.AlterUniqueTogether(
            name='genrefilmwork',
            unique_together={('film_work_id', 'genre_id')},
        ),
        migrations.AlterUniqueTogether(
            name='personfilmwork',
            unique_together={('film_work_id', 'person_id', 'role')},
        ),
    ]
