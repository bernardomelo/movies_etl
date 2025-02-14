# Generated by Django 5.0.1 on 2025-02-12 23:05

import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):

    initial = True

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='Genre',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('name', models.CharField(max_length=100, unique=True)),
            ],
        ),
        migrations.CreateModel(
            name='Movie',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('omdb_id', models.CharField(max_length=100, unique=True)),
                ('title', models.CharField(max_length=255)),
                ('release_year', models.IntegerField()),
                ('director', models.CharField(max_length=255)),
                ('synopsis', models.TextField()),
                ('days_since_release', models.IntegerField()),
                ('months_since_release', models.IntegerField()),
                ('years_since_release', models.IntegerField()),
                ('created_at', models.DateTimeField(auto_now_add=True)),
                ('updated_at', models.DateTimeField(auto_now=True)),
            ],
            options={
                'indexes': [models.Index(fields=['omdb_id'], name='etl_movie_omdb_id_acf104_idx'), models.Index(fields=['release_year'], name='etl_movie_release_0eb7aa_idx')],
            },
        ),
        migrations.CreateModel(
            name='MovieGenre',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('genre', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='etl.genre')),
                ('movie', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='etl.movie')),
            ],
            options={
                'unique_together': {('movie', 'genre')},
            },
        ),
        migrations.AddField(
            model_name='genre',
            name='movies',
            field=models.ManyToManyField(related_name='genres', through='etl.MovieGenre', to='etl.movie'),
        ),
    ]
