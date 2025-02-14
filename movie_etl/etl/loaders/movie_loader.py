from django.db import transaction
from django.core.exceptions import ValidationError
import logging
from ..models.models import Movie, Genre, MovieGenre
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from django.db.utils import IntegrityError

logger = logging.getLogger(__name__)


class MovieLoader:
    def __init__(self, batch_size=100):
        self.batch_size = batch_size
        self.stats = {
            'processed': 0,
            'success': 0,
            'failed': 0,
            'genres_created': 0
        }

    @transaction.atomic
    def _create_or_update_movie(self, movie_data):
        try:
            movie, created = Movie.objects.update_or_create(
                omdb_id=movie_data['omdb_id'],
                defaults={
                    'title': movie_data['title'],
                    'release_year': movie_data['release_year'],
                    'director': movie_data['director'],
                    'synopsis': movie_data['synopsis'],
                    'days_since_release': movie_data['days_since_release'],
                    'months_since_release': movie_data['months_since_release'],
                    'years_since_release': movie_data['years_since_release']
                }
            )

            current_genres = []
            for genre_name in movie_data['genres']:
                genre, genre_created = Genre.objects.get_or_create(name=genre_name)
                if genre_created:
                    self.stats['genres_created'] += 1
                current_genres.append(genre)

            MovieGenre.objects.filter(movie=movie).exclude(
                genre__in=current_genres
            ).delete()

            existing_genres = set(MovieGenre.objects.filter(
                movie=movie
            ).values_list('genre_id', flat=True))

            new_genres = [
                MovieGenre(movie=movie, genre=genre)
                for genre in current_genres
                if genre.id not in existing_genres
            ]

            MovieGenre.objects.bulk_create(new_genres, ignore_conflicts=True)

            return movie

        except Exception as e:
            logger.error(f"Error processing movie {movie_data['omdb_id']}: {str(e)}")
            raise

    def load_batch(self, transformed_data):
        successes = []
        failures = []

        for batch_start in range(0, len(transformed_data), self.batch_size):
            batch = transformed_data[batch_start:batch_start + self.batch_size]

            try:
                with transaction.atomic():
                    for movie_data in batch:
                        try:
                            movie = self._create_or_update_movie(movie_data)
                            successes.append({
                                'omdb_id': movie_data['omdb_id'],
                                'title': movie_data['title']
                            })
                            self.stats['success'] += 1
                        except Exception as e:
                            print(e)
                            failures.append({
                                'omdb_id': movie_data['omdb_id'],
                                'error': str(e)
                            })
                            self.stats['failed'] += 1
                            logger.error(f"Failed to load movie: {str(e)}")

            except Exception as e:
                print(e)
                logger.error(f"Batch transaction failed: {str(e)}")
                failures.extend([{
                    'omdb_id': m['omdb_id'],
                    'error': str(e)
                } for m in batch])
                self.stats['failed'] += len(batch)

        self.stats['processed'] = len(transformed_data)

        return {
            'stats': self.stats,
            'successes': successes,
            'failures': failures
        }
