from celery import shared_task
from ..extractors.omdb import OMDBExtractor
from django.core.cache import cache
import hashlib
import json
import logging

logger = logging.getLogger(__name__)


@shared_task(bind=True, max_retries=3)
def fetch_movies_for_year(self, year):

    extractor = OMDBExtractor()
    try:
        print(f"Fetching all movies for year {year}...")
        movies_list = extractor.run_async(extractor.fetch_all_movies_by_year(year))

        movie_ids = [movie['imdbID'] for movie in movies_list]

        batch_size = 50
        all_movies = []
        all_errors = []

        print(f"Fetching details for each movie for year {year}...")
        for i in range(0, len(movie_ids), batch_size):
            batch_ids = movie_ids[i:i + batch_size]
            batch_results = extractor.run_async(
                extractor.get_movie_details_batch(batch_ids)
            )
            all_movies.extend(batch_results['movies'])
            all_errors.extend(batch_results['errors'])

        results = {
            'year': year,
            'errors': all_errors,
            'stats': {
                'total_found': len(movie_ids),
                'successfully_fetched': len(all_movies),
                'failed': len(all_errors)
            }
        }

        extractor.run_async(extractor.cleanup())

        print("Extract stage results:")
        print(results)
        print('=================================================================================')
        return all_movies

    except Exception as e:
        logger.error(f"Error in transform task: {e}")
        if self.request.retries < self.max_retries:
            raise self.retry(exc=e, countdown=2 ** self.request.retries)
        raise
