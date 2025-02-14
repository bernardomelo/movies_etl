from celery import shared_task
from ..loaders.movie_loader import MovieLoader
import logging

logger = logging.getLogger(__name__)


@shared_task(bind=True, max_retries=3)
def load_movies(self, transformed_data):
    try:
        loader = MovieLoader()
        result = loader.load_batch(transformed_data)
        logger.info(f"Loading completed. Stats: {result['stats']}")

        if result['failures']:
            logger.warning(
                f"Failed to load {len(result['failures'])} movies. "
                f"First error: {result['failures'][0]['error']}"
            )
        print("Load stage results:")
        print(result['stats'])
        print('=================================================================================')
        return result
    except Exception as e:
        logger.error(f"Error in load task: {e}")
        if self.request.retries < self.max_retries:
            raise self.retry(exc=e, countdown=2 ** self.request.retries)
        raise
