from celery import shared_task
from ..transformers.movie_transformer import MovieTransformer
import logging

logger = logging.getLogger(__name__)


@shared_task(bind=True, max_retries=3)
def transform_movies(self, movies_data):
    try:
        transformer = MovieTransformer()
        transformed_df = transformer.transform_batch(movies_data)

        result = {
            "status": "success",
            "data": len(transformed_df),
            "total_processed": len(transformed_df),
        }
        print("Transform stage results:")
        print(result)
        print('=================================================================================')
        return transformed_df.to_dict('records')
    except Exception as e:
        logger.error(f"Error in transform task: {e}")
        if self.request.retries < self.max_retries:
            raise self.retry(exc=e, countdown=2 ** self.request.retries)
        raise
