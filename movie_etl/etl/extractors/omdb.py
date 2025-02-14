import aiohttp
import asyncio
import backoff
import logging
import requests
from decouple import config


logger = logging.getLogger(__name__)


class OMDBExtractor:
    def __init__(self):
        self.api_key = config("OMDB_API_KEY")
        self.base_url = config("OMDB_API_BASE_URL")
        self.session = None
        self.semaphore = asyncio.Semaphore(10)

    async def _make_request(self, params):
        #print(params)
        async with self.semaphore:
            try:
                async with self.session.get(
                        self.base_url,
                        params={**params, 'apikey': self.api_key},
                        timeout=10
                ) as response:
                    response.raise_for_status()
                    result = await response.json()
                    print(result)
                    return result
            except Exception as e:
                logger.error(f"API request failed: {str(e)}")
                raise

    async def get_movie_details_batch(self, imdb_ids):
        if not self.session:
            self.session = aiohttp.ClientSession()

        tasks = []
        for imdb_id in imdb_ids:
            params = {'i': imdb_id, 'plot': 'full'}
            tasks.append(self._make_request(params))

        results = await asyncio.gather(*tasks, return_exceptions=True)

        movies = []
        errors = []
        for imdb_id, result in zip(imdb_ids, results):
            if isinstance(result, Exception):
                errors.append({'imdb_id': imdb_id, 'error': str(result)})
            else:
                movies.append(result)

        return {'movies': movies, 'errors': errors}

    async def fetch_all_movies_by_year(self, year):
        if not self.session:
            self.session = aiohttp.ClientSession()

        first_page = await self._make_request({
            'y': str(year),
            's': 'movie',
            'type': 'movie',
            'page': '1'
        })

        total_results = int(first_page['totalResults'])
        total_pages = (total_results + 9) // 10

        tasks = []
        for page in range(1, total_pages + 1):
            params = {
                'y': str(year),
                's': 'movie',
                'type': 'movie',
                'page': str(page)
            }
            tasks.append(self._make_request(params))

        results = await asyncio.gather(*tasks)

        movies = []
        for result in results:
            if result.get('Response') == 'True':
                movies.extend(result['Search'])

        return movies

    async def cleanup(self):
        if self.session:
            await self.session.close()
            self.session = None

    # Helper method for calls from sync
    def run_async(self, coro):
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(coro)
