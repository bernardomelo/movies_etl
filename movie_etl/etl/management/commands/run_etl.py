from django.core.management.base import BaseCommand
from celery import chain
import logging, time

from etl.models.models import ETLRun

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = 'Run the complete ETL pipeline for movies'

    def add_arguments(self, parser):
        parser.add_argument(
            '--years',
            nargs='+',
            type=str,
            help='List of years to process'
        )
        parser.add_argument(
            '--batch-size',
            type=int,
            default=100,
            help='Size of batches for processing'
        )

    def handle(self, *args, **options):
        from etl.tasks.extractor_tasks import fetch_movies_for_year
        from etl.tasks.transform_tasks import transform_movies
        from etl.tasks.load_tasks import load_movies

        years = options['years']

        if not years:
            self.stderr.write('No year provided')
            return

        start_time = time.time()
        try:
            for year in years:
                etl_run = ETLRun.objects.create(year=year, status="running")

                try:
                    self.stdout.write(
                        self.style.SUCCESS(
                            f'Startin ETL pipeline for year {year}...'
                        )
                    )

                    etl_chain = chain(
                        fetch_movies_for_year.s(year),
                        transform_movies.s(),
                        load_movies.s()
                    )

                    result = etl_chain.apply_async()

                    self.stdout.write(
                        self.style.SUCCESS(
                            f'Started ETL pipeline with task ID: {result.id}'
                        )
                    )

                    # This waits for completion
                    final_result = result.get()

                    self.stdout.write(
                        self.style.SUCCESS(
                            f"ETL pipeline completed. "
                            f"Processed: {final_result['stats']['processed']}, "
                            f"Succeeded: {final_result['stats']['success']}, "
                            f"Failed: {final_result['stats']['failed']}"
                        )
                    )
                    etl_run.mark_completed(final_result["stats"])
                    print('=================================================================================')

                except Exception as e:
                    etl_run.mark_failed(str(e))
                    self.stderr.write(
                        self.style.ERROR(f'ETL pipeline failed for {year}: {str(e)}')
                    )

            end_time = time.time()
            elapsed_time = end_time - start_time

            self.stdout.write(
                self.style.SUCCESS(
                    f'ETL Processes completed for years {years}. \n Elapsed time: {elapsed_time}'
                )
            )

        except Exception as e:
            etl_run.mark_failed(str(e))
            self.stderr.write(
                self.style.ERROR(f'Major ETL pipeline fail: {str(e)}')
            )