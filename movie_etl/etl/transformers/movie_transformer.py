import pandas as pd
from typing import List, Dict, Any
from datetime import datetime
import logging
from django.utils import timezone
from pydantic import BaseModel, Field
import numpy as np
pd.set_option('display.max_columns', None)  # Show all columns

logger = logging.getLogger(__name__)

mock_movies = [
    {
        'Title': 'The Lost Galaxy',
        'Year': '2015',
        'Rated': 'PG-13',
        'Released': '22 Jul 2015',
        'Runtime': '120 min',
        'Genre': 'Action, Adventure, Sci-Fi',
        'Director': 'John Smith',
        'Writer': 'Jane Doe',
        'Actors': 'Chris Evans, Scarlett Johansson, Robert Downey Jr.',
        'Plot': 'A group of explorers set out on a journey to a newly discovered galaxy.',
        'Language': 'English',
        'Country': 'USA',
        'Awards': 'Nominated for 3 Oscars',
        'Poster': 'https://example.com/poster1.jpg',
        'Ratings': [{'Source': 'Internet Movie Database', 'Value': '7.8/10'}],
        'Metascore': '78',
        'imdbRating': '7.8',
        'imdbVotes': '150,000',
        'imdbID': 'tt1234567',
        'Type': 'movie',
        'DVD': '10 Oct 2015',
        'BoxOffice': '$850,000,000',
        'Production': 'Legendary Pictures',
        'Website': 'https://example.com/movie1',
        'Response': 'True'
    },
    {
        'Title': 'Haunted Manor',
        'Year': '2020',
        'Rated': 'R',
        'Released': '31 Oct 2020',
        'Runtime': '95 min',
        'Genre': 'Horror, Thriller',
        'Director': 'Emily Richards',
        'Writer': 'Mark Lewis',
        'Actors': 'Emma Stone, Daniel Radcliffe',
        'Plot': 'A family moves into a haunted house and must uncover its dark past.',
        'Language': 'English',
        'Country': 'UK',
        'Awards': 'N/A',
        'Poster': 'https://example.com/poster2.jpg',
        'Ratings': [{'Source': 'Internet Movie Database', 'Value': '6.5/10'}],
        'Metascore': 'N/A',
        'imdbRating': '6.5',
        'imdbVotes': '40,000',
        'imdbID': 'tt7654321',
        'Type': 'movie',
        'DVD': 'N/A',
        'BoxOffice': '$120,000,000',
        'Production': 'Warner Bros',
        'Website': 'https://example.com/movie2',
        'Response': 'True'
    },
    {
        'Title': 'Cybernetic Revolt',
        'Year': '2023',
        'Rated': 'PG-13',
        'Released': '15 Jun 2023',
        'Runtime': '130 min',
        'Genre': 'Action, Sci-Fi, Thriller',
        'Director': 'Michael Bay',
        'Writer': 'Sarah Connor',
        'Actors': 'Tom Hardy, Charlize Theron',
        'Plot': 'A rogue AI takes over the world, and a small resistance fights back.',
        'Language': 'English',
        'Country': 'Canada',
        'Awards': 'Nominated for 2 BAFTA Awards',
        'Poster': 'https://example.com/poster3.jpg',
        'Ratings': [{'Source': 'Internet Movie Database', 'Value': '8.2/10'}],
        'Metascore': '85',
        'imdbRating': '8.2',
        'imdbVotes': '200,000',
        'imdbID': 'tt9876543',
        'Type': 'movie',
        'DVD': 'N/A',
        'BoxOffice': '$950,000,000',
        'Production': '20th Century Studios',
        'Website': 'https://example.com/movie3',
        'Response': 'True'
    },
    {
        'Title': 'Romance in Paris',
        'Year': '2018',
        'Rated': 'PG',
        'Released': '14 Feb 2018',
        'Runtime': '105 min',
        'Genre': 'Romance, Drama',
        'Director': 'Anna White',
        'Writer': 'David Scott',
        'Actors': 'Ryan Gosling, Lily Collins',
        'Plot': 'Two strangers meet in Paris and fall in love against all odds.',
        'Language': 'French, English',
        'Country': 'France',
        'Awards': 'Won 1 Golden Globe',
        'Poster': 'https://example.com/poster4.jpg',
        'Ratings': [{'Source': 'Internet Movie Database', 'Value': '7.3/10'}],
        'Metascore': '72',
        'imdbRating': '7.3',
        'imdbVotes': '95,000',
        'imdbID': 'tt1122334',
        'Type': 'movie',
        'DVD': '05 May 2018',
        'BoxOffice': '$210,000,000',
        'Production': 'Sony Pictures',
        'Website': 'https://example.com/movie4',
        'Response': 'True'
    },
    {
        'Title': 'The Last Samurai 2',
        'Year': '2022',
        'Rated': 'R',
        'Released': '10 Dec 2022',
        'Runtime': '140 min',
        'Genre': 'Action, History, Drama',
        'Director': 'James Cameron',
        'Writer': 'Chris Nolan',
        'Actors': 'Keanu Reeves, Ken Watanabe',
        'Plot': 'A warrior’s legacy continues as he fights to restore honor.',
        'Language': 'English, Japanese',
        'Country': 'Japan, USA',
        'Awards': 'Pending Academy Award nominations',
        'Poster': 'https://example.com/poster5.jpg',
        'Ratings': [{'Source': 'Internet Movie Database', 'Value': '8.5/10'}],
        'Metascore': '90',
        'imdbRating': '8.5',
        'imdbVotes': '300,000',
        'imdbID': 'tt5566778',
        'Type': 'movie',
        'DVD': 'N/A',
        'BoxOffice': '$1,200,000,000',
        'Production': 'Paramount Pictures',
        'Website': 'https://example.com/movie5',
        'Response': 'True'
    }
]

class MovieData(BaseModel):
    Title: str
    Year: str
    Director: str
    Genre: str
    Plot: str
    imdbID: str = Field(alias="imdbID")


class MovieTransformer:
    def __init__(self):
        self.current_date = timezone.now()

    def _normalize_names(self, text):
        if pd.isna(text) or not isinstance(text, str):
            return ""

        special_cases = {'ii': 'II', 'iii': 'III', 'iv': 'IV', 'dc': 'DC', 'mc': 'Mc'}
        words = text.title().split()

        return ' '.join(
            special_cases.get(word.lower(), word)
            for word in words
        )

    def _calculate_time_since_release(self, release_year):
        try:
            release_date = pd.to_datetime(f"{release_year}-01-01").to_pydatetime()
            release_date = timezone.make_aware(release_date)
            time_diff = self.current_date - release_date

            days = int(time_diff.days)
            months = int(days / 30.44)
            years = int(days / 365.25)

            return {
                "days_since_release": days,
                "months_since_release": months,
                "years_since_release": years
            }
        except Exception as e:
            print(e)
            logger.error(f"Error calculating time since release: {e}")
            return {
                "days_since_release": 0,
                "months_since_release": 0,
                "years_since_release": 0
            }

    def _split_genres(self, genre_string):
        if pd.isna(genre_string) or not isinstance(genre_string, str):
            return []

        return [
            genre.strip().title()
            for genre in genre_string.split(',')
            if genre.strip()
        ]

    def transform_batch(self, movies_data):
        try:
            df = pd.DataFrame(movies_data)
            # df = pd.DataFrame(mock_movies)

            print("Validating data...")
            validated_data = []
            for _, row in df.iterrows():
                try:
                    MovieData(**row.to_dict())
                    validated_data.append(row)
                except Exception as e:
                    logger.error(f"Validation error for movie {row.get('imdbID', 'unknown')}: {e}")
                    continue

            df = pd.DataFrame(validated_data)

            print("Applying transformations...")
            df['title'] = df['Title'].apply(self._normalize_names)
            df['director'] = df['Director'].apply(self._normalize_names)
            df['synopsis'] = df['Plot']

            print("Calculating date related times...")
            time_since_release = df['Year'].apply(self._calculate_time_since_release)
            time_df = pd.DataFrame(time_since_release.tolist())
            df = pd.concat([df, time_df], axis=1)

            print("Splitting genres...")
            df['genres'] = df['Genre'].apply(self._split_genres)

            # Select and rename final columns
            final_columns = {
                'imdbID': 'omdb_id',
                'title': 'title',
                'Year': 'release_year',
                'director': 'director',
                'synopsis': 'synopsis',
                'genres': 'genres',
                'days_since_release': 'days_since_release',
                'months_since_release': 'months_since_release',
                'years_since_release': 'years_since_release'
            }

            transformed_df = df[final_columns.keys()].rename(columns=final_columns)

            return transformed_df

        except Exception as e:
            logger.error(f"Error in batch transformation: {e}")
            raise
