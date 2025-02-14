from django.db import models
from django.utils import timezone
from django.utils.timezone import now


class Movie(models.Model):
    omdb_id = models.CharField(max_length=100, unique=True)
    title = models.CharField(max_length=255)
    release_year = models.IntegerField()
    director = models.CharField(max_length=255)
    synopsis = models.TextField()
    days_since_release = models.IntegerField()
    months_since_release = models.IntegerField()
    years_since_release = models.IntegerField()
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        indexes = [
            models.Index(fields=['omdb_id']),
            models.Index(fields=['release_year'])
        ]

    def __str__(self):
        return f"{self.title} ({self.release_year})"


class Genre(models.Model):
    name = models.CharField(max_length=100, unique=True)
    movies = models.ManyToManyField(
        Movie,
        through='MovieGenre',
        related_name='genres'
    )

    def __str__(self):
        return self.name


class MovieGenre(models.Model):
    movie = models.ForeignKey(Movie, on_delete=models.CASCADE)
    genre = models.ForeignKey(Genre, on_delete=models.CASCADE)

    class Meta:
        unique_together = ['movie', 'genre']

    def __str__(self):
        return f"{self.movie.title} - {self.genre.name}"


class ETLRun(models.Model):
    STATUS_CHOICES = [
        ('pending', 'Pending'),
        ('running', 'Running'),
        ('completed', 'Completed'),
        ('failed', 'Failed'),
    ]

    year = models.IntegerField()
    start_time = models.DateTimeField(default=now)
    end_time = models.DateTimeField(null=True, blank=True)
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='pending')

    processed = models.IntegerField(default=0)
    success = models.IntegerField(default=0)
    failed = models.IntegerField(default=0)
    genres_created = models.IntegerField(default=0)

    error_message = models.TextField(blank=True, null=True)

    def mark_completed(self, stats):
        self.end_time = now()
        self.status = 'completed'
        self.processed = stats.get('processed', 0)
        self.success = stats.get('success', 0)
        self.failed = stats.get('failed', 0)
        self.genres_created = stats.get('genres_created', 0)
        self.save()

    def mark_failed(self, error_message):
        self.end_time = now()
        self.status = 'failed'
        self.error_message = error_message
        self.save()

    def __str__(self):
        return f"ETL Run {self.year} - {self.status}"

# from sqlalchemy import (
#     Column, Integer, String, Text, DateTime, ForeignKey, UniqueConstraint, Index
# )
# from sqlalchemy.orm import relationship, declarative_base
# from datetime import datetime
#
# Base = declarative_base()
#
#
# class Movie(Base):
#     __tablename__ = 'movies'
#
#     id = Column(Integer, primary_key=True, autoincrement=True)
#     omdb_id = Column(String(100), unique=True, nullable=False)
#     title = Column(String(255), nullable=False)
#     release_year = Column(Integer, nullable=False)
#     director = Column(String(255), nullable=False)
#     synopsis = Column(Text, nullable=False)
#     days_since_release = Column(Integer, nullable=False)
#     months_since_release = Column(Integer, nullable=False)
#     years_since_release = Column(Integer, nullable=False)
#     created_at = Column(DateTime, default=datetime.utcnow)
#     updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
#
#     # Relationships
#     genres = relationship('Genre', secondary='movies_genres', back_populates='movies')
#
#     # Indexes
#     __table_args__ = (
#         Index('idx_omdb_id', 'omdb_id'),
#         Index('idx_release_year', 'release_year'),
#     )
#
#     def __repr__(self):
#         return f"<Movie(title={self.title}, year={self.release_year})>"
#
#
# class Genre(Base):
#     __tablename__ = 'genres'
#
#     id = Column(Integer, primary_key=True, autoincrement=True)
#     name = Column(String(100), unique=True, nullable=False)
#
#     # Relationships
#     movies = relationship('Movie', secondary='movies_genres', back_populates='genres')
#
#     def __repr__(self):
#         return f"<Genre(name={self.name})>"
#
#
# class MovieGenre(Base):
#     __tablename__ = 'movies_genres'
#
#     movie_id = Column(Integer, ForeignKey('movies.id', ondelete="CASCADE"), primary_key=True)
#     genre_id = Column(Integer, ForeignKey('genres.id', ondelete="CASCADE"), primary_key=True)
#
#     # Unique constraint
#     __table_args__ = (
#         UniqueConstraint('movie_id', 'genre_id', name='uq_movie_genre'),
#     )
#
#     def __repr__(self):
#         return f"<MovieGenre(movie_id={self.movie_id}, genre_id={self.genre_id})>"
