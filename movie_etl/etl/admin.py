from django.contrib import admin
from .models.models import Movie, Genre, MovieGenre, ETLRun


@admin.register(Movie)
class MovieAdmin(admin.ModelAdmin):
    list_display = ('title', 'release_year', 'director', 'created_at')
    search_fields = ('title', 'director', 'omdb_id')
    list_filter = ('release_year',)
    ordering = ('-release_year',)


@admin.register(Genre)
class GenreAdmin(admin.ModelAdmin):
    list_display = ('name',)
    search_fields = ('name',)


@admin.register(MovieGenre)
class MovieGenreAdmin(admin.ModelAdmin):
    list_display = ('movie', 'genre')
    search_fields = ('movie__title', 'genre__name')


@admin.register(ETLRun)
class ETLRunAdmin(admin.ModelAdmin):
    list_display = ('year', 'start_time', 'end_time', 'status', 'processed', 'success', 'failed', 'genres_created')
    list_filter = ('status', 'year')
    search_fields = ('year',)
    readonly_fields = ('start_time', 'end_time', 'processed', 'success', 'failed', 'genres_created', 'error_message')

    def has_add_permission(self, request):
        return False
