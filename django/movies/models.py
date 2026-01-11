from django.db import models


# ============ MODÈLES SQLITE (base 'imdb') ============

class Movie(models.Model):
    mid = models.TextField(primary_key=True)
    titleType = models.TextField(null=True, blank=True)
    primaryTitle = models.TextField(null=True, blank=True)
    originalTitle = models.TextField(null=True, blank=True)
    isAdult = models.BooleanField(null=True)
    startYear = models.IntegerField(null=True, blank=True)
    endYear = models.IntegerField(null=True, blank=True)
    runtimeMinutes = models.IntegerField(null=True, blank=True)

    class Meta:
        db_table = 'movies'
        app_label = 'movies'
        managed = False

    def __str__(self):
        return self.primaryTitle or self.mid


class Person(models.Model):
    pid = models.TextField(primary_key=True)
    primaryName = models.TextField(null=True, blank=True)
    birthYear = models.IntegerField(null=True, blank=True)
    deathYear = models.IntegerField(null=True, blank=True)

    class Meta:
        db_table = 'persons'
        app_label = 'movies'
        managed = False

    def __str__(self):
        return self.primaryName or self.pid


class Rating(models.Model):
    mid = models.OneToOneField(Movie, on_delete=models.CASCADE, primary_key=True, db_column='mid')
    averageRating = models.FloatField(null=True, blank=True)
    numVotes = models.IntegerField(null=True, blank=True)

    class Meta:
        db_table = 'ratings'
        app_label = 'movies'
        managed = False


class Genre(models.Model):
    mid = models.ForeignKey(Movie, on_delete=models.CASCADE, db_column='mid')
    genre = models.TextField()

    class Meta:
        db_table = 'genres'
        app_label = 'movies'
        managed = False
        unique_together = (('mid', 'genre'),)


class Director(models.Model):
    mid = models.ForeignKey(Movie, on_delete=models.CASCADE, db_column='mid')
    pid = models.ForeignKey(Person, on_delete=models.CASCADE, db_column='pid')

    class Meta:
        db_table = 'directors'
        app_label = 'movies'
        managed = False
        unique_together = (('mid', 'pid'),)


class Writer(models.Model):
    mid = models.ForeignKey(Movie, on_delete=models.CASCADE, db_column='mid')
    pid = models.ForeignKey(Person, on_delete=models.CASCADE, db_column='pid')

    class Meta:
        db_table = 'writers'
        app_label = 'movies'
        managed = False
        unique_together = (('mid', 'pid'),)


class Character(models.Model):
    mid = models.ForeignKey(Movie, on_delete=models.CASCADE, db_column='mid')
    pid = models.ForeignKey(Person, on_delete=models.CASCADE, db_column='pid')
    name = models.TextField()

    class Meta:
        db_table = 'characters'
        app_label = 'movies'
        managed = False
        unique_together = (('mid', 'pid', 'name'),)


class Principal(models.Model):
    mid = models.ForeignKey(Movie, on_delete=models.CASCADE, db_column='mid')
    ordering = models.IntegerField()
    pid = models.ForeignKey(Person, on_delete=models.CASCADE, db_column='pid')
    category = models.TextField(null=True, blank=True)
    job = models.TextField(null=True, blank=True)
    characters = models.TextField(null=True, blank=True)

    class Meta:
        db_table = 'principals'
        app_label = 'movies'
        managed = False
        unique_together = (('mid', 'ordering', 'pid', 'category'),)


class Profession(models.Model):
    pid = models.ForeignKey(Person, on_delete=models.CASCADE, db_column='pid')
    jobName = models.TextField()

    class Meta:
        db_table = 'professions'
        app_label = 'movies'
        managed = False
        unique_together = (('pid', 'jobName'),)


class Title(models.Model):
    mid = models.ForeignKey(Movie, on_delete=models.CASCADE, db_column='mid')
    ordering = models.IntegerField()
    title = models.TextField(null=True, blank=True)
    region = models.TextField(null=True, blank=True)
    language = models.TextField(null=True, blank=True)
    types = models.TextField(null=True, blank=True)
    attributes = models.TextField(null=True, blank=True)
    isOriginalTitle = models.BooleanField(null=True)

    class Meta:
        db_table = 'titles'
        app_label = 'movies'
        managed = False
        unique_together = (('mid', 'ordering'),)


class KnownForMovie(models.Model):
    pid = models.ForeignKey(Person, on_delete=models.CASCADE, db_column='pid')
    mid = models.ForeignKey(Movie, on_delete=models.CASCADE, db_column='mid')

    class Meta:
        db_table = 'knownformovies'
        app_label = 'movies'
        managed = False
        unique_together = (('pid', 'mid'),)
