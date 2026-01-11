"""
mongo_service.py
================
Service pour interagir avec la base MongoDB.
Utilisé principalement pour les détails de films (collection movies_complete).
"""

from pymongo import MongoClient
from django.conf import settings
import random as random_module


def get_mongo_connection():
    """
    Établit la connexion MongoDB vers le replica set.
    Retourne l'objet database.
    """
    client = MongoClient(settings.MONGODB_URI)
    db = client[settings.MONGODB_NAME]
    return db


def get_mongo_stats():
    """
    Récupère les statistiques globales de la base MongoDB.
    Retourne le nombre de collections, documents par collection, et taille totale.
    """
    db = get_mongo_connection()
    collections = db.list_collection_names()
    
    # Nombre de documents par collection
    collection_counts = {}
    for collection in collections:
        collection_counts[collection] = db[collection].count_documents({})
    
    # Taille totale de la base
    stats = db.command('dbStats')
    db_size_mb = stats.get('dataSize', 0) / (1024 * 1024)  # en MB
    
    # Statistiques supplémentaires pour la page d'accueil
    movies_count = db['movies'].count_documents({})
    persons_count = db['persons'].count_documents({})
    directors_count = db['directors'].count_documents({})
    
    return {
        'collections': collections,
        'collection_counts': collection_counts,
        'total_size_mb': round(db_size_mb, 2),
        'movies_count': movies_count,
        'persons_count': persons_count,
        'directors_count': directors_count
    }


def get_movie_detail_mongo(movie_id):
    """
    Récupère les détails complets d'un film depuis la collection movies_complete.
    Cette collection contient les données dénormalisées (cast, directors, etc.).
    Enrichit les personnages avec les données de SQLite (plus fiables).
    
    Args:
        movie_id: L'identifiant du film (ex: 'tt0000177')
    
    Returns:
        dict: Document complet du film ou None si non trouvé
    """
    db = get_mongo_connection()
    
    # Chercher dans movies_complete (documents pré-agrégés)
    movie = db['movies_complete'].find_one({'_id': movie_id})
    
    if movie:
        # Renommer _id en id pour compatibilité avec les templates Django
        movie['id'] = movie.pop('_id')
        
        # Enrichir le cast avec les personnages de SQLite (plus fiables que MongoDB)
        from .sqlite_service import get_movie_characters
        characters_map = get_movie_characters(movie_id)
        
        if movie.get('cast') and characters_map:
            for actor in movie['cast']:
                person_id = actor.get('person_id')
                if person_id and person_id in characters_map:
                    # Remplacer les personnages MongoDB par ceux de SQLite
                    actor['characters'] = characters_map[person_id]
        
        return movie
    
    # Si pas trouvé dans movies_complete, essayer la collection movies simple
    movie = db['movies'].find_one({'mid': movie_id})
    if movie:
        # Convertir en format compatible
        return {
            'id': movie.get('mid'),
            'title': movie.get('primaryTitle'),
            'year': movie.get('startYear'),
            'runtime': movie.get('runtimeMinutes'),
            'genres': [],
            'rating': None,
            'directors': [],
            'cast': [],
            'writers': [],
            'titles': []
        }
    
    return None


def search_movies_mongo(query, limit=20):
    """
    Recherche des films par titre dans MongoDB.
    Utilise une recherche case-insensitive.
    
    Args:
        query: Terme de recherche
        limit: Nombre maximum de résultats
    
    Returns:
        list: Liste de films correspondants
    """
    db = get_mongo_connection()
    
    # Recherche dans movies_complete d'abord (plus de données)
    results = list(db['movies_complete'].find(
        {'title': {'$regex': query, '$options': 'i'}},
        {'_id': 1, 'title': 1, 'year': 1, 'rating': 1, 'genres': 1}
    ).limit(limit))
    
    # Si pas assez de résultats, chercher aussi dans movies
    if len(results) < limit:
        simple_results = list(db['movies'].find(
            {'primaryTitle': {'$regex': query, '$options': 'i'}},
            {'mid': 1, 'primaryTitle': 1, 'startYear': 1}
        ).limit(limit - len(results)))
        
        # Convertir au même format
        for movie in simple_results:
            results.append({
                '_id': movie.get('mid'),
                'title': movie.get('primaryTitle'),
                'year': movie.get('startYear'),
                'rating': None,
                'genres': []
            })
    
    return results


def get_similar_movies(movie_id, genres, limit=6):
    """
    Récupère des films similaires basés sur les genres.
    
    Args:
        movie_id: ID du film actuel (à exclure des résultats)
        genres: Liste de genres à rechercher
        limit: Nombre maximum de films à retourner
    
    Returns:
        list: Liste de films similaires
    """
    db = get_mongo_connection()
    
    if not genres:
        return []
    
    # Recherche de films avec au moins un genre en commun
    similar = list(db['movies_complete'].find(
        {
            '_id': {'$ne': movie_id},  # Exclure le film actuel
            'genres': {'$in': genres},  # Au moins un genre en commun
            'rating.average': {'$exists': True}  # Avoir une note
        },
        {'_id': 1, 'title': 1, 'year': 1, 'rating': 1, 'genres': 1}
    ).sort('rating.average', -1).limit(limit))
    
    # Renommer _id en id pour compatibilité avec les templates Django
    for movie in similar:
        movie['id'] = movie.pop('_id')
    
    return similar


def get_top_movies(limit=10):
    """
    Récupère les films les mieux notés depuis MongoDB.
    
    Args:
        limit: Nombre de films à retourner
    
    Returns:
        list: Liste des top films triés par note décroissante
    """
    db = get_mongo_connection()
    
    # Récupérer les films les mieux notés avec un minimum de votes
    top_movies = list(db['movies_complete'].find(
        {
            'rating.average': {'$exists': True, '$gte': 7.0},
            'rating.votes': {'$exists': True, '$gte': 1000}
        },
        {'_id': 1, 'title': 1, 'year': 1, 'rating': 1, 'genres': 1, 'directors': 1}
    ).sort('rating.average', -1).limit(limit))
    
    # Renommer _id en id pour compatibilité avec les templates Django
    for movie in top_movies:
        movie['id'] = movie.pop('_id')
    
    return top_movies


def get_random_movies(limit=6):
    """
    Récupère des films aléatoires depuis MongoDB.
    Utilise l'agrégation $sample pour une sélection vraiment aléatoire.
    
    Args:
        limit: Nombre de films à retourner
    
    Returns:
        list: Liste de films aléatoires
    """
    db = get_mongo_connection()
    
    # Utiliser $sample pour obtenir des films aléatoires avec une note
    random_movies = list(db['movies_complete'].aggregate([
        {'$match': {
            'rating.average': {'$exists': True, '$gte': 5.0}
        }},
        {'$sample': {'size': limit}},
        {'$project': {
            '_id': 1, 'title': 1, 'year': 1, 'rating': 1, 'genres': 1
        }}
    ]))
    
    # Renommer _id en id pour compatibilité avec les templates Django
    for movie in random_movies:
        movie['id'] = movie.pop('_id')
    
    return random_movies


def get_genres_list():
    """
    Récupère la liste de tous les genres uniques.
    
    Returns:
        list: Liste de genres triés alphabétiquement
    """
    db = get_mongo_connection()
    
    # Utiliser distinct pour obtenir tous les genres uniques
    genres = db['movies_complete'].distinct('genres')
    
    # Filtrer les valeurs nulles et trier
    genres = sorted([g for g in genres if g])
    
    return genres


def get_movies_by_genre_stats():
    """
    Récupère le nombre de films par genre via agrégation MongoDB.
    
    Returns:
        dict: Dictionnaire {genre: count}
    """
    db = get_mongo_connection()
    
    pipeline = [
        {'$unwind': '$genres'},
        {'$group': {'_id': '$genres', 'count': {'$sum': 1}}},
        {'$sort': {'count': -1}},
        {'$limit': 15}
    ]
    
    result = list(db['movies_complete'].aggregate(pipeline))
    
    return {item['_id']: item['count'] for item in result if item['_id']}


def get_movies_by_decade_stats():
    """
    Récupère le nombre de films par décennie via agrégation MongoDB.
    
    Returns:
        dict: Dictionnaire {décennie: count}
    """
    db = get_mongo_connection()
    
    pipeline = [
        {'$match': {'year': {'$exists': True, '$ne': None}}},
        {'$group': {
            '_id': {'$multiply': [{'$floor': {'$divide': ['$year', 10]}}, 10]},
            'count': {'$sum': 1}
        }},
        {'$sort': {'_id': 1}}
    ]
    
    result = list(db['movies_complete'].aggregate(pipeline))
    
    return {int(item['_id']): item['count'] for item in result if item['_id']}
