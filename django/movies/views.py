"""
views.py
========
Vues Django pour l'application CineExplorer.
Gère les 5 pages principales : accueil, liste, détail, recherche, statistiques.
"""

from django.shortcuts import render, get_object_or_404
from django.http import Http404

# Import des services (SQLite et MongoDB)
from .mongo_service import (
    get_mongo_stats, 
    get_movie_detail_mongo, 
    get_similar_movies,
    get_top_movies as get_top_movies_mongo,
    get_random_movies
)
from .sqlite_service import (
    get_sqlite_stats,
    get_global_stats,
    get_all_genres,
    get_movies_list,
    get_top_rated_movies,
    search_movies,
    search_persons,
    get_genre_stats,
    get_decade_stats,
    get_rating_distribution,
    get_top_actors,
    get_person_detail,
    get_person_filmography
)


def home(request):
    """
    Page d'accueil (/)
    Affiche :
    - Statistiques globales (nombre de films, acteurs, réalisateurs)
    - Top 10 des films les mieux notés
    - Formulaire de recherche rapide
    - Films aléatoires (sélection du jour)
    
    Base utilisée : SQLite pour les stats, MongoDB pour top films et aléatoires
    """
    # Statistiques globales depuis SQLite (plus rapide pour les COUNT)
    stats = get_global_stats()
    
    # Top 10 films - essayer MongoDB d'abord, sinon SQLite
    try:
        top_movies = get_top_movies_mongo(limit=10)
    except Exception:
        # Fallback sur SQLite si MongoDB échoue
        top_movies = get_top_rated_movies(limit=10)
    
    # Films aléatoires depuis MongoDB (utilise $sample)
    try:
        random_movies = get_random_movies(limit=6)
    except Exception:
        random_movies = []
    
    context = {
        'stats': stats,
        'top_movies': top_movies,
        'random_movies': random_movies,
    }
    return render(request, 'movies/index.html', context)


def movie_list(request):
    """
    Liste des films (/movies/)
    Affiche :
    - Liste paginée des films (20 par page)
    - Filtres : genre, année (min/max), note minimale
    - Tri : titre, année, note (ASC/DESC)
    
    Base utilisée : SQLite (requêtes relationnelles efficaces pour les filtres)
    """
    # Récupérer les paramètres de filtrage
    page = int(request.GET.get('page', 1))
    genre = request.GET.get('genre', '')
    year_min = request.GET.get('year_min', '')
    year_max = request.GET.get('year_max', '')
    rating_min = request.GET.get('rating_min', '')
    sort_by = request.GET.get('sort', 'rating')  # title, year, rating
    sort_order = request.GET.get('order', 'desc')  # asc, desc
    
    # Récupérer la liste des films depuis SQLite
    result = get_movies_list(
        page=page,
        per_page=20,
        genre=genre if genre else None,
        year_min=year_min if year_min else None,
        year_max=year_max if year_max else None,
        rating_min=rating_min if rating_min else None,
        sort_by=sort_by,
        sort_order=sort_order
    )
    
    # Récupérer la liste des genres pour le filtre
    genres = get_all_genres()
    
    # Construire la plage de pages pour la pagination
    current_page = result['current_page']
    total_pages = result['pages']
    
    # Afficher 5 pages autour de la page courante
    start_page = max(1, current_page - 2)
    end_page = min(total_pages, current_page + 2)
    page_range = range(start_page, end_page + 1)
    
    context = {
        'movies': result['movies'],
        'total': result['total'],
        'current_page': current_page,
        'total_pages': total_pages,
        'page_range': page_range,
        'has_previous': current_page > 1,
        'has_next': current_page < total_pages,
        'previous_page': current_page - 1,
        'next_page': current_page + 1,
        'genres': genres,
        # Conserver les filtres actuels
        'current_filters': {
            'genre': genre,
            'year_min': year_min,
            'year_max': year_max,
            'rating_min': rating_min,
            'sort': sort_by,
            'order': sort_order,
        }
    }
    return render(request, 'movies/movie_list.html', context)


def movie_detail(request, movie_id):
    """
    Détail d'un film (/movies/<id>/)
    Affiche :
    - Toutes les informations du film
    - Casting complet avec personnages
    - Réalisateurs et scénaristes
    - Titres alternatifs par région
    - Films similaires (même genre/réalisateur)
    
    Base utilisée : MongoDB (collection movies_complete, document pré-agrégé)
    """
    # Récupérer le film depuis MongoDB (document complet)
    movie = get_movie_detail_mongo(movie_id)
    
    if not movie:
        raise Http404("Film non trouvé")
    
    # Récupérer les films similaires (basés sur les genres)
    similar_movies = []
    if movie.get('genres'):
        similar_movies = get_similar_movies(
            movie_id, 
            movie['genres'][:2],  # Utiliser les 2 premiers genres
            limit=6
        )
    
    context = {
        'movie': movie,
        'similar_movies': similar_movies,
    }
    return render(request, 'movies/movie_detail.html', context)


def search(request):
    """
    Page de recherche (/search/?q=...)
    Affiche :
    - Résultats de recherche par titre de film
    - Résultats de recherche par nom de personne
    - Résultats groupés par type
    
    Base utilisée : SQLite (LIKE - simple et suffisant)
    """
    query = request.GET.get('q', '').strip()
    
    # Résultats vides par défaut
    results = {
        'movies': [],
        'persons': [],
        'total': 0
    }
    
    if query and len(query) >= 2:
        # Recherche de films
        movies = search_movies(query, limit=20)
        
        # Recherche de personnes
        persons = search_persons(query, limit=20)
        
        results = {
            'movies': movies,
            'persons': persons,
            'total': len(movies) + len(persons)
        }
    
    context = {
        'query': query,
        'results': results,
    }
    return render(request, 'movies/search.html', context)


def stats(request):
    """
    Page de statistiques (/stats/)
    Affiche :
    - Films par genre (bar chart)
    - Films par décennie (line chart)
    - Distribution des notes (histogram)
    - Top 10 acteurs prolifiques
    - Statistiques des bases de données
    
    Base utilisée : SQLite (agrégations SQL efficaces)
    """
    # Statistiques pour les graphiques
    genre_stats = get_genre_stats()
    decade_stats = get_decade_stats()
    rating_distribution = get_rating_distribution()
    top_actors = get_top_actors(limit=10)
    
    # Statistiques des bases de données
    sqlite_stats = get_sqlite_stats()
    
    try:
        mongo_stats = get_mongo_stats()
    except Exception:
        mongo_stats = {'collections': [], 'collection_counts': {}, 'total_size_mb': 0}
    
    context = {
        'genre_stats': genre_stats,
        'decade_stats': decade_stats,
        'rating_distribution': rating_distribution,
        'top_actors': top_actors,
        'sqlite_stats': sqlite_stats,
        'mongo_stats': mongo_stats,
    }
    return render(request, 'movies/stats.html', context)


def person_detail(request, person_id):
    """
    Page de détail d'une personne (/person/<id>/)
    Affiche :
    - Informations personnelles (nom, dates, professions)
    - Filmographie complète groupée par rôle
    
    Base utilisée : SQLite
    """
    # Récupérer les détails de la personne
    person = get_person_detail(person_id)
    
    if not person:
        raise Http404("Personne non trouvée")
    
    # Récupérer la filmographie
    filmography = get_person_filmography(person_id)
    
    # Compter le nombre total de films
    total_films = sum(len(films) for films in filmography.values())
    
    # Ordre d'affichage des catégories
    category_order = ['actor', 'actress', 'director', 'writer', 'producer', 'composer', 'cinematographer', 'editor']
    sorted_filmography = []
    
    for cat in category_order:
        if cat in filmography:
            sorted_filmography.append((cat, filmography[cat]))
    
    # Ajouter les catégories restantes
    for cat, films in filmography.items():
        if cat not in category_order:
            sorted_filmography.append((cat, films))
    
    context = {
        'person': person,
        'filmography': sorted_filmography,
        'total_films': total_films,
    }
    return render(request, 'movies/person_detail.html', context)
