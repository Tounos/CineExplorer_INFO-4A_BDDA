"""
sqlite_service.py
=================
Service pour interagir avec la base SQLite (imdb.db).
Utilisé pour les listes, filtres, recherches et statistiques.
"""

import sqlite3
import os
from pathlib import Path


def get_sqlite_connection():
    """
    Établit la connexion à la base de données SQLite imdb.
    
    Returns:
        sqlite3.Connection: Connexion à la base de données
    """
    from django.conf import settings
    db_path = settings.DATABASES['imdb']['NAME']
    conn = sqlite3.connect(str(db_path))
    # Retourner les résultats sous forme de dictionnaires
    conn.row_factory = sqlite3.Row
    return conn


def get_sqlite_stats():
    """
    Récupère les statistiques globales de la base SQLite.
    
    Returns:
        dict: Statistiques (tables, nombre de lignes, taille)
    """
    from django.conf import settings
    db_path = settings.DATABASES['imdb']['NAME']
    
    conn = get_sqlite_connection()
    cursor = conn.cursor()
    
    # Liste des tables
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [row[0] for row in cursor.fetchall()]
    
    # Nombre de lignes par table
    table_counts = {}
    for table in tables:
        cursor.execute(f"SELECT COUNT(*) FROM {table};")
        table_counts[table] = cursor.fetchone()[0]
    
    conn.close()
    
    # Taille du fichier
    db_size = os.path.getsize(db_path) / (1024 * 1024)  # en MB
    
    return {
        'tables': tables,
        'table_counts': table_counts,
        'total_size_mb': round(db_size, 2)
    }


def get_global_stats():
    """
    Récupère les statistiques globales pour la page d'accueil.
    Nombre de films, acteurs, réalisateurs.
    
    Returns:
        dict: Statistiques globales
    """
    conn = get_sqlite_connection()
    cursor = conn.cursor()
    
    # Nombre de films
    cursor.execute("SELECT COUNT(*) FROM movies")
    movies_count = cursor.fetchone()[0]
    
    # Nombre de personnes
    cursor.execute("SELECT COUNT(*) FROM persons")
    persons_count = cursor.fetchone()[0]
    
    # Nombre de réalisateurs uniques
    cursor.execute("SELECT COUNT(DISTINCT pid) FROM directors")
    directors_count = cursor.fetchone()[0]
    
    # Nombre d'acteurs uniques (catégorie actor ou actress)
    cursor.execute("""
        SELECT COUNT(DISTINCT pid) FROM principals 
        WHERE category IN ('actor', 'actress')
    """)
    actors_count = cursor.fetchone()[0]
    
    conn.close()
    
    return {
        'movies_count': movies_count,
        'persons_count': persons_count,
        'directors_count': directors_count,
        'actors_count': actors_count
    }


def get_all_genres():
    """
    Récupère la liste de tous les genres uniques.
    
    Returns:
        list: Liste de genres triés alphabétiquement
    """
    conn = get_sqlite_connection()
    cursor = conn.cursor()
    
    cursor.execute("SELECT DISTINCT genre FROM genres ORDER BY genre")
    genres = [row[0] for row in cursor.fetchall() if row[0]]
    
    conn.close()
    return genres


def get_movies_list(page=1, per_page=20, genre=None, year_min=None, year_max=None, 
                    rating_min=None, sort_by='rating', sort_order='desc'):
    """
    Récupère une liste paginée de films avec filtres.
    
    Args:
        page: Numéro de page (1-indexed)
        per_page: Nombre de films par page
        genre: Filtre par genre (optionnel)
        year_min: Année minimum (optionnel)
        year_max: Année maximum (optionnel)
        rating_min: Note minimum (optionnel)
        sort_by: Champ de tri ('title', 'year', 'rating')
        sort_order: Ordre de tri ('asc', 'desc')
    
    Returns:
        dict: {movies: list, total: int, pages: int}
    """
    conn = get_sqlite_connection()
    cursor = conn.cursor()
    
    # Construire la requête de base
    base_query = """
        SELECT DISTINCT m.mid, m.primaryTitle, m.startYear, m.runtimeMinutes,
               r.averageRating, r.numVotes
        FROM movies m
        LEFT JOIN ratings r ON m.mid = r.mid
    """
    
    # Conditions WHERE
    conditions = ["m.titleType = 'movie'"]
    params = []
    
    # Filtre par genre (nécessite une jointure)
    if genre:
        base_query += " JOIN genres g ON m.mid = g.mid"
        conditions.append("g.genre = ?")
        params.append(genre)
    
    # Filtre par année
    if year_min:
        conditions.append("m.startYear >= ?")
        params.append(int(year_min))
    if year_max:
        conditions.append("m.startYear <= ?")
        params.append(int(year_max))
    
    # Filtre par note
    if rating_min:
        conditions.append("r.averageRating >= ?")
        params.append(float(rating_min))
    
    # Ajouter les conditions WHERE
    if conditions:
        base_query += " WHERE " + " AND ".join(conditions)
    
    # Tri
    sort_column_map = {
        'title': 'm.primaryTitle',
        'year': 'm.startYear',
        'rating': 'r.averageRating'
    }
    sort_col = sort_column_map.get(sort_by, 'r.averageRating')
    order = 'DESC' if sort_order == 'desc' else 'ASC'
    
    # Compter le total pour la pagination
    count_query = f"SELECT COUNT(*) FROM ({base_query})"
    cursor.execute(count_query, params)
    total = cursor.fetchone()[0]
    
    # Ajouter tri et pagination
    base_query += f" ORDER BY {sort_col} {order} NULLS LAST"
    base_query += " LIMIT ? OFFSET ?"
    params.extend([per_page, (page - 1) * per_page])
    
    # Exécuter la requête
    cursor.execute(base_query, params)
    rows = cursor.fetchall()
    
    # Convertir en liste de dictionnaires
    movies = []
    for row in rows:
        movies.append({
            'mid': row['mid'],
            'primaryTitle': row['primaryTitle'],
            'startYear': row['startYear'],
            'runtimeMinutes': row['runtimeMinutes'],
            'averageRating': row['averageRating'],
            'numVotes': row['numVotes']
        })
    
    conn.close()
    
    # Calculer le nombre de pages
    total_pages = (total + per_page - 1) // per_page
    
    return {
        'movies': movies,
        'total': total,
        'pages': total_pages,
        'current_page': page
    }


def get_top_rated_movies(limit=10):
    """
    Récupère les films les mieux notés.
    
    Args:
        limit: Nombre de films à retourner
    
    Returns:
        list: Liste des top films
    """
    conn = get_sqlite_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT m.mid, m.primaryTitle, m.startYear, m.runtimeMinutes,
               r.averageRating, r.numVotes
        FROM movies m
        JOIN ratings r ON m.mid = r.mid
        WHERE m.titleType = 'movie'
          AND r.numVotes >= 1000
        ORDER BY r.averageRating DESC, r.numVotes DESC
        LIMIT ?
    """, (limit,))
    
    movies = []
    for row in cursor.fetchall():
        movies.append({
            'mid': row['mid'],
            'primaryTitle': row['primaryTitle'],
            'startYear': row['startYear'],
            'runtimeMinutes': row['runtimeMinutes'],
            'averageRating': row['averageRating'],
            'numVotes': row['numVotes']
        })
    
    conn.close()
    return movies


def search_movies(query, limit=20):
    """
    Recherche de films par titre (LIKE).
    Exclut les épisodes TV pour ne retourner que les films/séries.
    
    Args:
        query: Terme de recherche
        limit: Nombre maximum de résultats
    
    Returns:
        list: Liste de films correspondants
    """
    conn = get_sqlite_connection()
    cursor = conn.cursor()
    
    search_term = f"%{query}%"
    
    cursor.execute("""
        SELECT m.mid, m.primaryTitle, m.startYear, m.runtimeMinutes,
               m.titleType, r.averageRating, r.numVotes
        FROM movies m
        LEFT JOIN ratings r ON m.mid = r.mid
        WHERE (m.primaryTitle LIKE ? OR m.originalTitle LIKE ?)
          AND m.titleType NOT IN ('tvEpisode')
        ORDER BY r.averageRating DESC NULLS LAST
        LIMIT ?
    """, (search_term, search_term, limit))
    
    movies = []
    for row in cursor.fetchall():
        movies.append({
            'mid': row['mid'],
            'primaryTitle': row['primaryTitle'],
            'startYear': row['startYear'],
            'runtimeMinutes': row['runtimeMinutes'],
            'titleType': row['titleType'],
            'averageRating': row['averageRating'],
            'numVotes': row['numVotes']
        })
    
    conn.close()
    return movies


def search_persons(query, limit=20):
    """
    Recherche de personnes par nom (LIKE).
    
    Args:
        query: Terme de recherche
        limit: Nombre maximum de résultats
    
    Returns:
        list: Liste de personnes correspondantes
    """
    conn = get_sqlite_connection()
    cursor = conn.cursor()
    
    search_term = f"%{query}%"
    
    cursor.execute("""
        SELECT p.pid, p.primaryName, p.birthYear, p.deathYear,
               (SELECT COUNT(*) FROM principals pr WHERE pr.pid = p.pid) as film_count
        FROM persons p
        WHERE p.primaryName LIKE ?
        ORDER BY film_count DESC
        LIMIT ?
    """, (search_term, limit))
    
    persons = []
    for row in cursor.fetchall():
        persons.append({
            'pid': row['pid'],
            'primaryName': row['primaryName'],
            'birthYear': row['birthYear'],
            'deathYear': row['deathYear'],
            'film_count': row['film_count']
        })
    
    conn.close()
    return persons


def get_movie_characters(movie_id):
    """
    Récupère les personnages joués par les acteurs dans un film.
    Les données sont dans SQLite (table characters).
    
    Args:
        movie_id: L'identifiant du film (ex: 'tt10872600')
    
    Returns:
        dict: {person_id: [list of character names]}
    """
    conn = get_sqlite_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT pid, name
        FROM characters
        WHERE mid = ?
    """, (movie_id,))
    
    characters_map = {}
    for row in cursor.fetchall():
        pid = row['pid']
        char_name = row['name']
        if pid not in characters_map:
            characters_map[pid] = []
        if char_name:
            characters_map[pid].append(char_name)
    
    conn.close()
    return characters_map


def get_genre_stats():
    """
    Récupère le nombre de films par genre pour les statistiques.
    
    Returns:
        dict: {genre: count}
    """
    conn = get_sqlite_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT g.genre, COUNT(*) as count
        FROM genres g
        JOIN movies m ON g.mid = m.mid
        WHERE m.titleType = 'movie'
        GROUP BY g.genre
        ORDER BY count DESC
        LIMIT 15
    """)
    
    result = {row['genre']: row['count'] for row in cursor.fetchall() if row['genre']}
    
    conn.close()
    return result


def get_decade_stats():
    """
    Récupère le nombre de films par décennie.
    
    Returns:
        dict: {décennie: count}
    """
    conn = get_sqlite_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT (startYear / 10) * 10 as decade, COUNT(*) as count
        FROM movies
        WHERE titleType = 'movie' AND startYear IS NOT NULL
        GROUP BY decade
        ORDER BY decade
    """)
    
    result = {int(row['decade']): row['count'] for row in cursor.fetchall() if row['decade']}
    
    conn.close()
    return result


def get_rating_distribution():
    """
    Récupère la distribution des notes (histogramme).
    
    Returns:
        dict: {note: count}
    """
    conn = get_sqlite_connection()
    cursor = conn.cursor()
    
    result = {}
    for i in range(0, 11):
        cursor.execute("""
            SELECT COUNT(*) as count
            FROM ratings r
            JOIN movies m ON r.mid = m.mid
            WHERE m.titleType = 'movie'
              AND r.averageRating >= ? AND r.averageRating < ?
        """, (i, i + 1))
        result[i] = cursor.fetchone()['count']
    
    conn.close()
    return result


def get_top_actors(limit=10):
    """
    Récupère les acteurs les plus prolifiques.
    
    Args:
        limit: Nombre d'acteurs à retourner
    
    Returns:
        list: Liste des top acteurs avec leur nombre de films
    """
    conn = get_sqlite_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT p.pid, p.primaryName, COUNT(DISTINCT pr.mid) as film_count
        FROM persons p
        JOIN principals pr ON p.pid = pr.pid
        WHERE pr.category IN ('actor', 'actress')
        GROUP BY p.pid, p.primaryName
        ORDER BY film_count DESC
        LIMIT ?
    """, (limit,))
    
    actors = []
    for row in cursor.fetchall():
        actors.append({
            'pid': row['pid'],
            'primaryName': row['primaryName'],
            'film_count': row['film_count']
        })
    
    conn.close()
    return actors


def get_person_detail(person_id):
    """
    Récupère les détails d'une personne.
    
    Args:
        person_id: ID de la personne (ex: 'nm0000001')
    
    Returns:
        dict: Détails de la personne ou None
    """
    conn = get_sqlite_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT p.pid, p.primaryName, p.birthYear, p.deathYear
        FROM persons p
        WHERE p.pid = ?
    """, (person_id,))
    
    row = cursor.fetchone()
    if not row:
        conn.close()
        return None
    
    person = {
        'pid': row['pid'],
        'primaryName': row['primaryName'],
        'birthYear': int(row['birthYear']) if row['birthYear'] else None,
        'deathYear': int(row['deathYear']) if row['deathYear'] else None,
    }
    
    # Récupérer les professions
    cursor.execute("""
        SELECT DISTINCT jobName FROM professions WHERE pid = ?
    """, (person_id,))
    person['professions'] = [row['jobName'] for row in cursor.fetchall()]
    
    conn.close()
    return person


def get_person_filmography(person_id):
    """
    Récupère la filmographie complète d'une personne.
    
    Args:
        person_id: ID de la personne
    
    Returns:
        dict: Filmographie groupée par rôle (actor, director, writer, etc.)
    """
    conn = get_sqlite_connection()
    cursor = conn.cursor()
    
    # Récupérer tous les films avec le rôle
    cursor.execute("""
        SELECT DISTINCT m.mid, m.primaryTitle, m.startYear, m.runtimeMinutes,
               r.averageRating, r.numVotes, pr.category,
               (SELECT GROUP_CONCAT(c.name, ', ') 
                FROM characters c 
                WHERE c.mid = m.mid AND c.pid = pr.pid) as characters
        FROM principals pr
        JOIN movies m ON pr.mid = m.mid
        LEFT JOIN ratings r ON m.mid = r.mid
        WHERE pr.pid = ?
        ORDER BY m.startYear DESC NULLS LAST
    """, (person_id,))
    
    # Grouper par catégorie
    filmography = {}
    for row in cursor.fetchall():
        category = row['category'] or 'other'
        if category not in filmography:
            filmography[category] = []
        
        filmography[category].append({
            'mid': row['mid'],
            'primaryTitle': row['primaryTitle'],
            'startYear': row['startYear'],
            'runtimeMinutes': row['runtimeMinutes'],
            'averageRating': row['averageRating'],
            'numVotes': row['numVotes'],
            'characters': row['characters']
        })
    
    conn.close()
    return filmography
