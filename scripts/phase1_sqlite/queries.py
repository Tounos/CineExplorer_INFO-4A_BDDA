import sqlite3
import os

def query_actor_filmography(conn, actor_name: str) -> list:
    """
    Retourne la filmographie d’un acteur.
    Args:
        conn: Connexion SQLite
        actor_name: Nom de l’acteur (ex: "Tom Hanks")
    Returns:
        Liste de tuples (titre)
    SQL utilisé:
        SELECT m.primaryTitle FROM movies m OIN principals p ON m.mid = p.mid JOIN persons pe ON p.pid = pe.pid LEFT JOIN characters c ON c.mid = m.mid AND c.pid = pe.pid WHERE pe.primaryName LIKE ?
    """

    sql = """
        SELECT m.primaryTitle
        FROM movies m
        JOIN principals p ON m.mid = p.mid
        JOIN persons pe ON p.pid = pe.pid
        WHERE pe.primaryName LIKE ?
        ORDER BY m.startYear ASC
    """
    return conn.execute(sql, (f'%{actor_name}%',)).fetchall()


def top_n_films(conn, genre: str, annee_debut: str, annee_fin: str, n: int) -> list:
    """
    Retourne les n films les mieux notés.
    Args:
        conn: Connexion SQLite
        genre : Genre du film
        annee_debut : Année de début
        annee_fin : Année de fin
        n: Nombre de films à retourner
    Returns:
        Liste de tuples (titre, année, note)
    SQL utilisé:
        SELECT m.primaryTitle, r.averageRating FROM movies m JOIN ratings r ON m.mid = r.mid JOIN genres g ON m.mid = g.mid WHERE g.genre = ? AND m.startYear BETWEEN ? AND ? ORDER BY r.averageRating DESC LIMIT ?
    """
    sql = """
        SELECT m.primaryTitle, r.averageRating
        FROM movies m
        JOIN ratings r ON m.mid = r.mid
        JOIN genres g ON m.mid = g.mid
        WHERE g.genre = ?
        AND m.startYear BETWEEN ? AND ?
        ORDER BY r.averageRating DESC
        LIMIT ?
    """
    return conn.execute(sql, (genre, annee_debut, annee_fin, n)).fetchall()

def acteurs_multi_roles(conn, actor_name: str) -> list:
    """
    Retourne les films où un acteur a joué plusieurs personnages triés par nombre de rôles.
    Args:
        conn: Connexion SQLite
        actor_name: Nom de l’acteur (ex: "Tom Hanks")
    Returns:
        Liste de tuples (titre, nombre_de_rôles)
    SQL utilisé:
        SELECT m.primaryTitle, COUNT(c.name) as nombre_de_roles FROM movies m JOIN characters c ON m.mid = c.mid JOIN persons p ON c.pid = p.pid WHERE p.primaryName LIKE ? GROUP BY m.mid HAVING nombre_de_roles > 1 ORDER BY nombre_de_roles DESC
    """

    sql = """
        SELECT m.primaryTitle, COUNT(c.name) as nombre_de_roles
        FROM movies m
        JOIN characters c ON m.mid = c.mid
        JOIN persons p ON c.pid = p.pid
        WHERE p.primaryName LIKE ?
        GROUP BY m.mid
        HAVING nombre_de_roles > 1
        ORDER BY nombre_de_roles DESC
        
    """
    return conn.execute(sql, (f'%{actor_name}%',)).fetchall()

def collaborations(conn, actor_name: str) -> list:
    """
    Retourne les réalisateurs ayant travaillé avec un acteur spécifique, avec le nombre de films ensemble
    Args:
        conn: Connexion SQLite
        actor_name: Nom de l’acteur
    Returns:
        Liste de tuples (nom_du_réalisateur, nombre_de_films)
    SQL utilisé:
        SELECT pe.primaryName, COUNT(m.mid) as nombre_de_films FROM persons pe JOIN directors d ON pe.pid = d.pid JOIN movies m ON d.mid = m.mid WHERE m.mid IN ( SELECT m2.mid FROM movies m2 JOIN principals pr ON m2.mid = pr.mid JOIN persons pe ON pr.pid = pe.pid LEFT JOIN characters c ON c.mid = m2.mid AND c.pid = pe.pid WHERE pe.primaryName LIKE ? ) GROUP BY pe.pid ORDER BY nombre_de_films DESC
    """

    sql = """
        SELECT pe.primaryName, COUNT(m.mid) as nombre_de_films
        FROM persons pe
        JOIN directors d ON pe.pid = d.pid
        JOIN movies m ON d.mid = m.mid
        WHERE m.mid IN (
            SELECT m2.mid
            FROM movies m2
            JOIN principals pr ON m2.mid = pr.mid
            JOIN persons pe ON pr.pid = pe.pid
            LEFT JOIN characters c ON c.mid = m2.mid AND c.pid = pe.pid
            WHERE pe.primaryName LIKE ?
        )
        GROUP BY pe.pid
        ORDER BY nombre_de_films DESC
    """
    return conn.execute(sql, (f'%{actor_name}%',)).fetchall()

def genres_populaires(conn) -> list:
    """
    Retourne les genres ayant une note moyenne > 7.0 et +50 films, triés par note
    Args:
        conn: Connexion SQLite
    Returns:
        Liste de tuples (genre, note_moyenne, nombre_de_films)
        SQL utilisé:
            SELECT g.genre, AVG(r.averageRating) as note_moyenne, COUNT(m.mid) as nombre_de_films FROM genres g JOIN movies m ON g.mid = m.mid JOIN ratings r ON m.mid = r.mid GROUP BY g.genre HAVING note_moyenne > 7.0 AND nombre_de_films > 50 ORDER BY note_moyenne DESC
    """

    sql = """
        SELECT g.genre, AVG(r.averageRating) as note_moyenne, COUNT(m.mid) as nombre_de_films
        FROM genres g
        JOIN movies m ON g.mid = m.mid
        JOIN ratings r ON m.mid = r.mid
        GROUP BY g.genre
        HAVING note_moyenne > 7.0 
        AND nombre_de_films > 50
        ORDER BY note_moyenne DESC
        """
    return conn.execute(sql).fetchall()

def classement_par_genre(conn) -> list:
    """
    Retourne les 3 meilleurs films pour chaque genre.
    Args:
        conn: Connexion SQLite
    Returns:
        Liste de tuples (genre, titre, note)
    SQL utilisé:
        SELECT g.genre, m.primaryTitle, r.averageRating FROM genres g JOIN movies m ON g.mid = m.mid JOIN ratings r ON m.mid = r.mid ORDER BY g.genre, r.averageRating DESC
    """

    sql = """
        WITH films_classes AS ( SELECT g.genre, m.primaryTitle, r.averageRating, RANK() OVER (PARTITION BY g.genre ORDER BY r.averageRating DESC) as rang FROM movies m JOIN genres g ON m.mid = g.mid JOIN ratings r ON m.mid = r.mid ) SELECT genre, primaryTitle, averageRating, rang FROM films_classes WHERE rang <= 3 ORDER BY genre, rang  
    """
    return conn.execute(sql).fetchall()

def carriere_propulsee(conn) -> list:
    """
    Retourne les personnes ayant percé grâce à un film (avant : film < 200k votes, après : films > 200k votes)
    Args:
        conn: Connexion SQLite
    Returns:
        Liste de tuples (nom_de_la_personne, titre_du_film_de_percee, année_du_film_de_percee)
    SQL utilisé:
        SELECT per.primaryName, m.primaryTitle, m.startYear FROM persons per JOIN knownformovies kfm ON per.pid = kfm.pid JOIN movies m ON kfm.mid = m.mid JOIN ratings r ON m.mid = r.mid WHERE r.numVotes > 200000 AND IN ( SELECT m2.mid FROM movies m2 JOIN knownformovies kfm2 ON m2.mid = kfm2.mid JOIN ratings r2 ON m2.mid = r2.mid WHERE r2.numVotes < 200000) GROUP BY per.primaryName, m.primaryTitle ORDER BY m.startYear ASC
    """

    sql = """
        WITH film_de_percee AS (
            SELECT DISTINCT
                kfm.pid,
                m.mid,
                m.primaryTitle,
                m.startYear,
                r.numVotes
            FROM knownformovies kfm
            JOIN movies m ON kfm.mid = m.mid
            JOIN ratings r ON m.mid = r.mid
            WHERE r.numVotes > 200000
        ),
        avant_perce AS (
            SELECT kfm.pid, m.primaryTitle, m.startYear
            FROM knownformovies kfm
            JOIN movies m ON kfm.mid = m.mid
            JOIN ratings r ON m.mid = r.mid
            WHERE r.numVotes < 200000
            GROUP BY kfm.pid
            HAVING COUNT(DISTINCT m.mid) > 0
        )
        SELECT DISTINCT
            per.primaryName,
            fp.primaryTitle,
            fp.startYear,
            r.numVotes
        FROM avant_perce avp
        JOIN persons per ON avp.pid = per.pid
        JOIN film_de_percee fp ON avp.pid = fp.pid
        JOIN ratings r ON fp.mid = r.mid
        GROUP BY per.primaryName, avp.primaryTitle
        ORDER BY avp.startYear ASC
        """
    return conn.execute(sql).fetchall()

def films_par_realisateur_et_genre(conn, director_name: str) -> list:
    """
    Retourne tous les films d'un réalisateur par genre, avec la note moyenne et le nombre de votes.
    Args:
        conn: Connexion SQLite
        director_name: Nom du réalisateur (ex: "Steven Spielberg")
    Returns:
        Liste de tuples (genre, titre, note, votes)
    SQL utilisé:
        Utilise 4 jointures pour croiser:
        - persons (réalisateurs)
        - directors (liens réalisateurs-films)
        - movies (films)
        - genres (genres des films)
        - ratings (notes et votes)
    """

    sql = """
        SELECT 
            g.genre,
            m.primaryTitle,
            r.averageRating,
            r.numVotes,
            m.startYear
        FROM persons pe
        JOIN directors d ON pe.pid = d.pid
        JOIN movies m ON d.mid = m.mid
        JOIN genres g ON m.mid = g.mid
        JOIN ratings r ON m.mid = r.mid
        WHERE pe.primaryName LIKE ?
        ORDER BY g.genre, r.averageRating DESC
    """
    return conn.execute(sql, (f'%{director_name}%',)).fetchall()

if __name__ == "__main__":

    script_dir = os.path.dirname(os.path.abspath(__file__))
    chemin_db = os.path.join(script_dir, "..", "..", "data", "csv", "imdb.db")
    chemin_csv = os.path.join(script_dir, "..", "..", "data", "csv") + os.sep

    conn = sqlite3.connect(chemin_db)

    filmographie = query_actor_filmography(conn, "Tom Hanks")

    print(f"Filmographie de Tom Hanks :")
    for titre in filmographie:
        print(f"- {titre[0]}")

    n = 10
    genre = "Adventure"
    annee_debut = "1980"
    annee_fin = "1990"

    print(f"\nTop {n} des films de {genre} entre {annee_debut} et {annee_fin} :")

    top_films = top_n_films(conn, genre, annee_debut, annee_fin, n)
    for titre, rating in top_films:
        print(f"- {titre} | Rating: {rating}")

    actor_name = "Tom Hanks"
    print(f"\nFilms où {actor_name} a joué plusieurs rôles :")
    multi_roles = acteurs_multi_roles(conn, actor_name)
    for titre, nombre_de_roles in multi_roles:
        print(f"- {titre} | Nombre de rôles : {nombre_de_roles}")

    print(f"\nRéalisateurs ayant travaillé avec {actor_name} :")
    collaborations_list = collaborations(conn, actor_name)
    for director_name, nombre_de_films in collaborations_list:
        print(f"- {director_name} | Nombre de films : {nombre_de_films}")

    print(f"\nGenres populaires (note moyenne > 7.0 et +50 films) :")
    g_populaires = genres_populaires(conn)
    for genre, note_moyenne, nombre_de_films in g_populaires:
        print(f"- {genre} | Note moyenne : {note_moyenne} | Nombre de films : {nombre_de_films}")

    print(f"\nClassement par genre (3 meilleurs films pour chaque genre) :")
    classement = classement_par_genre(conn)
    for genre, titre, rating, rank in classement:
        print(f"- Genre: {genre} | Titre: {titre} | Note: {rating} | Rang: {rank}")

    person_name = "Tom Holland"
    print(f"\nPersonnes ayant percé grâce à un film :")
    carriere = carriere_propulsee(conn)
    for person_name, titre, annee, votes in carriere:
        print(f"- {person_name} | Titre du film de percée: {titre} | Année: {annee} | Votes: {votes}")

    director_name = "Steven Spielberg"
    print(f"\nFilms par réalisateur et genre pour {director_name} :")
    films_realisateur = films_par_realisateur_et_genre(conn, director_name)
    for genre, titre, rating, votes, annee in films_realisateur:
        print(f"- Genre: {genre} | Titre: {titre} | Note: {rating} | Votes: {votes} | Année: {annee}")

    conn.close()