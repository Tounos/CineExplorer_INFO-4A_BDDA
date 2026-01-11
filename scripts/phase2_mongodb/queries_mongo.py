import time
from pymongo import MongoClient
from typing import List, Dict, Any

def query_actor_filmography(db, actor_name: str) -> List[Dict]:
    """Retourne la filmographie d'un acteur."""
    start = time.time()
    actor = db.persons.find_one({"primaryName": {"$regex": actor_name}})
    actor_pid = actor["pid"]

    principals = list(db.principals.find({"pid": actor_pid}))

    films = []

    for principal in principals:
        movie = db.movies.find_one({"mid": principal["mid"]})
        if movie:
            films.append({
                "primaryTitle": movie["primaryTitle"], 
                "startYear": movie.get("startYear") #get pour eviter KeyError
            })

    print(f"query_actor_filmography: {time.time() - start:.4f}s")
    return films


def top_n_films(db, genre: str, annee_debut: int, annee_fin: int, n: int) -> List[Dict]:
    """Retourne les n films les mieux notés pour un genre et une période."""
    start = time.time()
    pipeline = [
        {"$match": {"startYear": {"$gte": annee_debut, "$lte": annee_fin}}},
        {"$lookup": {"from": "genres", "localField": "mid", "foreignField": "mid", "as": "genres_data"}},
        {"$unwind": "$genres_data"},
        {"$match": {"genres_data.genre": genre}},
        {"$lookup": {"from": "ratings", "localField": "mid", "foreignField": "mid", "as": "rating_data"}},
        {"$unwind": "$rating_data"},
        {"$sort": {"rating_data.averageRating": -1}},
        {"$limit": n},
        {"$project": {"_id": 0, "primaryTitle": 1, "averageRating": "$rating_data.averageRating"}}
    ]
    result = list(db.movies.aggregate(pipeline))
    print(f"top_n_films: {time.time() - start:.4f}s")
    return result


def acteur_multi_roles(db, actor_name: str) -> List[Dict]:
    """Retourne les films où un acteur a joué plusieurs personnages."""
    start = time.time()
    pipeline = [
        {"$match": {"primaryName": {"$regex": actor_name, "$options": "i"}}},
        {"$lookup": {"from": "characters", "localField": "pid", "foreignField": "pid", "as": "characters_data"}},
        {"$unwind": "$characters_data"},
        {"$lookup": {"from": "movies", "localField": "characters_data.mid", "foreignField": "mid", "as": "movie_data"}},
        {"$unwind": "$movie_data"},
        {"$group": {"_id": "$movie_data.mid", "primaryTitle": {"$first": "$movie_data.primaryTitle"}, "nombre_de_roles": {"$sum": 1}}},
        {"$match": {"nombre_de_roles": {"$gt": 1}}},
        {"$sort": {"nombre_de_roles": -1}},
        {"$project": {"_id": 0, "primaryTitle": 1, "nombre_de_roles": 1}} #on renvoie juste le titre et le nombre de rôles dans celui-ci
    ]
    result = list(db.persons.aggregate(pipeline))
    print(f"acteurs_multi_roles: {time.time() - start:.4f}s")
    return result


def collaborations(db, actor_name: str) -> List[Dict]:
    """Retourne les réalisateurs ayant travaillé avec un acteur spécifique."""
    start = time.time()
    pipeline = [
        {"$match": {"primaryName": {"$regex": actor_name, "$options": "i"}}},
        {"$lookup": {"from": "principals", "localField": "pid", "foreignField": "pid", "as": "principals_data"}},
        {"$unwind": "$principals_data"},
        {"$group": {"_id": "$principals_data.mid", "mid": {"$first": "$principals_data.mid"}}},
        {"$lookup": {"from": "directors", "localField": "mid", "foreignField": "mid", "as": "directors_data"}},
        {"$unwind": "$directors_data"},
        {"$lookup": {"from": "persons", "localField": "directors_data.pid", "foreignField": "pid", "as": "director_persons"}},
        {"$unwind": "$director_persons"},
        {"$group": {"_id": "$director_persons.pid", "primaryName": {"$first": "$director_persons.primaryName"}, "nombre_de_films": {"$sum": 1}}},
        {"$sort": {"nombre_de_films": -1}},
        {"$project": {"_id": 0, "primaryName": 1, "nombre_de_films": 1}}
    ]
    result = list(db.persons.aggregate(pipeline))
    print(f"collaborations: {time.time() - start:.4f}s")
    return result


def genres_populaires(db) -> List[Dict]:
    """Retourne les genres ayant une note moyenne > 7.0 et +50 films."""
    start = time.time()
    pipeline = [
        {"$lookup": {"from": "movies", "localField": "mid", "foreignField": "mid", "as": "movie_data"}},
        {"$unwind": "$movie_data"},
        {"$lookup": {"from": "ratings", "localField": "mid", "foreignField": "mid", "as": "rating_data"}},
        {"$unwind": "$rating_data"},
        {"$group": {"_id": "$genre", "note_moyenne": {"$avg": "$rating_data.averageRating"}, "nombre_de_films": {"$sum": 1}}},
        {"$match": {"note_moyenne": {"$gt": 7.0}, "nombre_de_films": {"$gt": 50}}},
        {"$sort": {"note_moyenne": -1}},
        {"$project": {"_id": 0, "genre": "$_id", "note_moyenne": 1, "nombre_de_films": 1}}
    ]
    result = list(db.genres.aggregate(pipeline))
    print(f"genres_populaires: {time.time() - start:.4f}s")
    return result


def classement_par_genre(db) -> List[Dict]:
    """Retourne les 3 meilleurs films pour chaque genre."""
    start = time.time()
    pipeline = [
        {"$lookup": {"from": "movies", "localField": "mid", "foreignField": "mid", "as": "movie_data"}},
        {"$unwind": "$movie_data"},
        {"$lookup": {"from": "ratings", "localField": "mid", "foreignField": "mid", "as": "rating_data"}},
        {"$unwind": "$rating_data"},
        {"$sort": {"genre": 1, "rating_data.averageRating": -1}},
        {"$group": {"_id": "$genre", "films": {"$push": {"primaryTitle": "$movie_data.primaryTitle", "averageRating": "$rating_data.averageRating"}}}},
        {"$project": {"_id": 0, "genre": "$_id", "films": {"$slice": ["$films", 3]}}}
    ]
    result = list(db.genres.aggregate(pipeline))
    print(f"classement_par_genre: {time.time() - start:.4f}s")
    return result


def carriere_propulsee(db) -> List[Dict]:
    """Retourne les personnes ayant percé grâce à un film."""
    start = time.time()
    pipeline = [
        {"$lookup": {"from": "ratings", "localField": "mid", "foreignField": "mid", "as": "rating_data"}},
        {"$unwind": "$rating_data"},
        {"$lookup": {"from": "movies", "localField": "mid", "foreignField": "mid", "as": "movie_data"}},
        {"$unwind": "$movie_data"},
        {"$group": {
            "_id": "$pid",
            "films": {
                "$push": {
                    "mid": "$mid",
                    "primaryTitle": "$movie_data.primaryTitle",
                    "startYear": "$movie_data.startYear",
                    "numVotes": "$rating_data.numVotes"
                }
            }
        }},
        {"$project": {
            "films_faibles": {"$filter": {"input": "$films", "as": "film", "cond": {"$lt": ["$$film.numVotes", 200000]}}},
            "films_forts": {"$filter": {"input": "$films", "as": "film", "cond": {"$gte": ["$$film.numVotes", 200000]}}}
        }},
        {"$match": {"films_faibles": {"$ne": []}, "films_forts": {"$ne": []}}},
        {"$lookup": {"from": "persons", "localField": "_id", "foreignField": "pid", "as": "person_info"}},
        {"$unwind": "$person_info"},
        {"$project": {
            "_id": 0, 
            "primaryName": "$person_info.primaryName", 
            "films_faibles": 1, 
            "films_forts": 1
        }}
    ]
    result = list(db.knownformovies.aggregate(pipeline))
    print(f"carriere_propulsee: {time.time() - start:.4f}s")
    return result


def films_par_realisateur_et_genre(db, director_name: str) -> List[Dict]:
    """Retourne tous les films d'un réalisateur par genre."""
    start = time.time()
    pipeline = [
        {"$match": {"primaryName": {"$regex": director_name, "$options": "i"}}},
        {"$lookup": {"from": "directors", "localField": "pid", "foreignField": "pid", "as": "directors_data"}},
        {"$unwind": "$directors_data"},
        {"$lookup": {"from": "movies", "localField": "directors_data.mid", "foreignField": "mid", "as": "movie_data"}},
        {"$unwind": "$movie_data"},
        {"$lookup": {"from": "genres", "localField": "movie_data.mid", "foreignField": "mid", "as": "genres_data"}},
        {"$unwind": "$genres_data"},
        {"$lookup": {"from": "ratings", "localField": "movie_data.mid", "foreignField": "mid", "as": "rating_data"}},
        {"$unwind": "$rating_data"},
        {"$sort": {"genres_data.genre": 1, "rating_data.averageRating": -1}},
        {"$project": {"_id": 0, "genre": "$genres_data.genre", "primaryTitle": "$movie_data.primaryTitle", "averageRating": "$rating_data.averageRating", "numVotes": "$rating_data.numVotes", "startYear": "$movie_data.startYear"}}
    ]
    result = list(db.persons.aggregate(pipeline))
    print(f"films_par_realisateur_et_genre: {time.time() - start:.4f}s")
    return result



if __name__ == "__main__":
    uri = "mongodb://localhost:27017/"
    client = MongoClient(uri)
    db = client["cineexplorer"]

    actor_name = "Tom Hanks"
    print(f"Filmographie de {actor_name} :")

    filmographie = query_actor_filmography(db, actor_name)

    for titre in filmographie:
        print(f"- {titre.get('primaryTitle')} ({titre.get('startYear')})")

    n = 3
    genre = "Adventure"
    annee_debut = 1980
    annee_fin = 1990

    print(f"\nTop {n} des films de {genre} entre {annee_debut} et {annee_fin} :")

    top_films = top_n_films(db, genre, annee_debut, annee_fin, n)
    for film in top_films:
        print(f"- {film.get('primaryTitle')} | Rating: {film.get('averageRating')}")

    print(f"\nFilms où {actor_name} a joué plusieurs rôles :")
    multi_roles = acteur_multi_roles(db, actor_name)
    for film in multi_roles:
        print(f"- {film.get('primaryTitle')} | Nombre de rôles : {film.get('nombre_de_roles')}")

    print(f"\nRéalisateurs ayant travaillé avec {actor_name} :")
    collaborations_list = collaborations(db, actor_name)
    for director in collaborations_list:
        print(f"- {director.get('primaryName')} | Nombre de films : {director.get('nombre_de_films')}")

    print(f"\nGenres populaires (note moyenne > 7.0 et +50 films) :")
    g_populaires = genres_populaires(db)
    for genre in g_populaires:
        print(f"- {genre.get('genre')} | Note moyenne : {genre.get('note_moyenne')} | Nombre de films : {genre.get('nombre_de_films')}")

    print(f"\nClassement par genre (3 meilleurs films pour chaque genre) :")
    classement = classement_par_genre(db)
    for film in classement:
        print(f"- Genre: {film.get('genre')} | Titre: {film.get('primaryTitle')} | Note: {film.get('averageRating')}")

    person_name = "Tom Holland"
    print(f"\nPersonnes ayant percé grâce à un film :")
    carriere = carriere_propulsee(db)
    for film in carriere:
        print(f"- {film.get('primaryName')} | Titre du film de percée: {film.get('primaryTitle')} | Année: {film.get('startYear')} | Votes: {film.get('numVotes')}")

    director_name = "Steven Spielberg"
    print(f"\nFilms par réalisateur et genre pour {director_name} :")
    films_realisateur = films_par_realisateur_et_genre(db, director_name)
    for film in films_realisateur:
        print(f"- Genre: {film.get('genre')} | Titre: {film.get('primaryTitle')} | Note: {film.get('averageRating')} | Votes: {film.get('numVotes')} | Année: {film.get('startYear')}")

    client.close()