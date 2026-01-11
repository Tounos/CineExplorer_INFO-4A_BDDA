import time
import sys
from pymongo import MongoClient

def print_progress(current, total, start_time):
    """Affiche une barre de progression."""
    percent = 100 * current / total
    filled = int(40 * current // total)
    bar = '█' * filled + '░' * (40 - filled)
    elapsed = time.time() - start_time
    speed = current / elapsed if elapsed > 0 else 0
    eta = (total - current) / speed if speed > 0 else 0
    sys.stdout.write(f'\r|{bar}| {percent:.1f}% | {current:,}/{total:,} | {speed:.0f}/s | ETA: {eta/60:.1f}min')
    sys.stdout.flush()

def create_movies_complete(db):
    """Crée la collection movies_complete avec tout dedans."""
    print("Création de movies_complete")
    start = time.time()
    
    # Compter le nombre total de films
    total_movies = db.movies.count_documents({})
    print(f"Nombre de films à traiter: {total_movies:,}")
    
    pipeline = [
        # Joindre les genres
        {"$lookup": {
            "from": "genres",
            "localField": "mid",
            "foreignField": "mid",
            "as": "genres_data"
        }},
        
        # Joindre les ratings
        {"$lookup": {
            "from": "ratings",
            "localField": "mid",
            "foreignField": "mid",
            "as": "rating_data"
        }},
        
        # Joindre les directors
        {"$lookup": {
            "from": "directors",
            "localField": "mid",
            "foreignField": "mid",
            "as": "directors_data"
        }},
        
        # infos sur les directors
        {"$lookup": {
            "from": "persons",
            "localField": "directors_data.pid",
            "foreignField": "pid",
            "as": "directors_persons"
        }},
        
        # Joindre principals
        {"$lookup": {
            "from": "principals",
            "localField": "mid",
            "foreignField": "mid",
            "as": "principals_data"
        }},
        
        # infos sur le cast
        {"$lookup": {
            "from": "persons",
            "localField": "principals_data.pid",
            "foreignField": "pid",
            "as": "cast_persons"
        }},
        
        # Récupérer les personnages
        {"$lookup": {
            "from": "characters",
            "localField": "mid",
            "foreignField": "mid",
            "as": "characters_data"
        }},

        # writers (juste les IDs)
        {"$lookup": {
            "from": "writers",
            "localField": "mid",
            "foreignField": "mid",
            "as": "writers_data"
        }},
        
        # infos sur les writers
        {"$lookup": {
            "from": "persons",
            "localField": "writers_data.pid",
            "foreignField": "pid",
            "as": "writers_persons"
        }},

        # titles
        {"$lookup": {
            "from": "titles",
            "localField": "mid",
            "foreignField": "mid",
            "as": "titles_data"
        }},

        # Restructuration
        {"$project": {
            "_id": "$mid",
            "title": "$primaryTitle",
            "year": "$startYear",
            "runtime": "$runtimeMinutes",
            
            "genres": {
                "$map": {
                    "input": "$genres_data",
                    "as": "g",
                    "in": "$$g.genre"
                }
            },

            "rating": {
                "$cond": {
                    "if": {"$gt": [{"$size": "$rating_data"}, 0]},
                    "then": {
                        "average": {"$arrayElemAt": ["$rating_data.averageRating", 0]},
                        "votes": {"$arrayElemAt": ["$rating_data.numVotes", 0]}
                    },
                    "else": None
                }
            },
            
            "directors": {
                "$map": {
                    "input": "$directors_persons",
                    "as": "d",
                    "in": {
                        "person_id": "$$d.pid",
                        "name": "$$d.primaryName"
                    }
                }
            },
            
            # Cast avec noms depuis cast_persons
            "cast": {
                "$map": {
                    "input": "$principals_data",
                    "as": "p",
                    "in": {
                        "person_id": "$$p.pid",
                        "ordering": "$$p.ordering",
                        # Trouver le nom dans cast_persons
                        "name": {
                            "$let": {
                                "vars": {
                                    "person": {
                                        "$arrayElemAt": [
                                            {"$filter": {
                                                "input": "$cast_persons",
                                                "as": "cp",
                                                "cond": {"$eq": ["$$cp.pid", "$$p.pid"]}
                                            }},
                                            0
                                        ]
                                    }
                                },
                                "in": "$$person.primaryName"
                            }
                        },
                        # Trouver les personnages dans characters_data
                        "characters": {
                            "$map": {
                                "input": {
                                    "$filter": {
                                        "input": "$characters_data",
                                        "as": "c",
                                        "cond": {"$eq": ["$$c.pid", "$$p.pid"]}
                                    }
                                },
                                "as": "char",
                                "in": "$$char.character"
                            }
                        }
                    }
                }
            },

            # Writers avec noms
            "writers": {
                "$map": {
                    "input": "$writers_data",
                    "as": "w",
                    "in": {
                        "person_id": "$$w.pid",
                        "category": "$$w.category",
                        "name": {
                            "$let": {
                                "vars": {
                                    "person": {
                                        "$arrayElemAt": [
                                            {"$filter": {
                                                "input": "$writers_persons",
                                                "as": "wp",
                                                "cond": {"$eq": ["$$wp.pid", "$$w.pid"]}
                                            }},
                                            0
                                        ]
                                    }
                                },
                                "in": "$$person.primaryName"
                            }
                        }
                    }
                }
            },

            "titles": {
                "$map": {
                    "input": "$titles_data",
                    "as": "t",
                    "in": {
                        "region": "$$t.region",
                        "title": "$$t.title"
                    }
                }
            }
        }}
    ]
    
    # Supprimer l'ancienne si elle existe
    db.movies_complete.drop()
    
    # Exécuter avec progression par lots
    print("Exécution du pipeline d'agrégation...")
    cursor = db.movies.aggregate(pipeline, allowDiskUse=True, batchSize=500)
    
    batch = []
    total_processed = 0
    batch_size = 500
    
    for doc in cursor:
        batch.append(doc)
        if len(batch) >= batch_size:
            db.movies_complete.insert_many(batch)
            total_processed += len(batch)
            print_progress(total_processed, total_movies, start)
            batch = []
    
    # Dernier lot
    if batch:
        db.movies_complete.insert_many(batch)
        total_processed += len(batch)
    
    print_progress(total_processed, total_movies, start)
    print(f"\n{total_processed} films créés")
    print(f"Temps: {time.time() - start:.2f}s")
    return total_processed


def compare_storage(db):
    """Compare la taille des collections."""
    print("\nTAILLE DE STOCKAGE")
    
    # Collections normalisées
    normal_collections = ["movies", "genres", "ratings", "directors", "principals", "persons"]
    total_normal = 0
    
    for coll in normal_collections:
        stats = db.command("collStats", coll)
        size_mb = stats["size"] / (1024 * 1024) #conversion en MB
        total_normal += size_mb
        print(f"{coll} : {size_mb:8.2f} MB")
    
    print(f"{'TOTAL'} : {total_normal:8.2f} MB")
    
    # Collection dénormalisée
    stats = db.command("collStats", "movies_complete")
    size_complete = stats["size"] / (1024 * 1024)
    print(f"\nmovies_complete : {size_complete:8.2f} MB")
    
    ratio = size_complete / total_normal
    print(f"\nRatio: {ratio:.2f}x")


def compare_perf(db):
    """Compare les perfs de lecture."""
    print("\nPERFORMANCES")
    
    movie_id = "tt0111161"  # Shawshank Redemption
    
    # Méthode normalisée (plusieurs requêtes)
    print("\n1. Normalisé (plusieurs requêtes):")
    start = time.time()
    
    movie = db.movies.find_one({"mid": movie_id})
    genres = list(db.genres.find({"mid": movie_id}))
    rating = db.ratings.find_one({"mid": movie_id})
    directors = list(db.directors.find({"mid": movie_id}))
    
    time_normal = (time.time() - start) * 1000
    print(f"Temps: {time_normal:.2f} ms")
    print(f"Requêtes: 4+")
    
    # Méthode dénormalisée (1 requête)
    print("\n2. Dénormalisé (1 requête):")
    start = time.time()
    
    movie_complete = db.movies_complete.find_one({"_id": movie_id})
    
    time_denorm = (time.time() - start) * 1000
    print(f"Temps: {time_denorm:.2f} ms")
    print(f"Requêtes: 1")
    
    # Comparaison
    print(f"\n{time_normal/time_denorm:.1f}x plus rapide")


def compare_code():
    """Compare la complexité du code."""
    print("\nCOMPLEXITÉ DU CODE")
    
    print("\n1. Normalisé:")
    print("   movie = db.movies.find_one({'mid': id})")
    print("   genres = list(db.genres.find({'mid': id}))")
    print("   rating = db.ratings.find_one({'mid': id})")
    print("   directors = list(db.directors.find({'mid': id}))")
    print("   ... (4+ requêtes)")
    
    print("\n2. Dénormalisé:")
    print("   movie = db.movies_complete.find_one({'_id': id})")
    print("   ... (1 requête)")
    
    print("\nBeaucoup plus simple")


if __name__ == "__main__":
    uri = "mongodb://localhost:27017/"
    client = MongoClient(uri)
    db = client["cineexplorer"]
    
    # Créer la collection
    create_movies_complete(db)
    
    # Comparaisons
    compare_storage(db)
    compare_perf(db)
    compare_code()
    
    print("\nFin des comparaisons.")
    client.close()