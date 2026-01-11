from pymongo import MongoClient
import time      

def create_indexes():
    uri = "mongodb://localhost:27017/"
    client = MongoClient(uri)
    db = client["cineexplorer"]
    print("\nCréation des index...")

    start = time.time()
        
    # Index sur movies
    db.movies.create_index([("mid", 1)])
    db.movies.create_index([("startYear", 1)])
    print("Index movies créés")

    # Index sur genres
    db.genres.create_index([("mid", 1)])
    db.genres.create_index([("genre", 1)])
    print("Index genres créés")

    # Index sur ratings
    db.ratings.create_index([("mid", 1)])
    print("Index ratings créés")

    # Index sur directors
    db.directors.create_index([("mid", 1)])
    db.directors.create_index([("pid", 1)])
    print("Index directors créés")

    # Index sur principals
    db.principals.create_index([("mid", 1)])
    db.principals.create_index([("pid", 1)])
    print("Index principals créés")

    # Index sur persons
    db.persons.create_index([("pid", 1)])
    db.persons.create_index([("primaryName", 1)])
    print("Index persons créés")

    # Index sur characters
    db.characters.create_index([("mid", 1)])
    db.characters.create_index([("pid", 1)])
    print("Index characters créés")

    # Index sur writers
    db.writers.create_index([("mid", 1)])
    db.writers.create_index([("pid", 1)])
    print("Index writers créés")

    # Index sur titles
    db.titles.create_index([("mid", 1)])
    print("Index titles créés")

    elapsed = time.time() - start

    print(f"\nTous les index ont été créés avec succès en {elapsed:.2f} secondes.")

    client.close()

if __name__ == "__main__":
    create_indexes()