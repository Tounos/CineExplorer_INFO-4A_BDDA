import time
from pymongo import MongoClient
from queries_mongo import (
    query_actor_filmography,
    top_n_films,
    acteur_multi_roles,
    collaborations,
    genres_populaires,
    classement_par_genre,
    carriere_propulsee,
    films_par_realisateur_et_genre
)


def compare_temps_requete(db):
    """Compare le temps pour récupérer un film complet."""
    print("\n" + "="*50)
    print("TEMPS DE REQUÊTE (film complet)")
    print("="*50)
    
    movie_id = "tt0111161"
    nb_tests = 10000
    
    # FLAT
    print(f"\n1. FLAT ({nb_tests} itérations)")
    start = time.time()
    for _ in range(nb_tests):
        movie = db.movies.find_one({"mid": movie_id})
        genres = list(db.genres.find({"mid": movie_id}))
        rating = db.ratings.find_one({"mid": movie_id})
        directors = list(db.directors.find({"mid": movie_id}))
        principals = list(db.principals.find({"mid": movie_id}))
        characters = list(db.characters.find({"mid": movie_id}))
        writers = list(db.writers.find({"mid": movie_id}))
        titles = list(db.titles.find({"mid": movie_id}))
    
    temps_flat = (time.time() - start) * 1000 / nb_tests
    temps_total_flat = time.time() - start
    print(f"   Temps moyen: {temps_flat:.2f} ms")
    print(f"   Temps total: {temps_total_flat:.2f} s")
    
    # STRUCTURÉ
    print(f"\n2. STRUCTURÉ ({nb_tests} itérations)")
    start = time.time()
    for _ in range(nb_tests):
        movie = db.movies_complete.find_one({"_id": movie_id})
    
    temps_struct = (time.time() - start) * 1000 / nb_tests
    temps_total_struct = time.time() - start
    print(f"   Temps moyen: {temps_struct:.2f} ms")
    print(f"   Temps total: {temps_total_struct:.2f} s")
    
    ratio = temps_flat / temps_struct if temps_struct > 0 else 0
    print(f"\n=> Structuré est {ratio:.1f}x plus rapide")
    
    return temps_flat, temps_struct, temps_total_flat, temps_total_struct


def compare_requetes_complexes(db):
    """Mesure le temps des 8 requêtes MongoDB."""
    print("\n" + "="*50)
    print("TEMPS PAR REQUÊTE MONGODB")
    print("="*50)
    
    nb_tests = 3
    resultats = {}
    
    # Q1 - Filmographie
    print("\nQ1 - Filmographie (Tom Hanks)")
    start = time.time()
    for _ in range(nb_tests):
        query_actor_filmography(db, "Tom Hanks")
    resultats["Q1"] = (time.time() - start) * 1000 / nb_tests
    print(f"   Temps moyen: {resultats['Q1']:.2f} ms")
    
    # Q2 - Top N films
    print("\nQ2 - Top 10 films Adventure (1980-1990)")
    start = time.time()
    for _ in range(nb_tests):
        top_n_films(db, "Adventure", 1980, 1990, 10)
    resultats["Q2"] = (time.time() - start) * 1000 / nb_tests
    print(f"   Temps moyen: {resultats['Q2']:.2f} ms")
    
    # Q3 - Acteurs multi-rôles
    print("\nQ3 - Acteurs multi-rôles (Tom Hanks)")
    start = time.time()
    for _ in range(nb_tests):
        acteur_multi_roles(db, "Tom Hanks")
    resultats["Q3"] = (time.time() - start) * 1000 / nb_tests
    print(f"   Temps moyen: {resultats['Q3']:.2f} ms")
    
    # Q4 - Collaborations
    print("\nQ4 - Collaborations (Tom Hanks)")
    start = time.time()
    for _ in range(nb_tests):
        collaborations(db, "Tom Hanks")
    resultats["Q4"] = (time.time() - start) * 1000 / nb_tests
    print(f"   Temps moyen: {resultats['Q4']:.2f} ms")
    
    # Q5 - Genres populaires
    print("\nQ5 - Genres populaires")
    start = time.time()
    for _ in range(nb_tests):
        genres_populaires(db)
    resultats["Q5"] = (time.time() - start) * 1000 / nb_tests
    print(f"   Temps moyen: {resultats['Q5']:.2f} ms")
    
    # Q6 - Classement par genre
    print("\nQ6 - Classement par genre")
    start = time.time()
    for _ in range(nb_tests):
        classement_par_genre(db)
    resultats["Q6"] = (time.time() - start) * 1000 / nb_tests
    print(f"   Temps moyen: {resultats['Q6']:.2f} ms")
    
    # Q7 - Carrière propulsée
    print("\nQ7 - Carrière propulsée")
    start = time.time()
    for _ in range(nb_tests):
        carriere_propulsee(db)
    resultats["Q7"] = (time.time() - start) * 1000 / nb_tests
    print(f"   Temps moyen: {resultats['Q7']:.2f} ms")
    
    # Q8 - Films par réalisateur
    print("\nQ8 - Films par réalisateur (Spielberg)")
    start = time.time()
    for _ in range(nb_tests):
        films_par_realisateur_et_genre(db, "Steven Spielberg")
    resultats["Q8"] = (time.time() - start) * 1000 / nb_tests
    print(f"   Temps moyen: {resultats['Q8']:.2f} ms")
    
    return resultats


def compare_taille_stockage(db):
    """Compare la taille de stockage."""
    print("\n" + "="*50)
    print("TAILLE DE STOCKAGE")
    print("="*50)
    
    collections_flat = ["movies", "genres", "ratings", "directors", 
                        "principals", "persons", "characters", "writers", "titles"]
    
    print("\n1. FLAT (collections séparées)")
    total_flat = 0
    for coll in collections_flat:
        if coll in db.list_collection_names():
            stats = db.command("collStats", coll)
            taille = stats["size"] / (1024 * 1024)
            total_flat += taille
            print(f"   {coll}: {taille:.2f} MB")
    print(f"   TOTAL: {total_flat:.2f} MB")
    
    print("\n2. STRUCTURÉ (une collection)")
    if "movies_complete" in db.list_collection_names():
        stats = db.command("collStats", "movies_complete")
        taille_struct = stats["size"] / (1024 * 1024)
        print(f"   movies_complete: {taille_struct:.2f} MB")
    else:
        taille_struct = 0
    
    if total_flat > 0 and taille_struct > 0:
        ratio = taille_struct / total_flat
        print(f"\n=> Structuré utilise {ratio:.2f}x l'espace")
    
    return total_flat, taille_struct


def compare_complexite_code():
    """Compare la complexité du code."""
    print("\n" + "="*50)
    print("COMPLEXITÉ DU CODE")
    print("="*50)
    
    print("\n1. FLAT (8+ requêtes)")
    print("   movie = db.movies.find_one({'mid': id})")
    print("   genres = list(db.genres.find({'mid': id}))")
    print("   # ... 6 autres requêtes + jointures manuelles")
    
    print("\n2. STRUCTURÉ (1 requête)")
    print("   movie = db.movies_complete.find_one({'_id': id})")
    
    print("\n=> Code beaucoup plus simple avec structuré")


def afficher_resume(temps, taille, requetes):
    """Affiche un résumé."""
    print("\n" + "="*50)
    print("RÉSUMÉ")
    print("="*50)
    
    temps_flat, temps_struct, temps_total_flat, temps_total_struct = temps
    taille_flat, taille_struct = taille
    
    print(f"\n{'Critère':<25} {'FLAT':<15} {'STRUCTURÉ':<15}")
    print("-"*55)
    print(f"{'Temps moyen/requête':<25} {temps_flat:.2f} ms{'':<6} {temps_struct:.2f} ms")
    print(f"{'Temps total (100000 req)':<25} {temps_total_flat:.2f} s{'':<7} {temps_total_struct:.2f} s")
    print(f"{'Taille stockage':<25} {taille_flat:.2f} MB{'':<5} {taille_struct:.2f} MB")
    
    # Tableau SQL vs MongoDB
    print("\n" + "="*50)
    print("COMPARATIF SQL vs MONGODB")
    print("="*50)
    
    sql_times = {
        "Q1": 207.26,
        "Q2": 65.90,
        "Q3": 202.09,
        "Q4": 530.24,
        "Q5": 914.78,
        "Q6": 1704.83,
        "Q7": 8524.95,
        "Q8": 1476.81
    }
    
    print(f"\n{'Requête':<25} {'SQL (ms)':<12} {'MongoDB (ms)':<12} {'Gain':<10}")
    print("-"*60)
    
    for q in ["Q1", "Q2", "Q3", "Q4", "Q5", "Q6", "Q7", "Q8"]:
        sql = sql_times[q]
        mongo = requetes[q]
        gain = sql / mongo if mongo > 0 else 0
        print(f"{q:<25} {sql:<12.2f} {mongo:<12.2f} {gain:.1f}x")
    
    total_sql = sum(sql_times.values())
    total_mongo = sum(requetes.values())
    gain_total = total_sql / total_mongo if total_mongo > 0 else 0
    
    print("-"*60)
    print(f"{'TOTAL':<25} {total_sql:<12.2f} {total_mongo:<12.2f} {gain_total:.1f}x")
    
    print("\nConclusion:")
    print("  + MongoDB = Plus rapide en lecture")
    print("  + Structuré = Code plus simple")
    print("  - Structuré = Plus d'espace disque")


if __name__ == "__main__":
    uri = "mongodb://localhost:27017/"
    client = MongoClient(uri)
    db = client["cineexplorer"]
    
    if "movies_complete" not in db.list_collection_names():
        print("Collection 'movies_complete' non trouvée!")
        print("Lance d'abord: python migrate_structured.py")
        client.close()
        exit(1)
    
    temps = compare_temps_requete(db)
    requetes = compare_requetes_complexes(db)
    taille = compare_taille_stockage(db)
    compare_complexite_code()
    afficher_resume(temps, taille, requetes)
    
    print("\nFin des comparaisons.")
    client.close()