import sqlite3
import time
import os
from queries import (
    query_actor_filmography,
    top_n_films,
    acteurs_multi_roles,
    collaborations,
    genres_populaires,
    classement_par_genre,
    carriere_propulsee,
    films_par_realisateur_et_genre
)

class BenchmarkDB:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self.conn.execute("PRAGMA foreign_keys = ON")

    # Mesurer le temps d'une requête
    def measure_query(self, query_name: str, query_func, *args, iterations: int = 3) -> float:
        times = []
        
        # On exécute 3 fois pour avoir une moyenne
        for i in range(iterations):
            start = time.perf_counter()
            result = query_func(self.conn, *args)
            end = time.perf_counter()
            times.append((end - start) * 1000)  # En millisecondes
        
        avg_time = sum(times) / len(times)
        print(f"  {query_name}: {avg_time:.2f} ms")
        return avg_time

    # Récupérer la taille de la base de données
    def get_db_size(self) -> float:
        size_bytes = os.path.getsize(self.db_path)
        return size_bytes / (1024 * 1024)  # Convertir en MB

    # Afficher le plan d'exécution
    def explain_query_plan(self, query_name: str, sql: str, params: tuple = ()):
        cur = self.conn.cursor()
        cur.execute(f"EXPLAIN QUERY PLAN {sql}", params)
        plans = cur.fetchall()
        
        print(f"\nPlan d'exécution pour {query_name}:")
        for plan in plans:
            print(f"  {plan}")

    # Créer les index
    def create_indexes(self):
        indexes = [
            # Index pour chercher par nom
            ("idx_persons_name", "CREATE INDEX IF NOT EXISTS idx_persons_name ON persons(primaryName)"),
            
            # Index pour les clés étrangères (jointures)
            ("idx_principals_mid", "CREATE INDEX IF NOT EXISTS idx_principals_mid ON principals(mid)"),
            ("idx_principals_pid", "CREATE INDEX IF NOT EXISTS idx_principals_pid ON principals(pid)"),
            ("idx_directors_mid", "CREATE INDEX IF NOT EXISTS idx_directors_mid ON directors(mid)"),
            ("idx_directors_pid", "CREATE INDEX IF NOT EXISTS idx_directors_pid ON directors(pid)"),
            ("idx_genres_mid", "CREATE INDEX IF NOT EXISTS idx_genres_mid ON genres(mid)"),
            ("idx_genres_genre", "CREATE INDEX IF NOT EXISTS idx_genres_genre ON genres(genre)"),
            
            # Index pour les ratings
            ("idx_ratings_mid", "CREATE INDEX IF NOT EXISTS idx_ratings_mid ON ratings(mid)"),
            ("idx_ratings_numVotes", "CREATE INDEX IF NOT EXISTS idx_ratings_numVotes ON ratings(numVotes)"),
            
            # Index pour les characters et knownformovies
            ("idx_characters_mid", "CREATE INDEX IF NOT EXISTS idx_characters_mid ON characters(mid)"),
            ("idx_characters_pid", "CREATE INDEX IF NOT EXISTS idx_characters_pid ON characters(pid)"),
            ("idx_knownformovies_pid", "CREATE INDEX IF NOT EXISTS idx_knownformovies_pid ON knownformovies(pid)"),
            ("idx_knownformovies_mid", "CREATE INDEX IF NOT EXISTS idx_knownformovies_mid ON knownformovies(mid)"),
        ]
        
        print("\nCréation des index...")
        for index_name, sql in indexes:
            self.conn.execute(sql)
            print(f"  OK - {index_name}")
        
        self.conn.commit()

    # Supprimer les index
    def drop_indexes(self):
        cur = self.conn.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='index' AND name LIKE 'idx_%'")
        indexes = cur.fetchall()
        
        print("\nSuppression des index...")
        for (index_name,) in indexes:
            self.conn.execute(f"DROP INDEX IF EXISTS {index_name}")
            print(f"  OK - {index_name} supprimé")
        
        self.conn.commit()

    # Lancer le benchmark complet
    def run_benchmark(self):
        
        # SANS INDEX
        print("\n" + "="*70)
        print("BENCHMARK SANS INDEX")
        print("="*70)
        
        db_size_before = self.get_db_size()
        print(f"\nTaille de la base : {db_size_before:.2f} MB")
        
        results_without_index = {}
        
        print("\nExécution des requêtes...")
        
        results_without_index["Q1 - Filmographie"] = self.measure_query(
            "Q1 - Filmographie (Tom Hanks)",
            query_actor_filmography,
            "Tom Hanks"
        )
        
        results_without_index["Q2 - Top N films"] = self.measure_query(
            "Q2 - Top 10 films Adventure (1980-1990)",
            top_n_films,
            "Adventure", "1980", "1990", 10
        )
        
        results_without_index["Q3 - Acteurs multi-rôles"] = self.measure_query(
            "Q3 - Acteurs multi-rôles (Tom Hanks)",
            acteurs_multi_roles,
            "Tom Hanks"
        )
        
        results_without_index["Q4 - Collaborations"] = self.measure_query(
            "Q4 - Collaborations (Tom Hanks)",
            collaborations,
            "Tom Hanks"
        )
        
        results_without_index["Q5 - Genres populaires"] = self.measure_query(
            "Q5 - Genres populaires",
            genres_populaires
        )
        
        results_without_index["Q6 - Classement par genre"] = self.measure_query(
            "Q6 - Classement par genre",
            classement_par_genre
        )
        
        results_without_index["Q7 - Carrière propulsée"] = self.measure_query(
            "Q7 - Carrière propulsée",
            carriere_propulsee
        )
        
        results_without_index["Q8 - Films par réalisateur"] = self.measure_query(
            "Q8 - Films par réalisateur (Spielberg)",
            films_par_realisateur_et_genre,
            "Steven Spielberg"
        )
        
        # AVEC INDEX
        print("\n" + "="*70)
        print("BENCHMARK AVEC INDEX")
        print("="*70)
        
        self.create_indexes()
        
        db_size_after = self.get_db_size()
        print(f"\nTaille après index : {db_size_after:.2f} MB")
        print(f"Augmentation : +{(db_size_after - db_size_before):.2f} MB")
        
        results_with_index = {}
        
        print("\nExécution des requêtes...")
        
        results_with_index["Q1 - Filmographie"] = self.measure_query(
            "Q1 - Filmographie (Tom Hanks)",
            query_actor_filmography,
            "Tom Hanks"
        )
        
        results_with_index["Q2 - Top N films"] = self.measure_query(
            "Q2 - Top 10 films Adventure (1980-1990)",
            top_n_films,
            "Adventure", "1980", "1990", 10
        )
        
        results_with_index["Q3 - Acteurs multi-rôles"] = self.measure_query(
            "Q3 - Acteurs multi-rôles (Tom Hanks)",
            acteurs_multi_roles,
            "Tom Hanks"
        )
        
        results_with_index["Q4 - Collaborations"] = self.measure_query(
            "Q4 - Collaborations (Tom Hanks)",
            collaborations,
            "Tom Hanks"
        )
        
        results_with_index["Q5 - Genres populaires"] = self.measure_query(
            "Q5 - Genres populaires",
            genres_populaires
        )
        
        results_with_index["Q6 - Classement par genre"] = self.measure_query(
            "Q6 - Classement par genre",
            classement_par_genre
        )
        
        results_with_index["Q7 - Carrière propulsée"] = self.measure_query(
            "Q7 - Carrière propulsée",
            carriere_propulsee
        )
        
        results_with_index["Q8 - Films par réalisateur"] = self.measure_query(
            "Q8 - Films par réalisateur (Spielberg)",
            films_par_realisateur_et_genre,
            "Steven Spielberg"
        )
        
        # RÉSULTATS
        print("\n" + "="*70)
        print("RÉSULTATS FINAUX")
        print("="*70)
        
        print(f"\n{'Requête':<35} {'Sans (ms)':<12} {'Avec (ms)':<12} {'Gain (%)':<10}")
        print("-" * 70)
        
        total_before = 0
        total_after = 0
        
        for query_name in results_without_index.keys():
            before = results_without_index[query_name]
            after = results_with_index[query_name]
            gain = ((before - after) / before * 100) if before > 0 else 0
            
            total_before += before
            total_after += after
            
            print(f"{query_name:<35} {before:<12.2f} {after:<12.2f} {gain:<10.1f}")
        
        print("-" * 70)
        total_gain = ((total_before - total_after) / total_before * 100) if total_before > 0 else 0
        print(f"{'TOTAL':<35} {total_before:<12.2f} {total_after:<12.2f} {total_gain:<10.1f}")
        
        print(f"\nTemps économisé : {(total_before - total_after):.2f} ms")
        print(f"Taille DB : {db_size_before:.2f} MB → {db_size_after:.2f} MB")
        
        self.conn.close()

def __main__():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    db_path = os.path.join(script_dir, "..", "..", "data", "csv", "imdb.db")
    
    benchmark = BenchmarkDB(db_path)
    benchmark.run_benchmark()

if __name__ == "__main__":
    __main__()