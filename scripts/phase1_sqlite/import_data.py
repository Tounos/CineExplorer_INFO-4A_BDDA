#T1.2 : Import des données

import pandas as pd
import sqlite3
import os
from datetime import datetime

print("Import des données dans la base de données SQLite")
temps = datetime.now()

script_dir = os.path.dirname(os.path.abspath(__file__))
chemin_db = os.path.join(script_dir, "..", "..", "data", "csv", "imdb.db")
chemin_csv = os.path.join(script_dir, "..", "..", "data", "csv") + os.sep

chemin_db = os.path.normpath(chemin_db)
chemin_csv = os.path.normpath(chemin_csv) + os.sep

stats = {"succes": {}, "erreurs": 0, "filtrees": {}}

try:
    conn = sqlite3.connect(chemin_db)
    conn.execute("PRAGMA foreign_keys = ON")
    cur = conn.cursor()

    # Chargement des CSV
    movies_df = pd.read_csv(chemin_csv + "movies.csv", low_memory=False)
    persons_df = pd.read_csv(chemin_csv + "persons.csv", low_memory=False)
    characters_df = pd.read_csv(chemin_csv + "characters.csv", low_memory=False)
    directors_df = pd.read_csv(chemin_csv + "directors.csv", low_memory=False)
    genres_df = pd.read_csv(chemin_csv + "genres.csv", low_memory=False)
    knownformovies_df = pd.read_csv(chemin_csv + "knownformovies.csv", low_memory=False)
    principals_df = pd.read_csv(chemin_csv + "principals.csv", low_memory=False)
    professions_df = pd.read_csv(chemin_csv + "professions.csv", low_memory=False)
    ratings_df = pd.read_csv(chemin_csv + "ratings.csv", low_memory=False)
    titles_df = pd.read_csv(chemin_csv + "titles.csv", low_memory=False)
    writers_df = pd.read_csv(chemin_csv + "writers.csv", low_memory=False)

    # Renommer les colonnes
    movies_df.columns = ['mid', 'titleType', 'primaryTitle', 'originalTitle', 
                         'isAdult', 'startYear', 'endYear', 'runtimeMinutes']
    persons_df.columns = ['pid', 'primaryName', 'birthYear', 'deathYear']
    genres_df.columns = ['mid', 'genre']
    ratings_df.columns = ['mid', 'averageRating', 'numVotes']
    titles_df.columns = ['mid', 'ordering', 'title', 'region', 'language', 
                         'types', 'attributes', 'isOriginalTitle']
    professions_df.columns = ['pid', 'jobName']
    directors_df.columns = ['mid', 'pid']
    writers_df.columns = ['mid', 'pid']
    characters_df.columns = ['mid', 'pid', 'name']
    principals_df.columns = ['mid', 'ordering', 'pid', 'category', 'job']
    knownformovies_df.columns = ['pid', 'mid']

    # Sets des clés primaires valides
    valid_mids = set(movies_df['mid'].dropna().unique())
    valid_pids = set(persons_df['pid'].dropna().unique())

    def filter_fk(df, table_name, mid_col=None, pid_col=None):
        """Filtre les lignes avec clés étrangères invalides"""
        original_len = len(df)
        if mid_col and mid_col in df.columns:
            df = df[df[mid_col].isin(valid_mids)]
        if pid_col and pid_col in df.columns:
            df = df[df[pid_col].isin(valid_pids)]
        filtered = original_len - len(df)
        if filtered > 0:
            stats["filtrees"][table_name] = filtered
        return df

    # Filtrer les tables avec FK
    directors_df = filter_fk(directors_df, "directors", mid_col='mid', pid_col='pid')
    writers_df = filter_fk(writers_df, "writers", mid_col='mid', pid_col='pid')
    characters_df = filter_fk(characters_df, "characters", mid_col='mid', pid_col='pid')
    principals_df = filter_fk(principals_df, "principals", mid_col='mid', pid_col='pid')
    knownformovies_df = filter_fk(knownformovies_df, "knownformovies", mid_col='mid', pid_col='pid')
    genres_df = filter_fk(genres_df, "genres", mid_col='mid')
    ratings_df = filter_fk(ratings_df, "ratings", mid_col='mid')
    titles_df = filter_fk(titles_df, "titles", mid_col='mid')
    professions_df = filter_fk(professions_df, "professions", pid_col='pid')

    # Ordre d'insertion (tables principales d'abord)
    tables_df = [
        ("movies", movies_df),
        ("persons", persons_df),
        ("genres", genres_df),
        ("ratings", ratings_df),
        ("titles", titles_df),
        ("professions", professions_df),
        ("directors", directors_df),
        ("writers", writers_df),
        ("characters", characters_df),
        ("principals", principals_df),
        ("knownformovies", knownformovies_df),
    ]

    for table, df in tables_df:
        try:
            df = df.where(pd.notnull(df), None)
            rows_avant = len(df)
            df = df.drop_duplicates()
            duplicatas = rows_avant - len(df)

            df.to_sql(table, conn, if_exists="append", index=False)

            print(f"Insertion réussie dans la table {table} : {len(df)} lignes insérées.")
            stats["succes"][table] = {
                "lignes_inserees": len(df),
                "duplicatas": duplicatas
            }

        except sqlite3.Error as e:
            print(f"Erreur lors de l'insertion dans la table {table} : {e}")
            stats["erreurs"] += 1

    conn.commit()

    # Stats
    temps = (datetime.now() - temps).total_seconds()
    total_rows = sum(t["lignes_inserees"] for t in stats["succes"].values())
    total_duplicatas = sum(t["duplicatas"] for t in stats["succes"].values())
    total_filtrees = sum(stats["filtrees"].values())

    print("\nStatistiques d'importation :")
    print(f"Tables importées    : {len(stats['succes'])}")
    print(f"Total lignes        : {total_rows:,}")
    print(f"Doublons supprimés  : {total_duplicatas:,}")
    print(f"Lignes filtrées (FK): {total_filtrees:,}")
    print(f"Erreurs             : {stats['erreurs']}")
    print(f"Temps d'exécution   : {temps:.2f}s")

    if stats["filtrees"]:
        print("\nLignes filtrées par table (clés étrangères invalides) :")
        for table, count in stats["filtrees"].items():
            print(f"  {table:<15} : {count:>6} lignes")

except sqlite3.Error as e:
    print(f"Erreur lors de l'import des données : {e}")
    conn.rollback()
finally:
    conn.close()
