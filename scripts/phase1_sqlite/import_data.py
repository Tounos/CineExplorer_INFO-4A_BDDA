#T1.2 : Import des données

import pandas as pd
import sqlite3
import os
from datetime import datetime

print("Import des données dans la base de données SQLite")
temps = datetime.now() #pour les stats

script_dir = os.path.dirname(os.path.abspath(__file__))
chemin_db = os.path.join(script_dir, "..", "..", "data", "csv", "imdb.db")
chemin_csv = os.path.join(script_dir, "..", "..", "data", "csv") + os.sep

# Normaliser les chemins
chemin_db = os.path.normpath(chemin_db)
chemin_csv = os.path.normpath(chemin_csv) + os.sep

stats = {"succes": {}, "erreurs": 0}

try:
    conn = sqlite3.connect(chemin_db)
    conn.execute("PRAGMA foreign_keys = ON")
    cur = conn.cursor()

    #Créations des dataframes pandas à partir des fichiers CSV
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

    #Dictionnaire avec les noms de colonnes pour chaque table
    tables_df = {
        "movies": (movies_df, ['mid', 'titleType', 'primaryTitle', 'originalTitle', 
                               'isAdult', 'startYear', 'endYear', 'runtimeMinutes']),
        "persons": (persons_df, ['pid', 'primaryName', 'birthYear', 'deathYear']),
        "genres": (genres_df, ['mid', 'genre']),
        "ratings": (ratings_df, ['mid', 'averageRating', 'numVotes']),
        "titles": (titles_df, ['mid', 'ordering', 'title', 'region', 'language', 
                               'types', 'attributes', 'isOriginalTitle']),
        "professions": (professions_df, ['pid', 'jobName']),
        "directors": (directors_df, ['mid', 'pid']),
        "writers": (writers_df, ['mid', 'pid']),
        "characters": (characters_df, ['mid', 'pid', 'name']),
        "principals": (principals_df, ['mid', 'ordering', 'pid', 'category', 'job']),
        "knownformovies": (knownformovies_df, ['pid', 'mid']),
    }

    for table, (df, columns) in tables_df.items():

        try:
            #Renommer les colonnes pour correspondre au schéma de la base de données
            df.columns = columns
            
            #Naan en None pour les valeurs manquantes
            df = df.where(pd.notnull(df), None)

            #supprimer les doublons si nécessaire
            rows_avant = len(df)
            df = df.drop_duplicates()
            duplicatas = rows_avant - len(df)

            #Insertion des données dans la table correspondante
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

    #Stats
    temps = (datetime.now() - temps).total_seconds()
    total_rows = sum(t["lignes_inserees"] for t in stats["succes"].values())
    total_duplicatas = sum(t["duplicatas"] for t in stats["succes"].values())
    print("\nStatistiques d'importation :")
    print(f"Tables importées    : {len(stats['succes'])}")
    print(f"Total lignes        : {total_rows:,}")
    print(f"Doublons supprimés  : {total_duplicatas:,}")
    print(f"Erreurs             : {stats['erreurs']}")
    print(f"Temps d'exécution   : {temps:.2f}s")

    print("\nDétail par table :")
    for table, info in stats["succes"].items():
        dup_str = f" ({info["duplicatas"]} doublons)" if info["duplicatas"] > 0 else ""
        print(f"  {table:<15} : {info["lignes_inserees"]:>6} lignes{dup_str}")

except sqlite3.Error as e:
    print(f"Erreur lors de l'import des données : {e}")
    conn.rollback()
finally:
    conn.close()
