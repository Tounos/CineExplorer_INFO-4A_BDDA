# T1.1 #T1.1

#1

import pandas as pd
import sqlite3
import os

print("Création des tables dans la base de données SQLite")

chemin_db = "../../data/csv/imdb.db"
if not os.path.exists(os.path.dirname(chemin_db)):
    os.makedirs(os.path.dirname(chemin_db))

conn = sqlite3.connect(chemin_db)
cur = conn.cursor()


#Suppression des tables si elles existent déjà

tables_a_supprimer = [
    "ratings", "titles", "movie_writers", "movie_directors", 
    "character_actors", "movie_genres", "writers", 
    "directors", "genres", "characters", "persons", 
    "professions", "principals", "knownformovies", "movies"
]

for table in tables_a_supprimer:
    cur.execute(f"DROP TABLE IF EXISTS {table}")
conn.commit()
print("Tables supprimées")



#Création des tables (3NF)
cur.execute("""
    CREATE TABLE IF NOT EXISTS movies(
        mid TEXT PRIMARY KEY,
        titleType TEXT,
        primaryTitle TEXT,
        originalTitle TEXT,
        isAdult BOOLEAN,
        startYear INTEGER,
        endYear INTEGER,
        runtimeMinutes INTEGER
    )
"""
)

cur.execute("""
    CREATE TABLE IF NOT EXISTS persons(
        pid TEXT PRIMARY KEY,
        primaryName TEXT,
        birthYear INTEGER,
        deathYear INTEGER
    )
"""
)

cur.execute("""
    CREATE TABLE IF NOT EXISTS characters(
        mid TEXT,
        pid TEXT,
        name TEXT,
        PRIMARY KEY (mid, pid, name),
        FOREIGN KEY (mid) REFERENCES movies(mid) ON DELETE CASCADE, 
        FOREIGN KEY (pid) REFERENCES persons(pid) ON DELETE CASCADE
    )
"""
)

cur.execute("""
    CREATE TABLE IF NOT EXISTS directors(
        mid TEXT,
        pid TEXT,
        PRIMARY KEY (mid, pid),
        FOREIGN KEY (mid) REFERENCES movies(mid) ON DELETE CASCADE,
        FOREIGN KEY (pid) REFERENCES persons(pid) ON DELETE CASCADE
    )    
"""
)

cur.execute(
"""
    CREATE TABLE IF NOT EXISTS genres(
        mid TEXT,
        genre TEXT,
        PRIMARY KEY (mid, genre),
        FOREIGN KEY (mid) REFERENCES movies(mid) ON DELETE CASCADE
    )
"""
)

cur.execute(
"""
    CREATE TABLE IF NOT EXISTS knownformovies(
        pid TEXT,
        mid TEXT,
        PRIMARY KEY (pid, mid),
        FOREIGN KEY (pid) REFERENCES persons(pid) ON DELETE CASCADE,
        FOREIGN KEY (mid) REFERENCES movies(mid) ON DELETE CASCADE
    )"""
)

cur.execute(
"""
    CREATE TABLE IF NOT EXISTS principals(
        mid TEXT,
        ordering INTEGER,
        pid TEXT,
        category TEXT,
        job TEXT,
        characters TEXT,
        PRIMARY KEY (mid, ordering, pid, category),
        FOREIGN KEY (mid) REFERENCES movies(mid) ON DELETE CASCADE,
        FOREIGN KEY (pid) REFERENCES persons(pid) ON DELETE CASCADE
    )    
"""
)

cur.execute(
"""
    CREATE TABLE IF NOT EXISTS professions(
        pid TEXT,
        jobName TEXT,
        PRIMARY KEY (pid, jobName),
        FOREIGN KEY (pid) REFERENCES persons(pid) ON DELETE CASCADE
    )
"""
)

cur.execute(
"""
    CREATE TABLE IF NOT EXISTS ratings(
        mid TEXT PRIMARY KEY,
        averageRating REAL,
        numVotes INTEGER,
        FOREIGN KEY (mid) REFERENCES movies(mid) ON DELETE CASCADE
    )
"""
)

cur.execute(
"""
    CREATE TABLE IF NOT EXISTS titles(
        mid TEXT,
        ordering INTEGER,
        title TEXT,
        region TEXT,
        language TEXT,
        types TEXT,
        attributes TEXT,
        isOriginalTitle BOOLEAN,
        PRIMARY KEY (mid, ordering),
        FOREIGN KEY (mid) REFERENCES movies(mid) ON DELETE CASCADE
    )
"""
)

cur.execute(
"""
    CREATE TABLE IF NOT EXISTS writers(
        mid TEXT,
        pid TEXT,
        PRIMARY KEY (mid, pid),
        FOREIGN KEY (mid) REFERENCES movies(mid) ON DELETE CASCADE,
        FOREIGN KEY (pid) REFERENCES persons(pid) ON DELETE CASCADE
    )    
"""
)

conn.commit()
print("Tables créées")


#Affichage du digramme E/R
print("DIAGRAMME ENTITÉ-RELATION (ER)")

cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
tables = cur.fetchall()

# Afficher chaque table avec ses colonnes
print("\nENTITÉS (Tables) :\n")

for table in tables:
    table_name = table[0]
    cur.execute(f"PRAGMA table_info({table_name})")
    columns = cur.fetchall()
    
    print(f"┌{'─' * 50}┐")
    print(f"│ {table_name.upper():^48} │")
    print(f"├{'─' * 50}┤")
    
    for col in columns:
        col_name, col_type, _, _, pk = col[1], col[2], col[3], col[4], col[5]
        pk_str = " 🔑 PK" if pk else ""
        line = f"  {col_name} ({col_type}) {pk_str}"
        print(f"│ {line:<48} │")
    
    print(f"└{'─' * 50}┘\n")

# Afficher les relations (clés étrangères)
print("\nRELATIONS (Clés étrangères) ──────► (Clé principale) :\n")

for table in tables:
    table_name = table[0]
    cur.execute(f"PRAGMA foreign_key_list({table_name})")
    fks = cur.fetchall()
    
    for fk in fks:
        ref_table = fk[2]
        from_col = fk[3]
        to_col = fk[4]
        print(f"  {table_name}.{from_col} ──────► {ref_table}.{to_col}")

conn.close()