import pymongo as mongo
import sqlite3
import os
from datetime import datetime

def migrate_flat():
    try: 
        uri = "mongodb://localhost:27017"
        client = mongo.MongoClient(uri)
        client.admin.command('ping')
        print("Connexion à MongoDB réussie")

        db = client["cineexplorer"]

        script_dir = os.path.dirname(os.path.abspath(__file__))
        chemin_db_sql = os.path.join(script_dir, "..", "..", "data", "csv", "imdb.db")
        if not os.path.exists(os.path.dirname(chemin_db_sql)):
            os.makedirs(os.path.dirname(chemin_db_sql))

        conn = sqlite3.connect(chemin_db_sql)
        conn.row_factory = sqlite3.Row #on formate les lignes en objects dict-like
        cur = conn.cursor()

        #créer une collection MongoDB pour chaque table SQLite
        cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cur.fetchall()]

        print("Tables SQLite trouvées : ", tables)

        temps = datetime.now()

        for nom_table in tables:
            print("Migration de la table : ", nom_table)
            cur.execute(f"SELECT * FROM {nom_table}")
            lignes = [dict(row) for row in cur.fetchall()] #on transforme chaque ligne en dict
            
            if not lignes:
                print(f"Aucune donnée à migrer pour la table {nom_table}")
                continue #sauter les tables vides

            #supprimer la collection si elle existe déjà
            if nom_table in db.list_collection_names():
                    db[nom_table].drop()
            
            #insertion des données dans MongoDB
            insertion = db[nom_table].insert_many(lignes)
            print(f"{len(insertion.inserted_ids)} documents insérés dans la collection {nom_table}\n")

        duree = (datetime.now() - temps).total_seconds()
        print(f"Migration terminée en {duree} secondes")


        #vérifier les comptages
        print("Vérification des comptages :")
        for nom_table in tables:
            nombre = db[nom_table].count_documents({})
            print(f"Collection {nom_table} : {nombre} documents")

    except mongo.errors.ConnectionFailure as e:
        print(f"Erreur de connexion à MongoDB : {e}")
    except Exception as e:
        print(f"Erreur : {e}")
    finally:
        client.close()

if __name__ == "__main__":
    migrate_flat()