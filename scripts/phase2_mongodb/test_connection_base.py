import pymongo as mongo

try:
    
    client = mongo.MongoClient("mongodb://localhost:27017")

    client.admin.command('ping')
    print("Connexion à MongoDB réussie")

    db = client["ma_base_de_donnees"]
    print(f"Bases disponibles : {client.list_database_names()}")

except mongo.errors.ConnectionFailure as e:
    print(f"Erreur de connexion à MongoDB : {e}")
except Exception as e:
    print(f"Erreur : {e}")
finally:
    client.close()