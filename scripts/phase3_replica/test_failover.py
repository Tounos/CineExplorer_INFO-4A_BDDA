import time
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError

# Connexion au Replica Set avec timeout plus court
client = MongoClient(
    "mongodb://localhost:27017,localhost:27018,localhost:27019/?replicaSet=rs0",
    serverSelectionTimeoutMS=5000
)

print("Test de failover")
print("=" * 50)

# Identifier le primaire actuel
print("\n1. Identification du primaire...")
try:
    primary_info = client.admin.command('isMaster')
    if 'primary' not in primary_info:
        print("   ERREUR: Pas de primaire trouvé. Le replica set est-il initialisé?")
        exit(1)
    primary_host = primary_info['primary']
    primary_port = int(primary_host.split(':')[1])
    print(f"   Primaire actuel : {primary_host}")
except Exception as e:
    print(f"   ERREUR: {e}")
    exit(1)

# Arrêter le primaire
print(f"\n2. Arrêt du primaire (port {primary_port})...")
try:
    primary_client = MongoClient(f"mongodb://localhost:{primary_port}/", serverSelectionTimeoutMS=2000)
    primary_client.admin.command('shutdown', force=True)
except (ConnectionFailure, ServerSelectionTimeoutError):
    print(f"   Primaire arrêté (connexion fermée)")
except Exception as e:
    print(f"   Primaire arrêté ou erreur: {type(e).__name__}")

# Mesurer le temps d'élection
print("\n3. Mesure du temps d'élection...")
start_time = time.time()
timeout = 30  # timeout de 30 secondes
new_primary = None

while time.time() - start_time < timeout:
    try:
        # Créer une nouvelle connexion pour éviter les caches
        test_client = MongoClient(
            "mongodb://localhost:27017,localhost:27018,localhost:27019/?replicaSet=rs0",
            serverSelectionTimeoutMS=2000
        )
        status = test_client.admin.command('isMaster')
        test_client.close()
        
        if 'primary' in status and status['primary'] != primary_host:
            elapsed = time.time() - start_time
            new_primary = status['primary']
            print(f"\n   Nouveau primaire élu : {new_primary}")
            print(f"   Temps d'élection : {elapsed:.2f} secondes")
            break
    except Exception:
        pass
    
    time.sleep(0.5)
    print(".", end="", flush=True)

if new_primary is None:
    print(f"\n   ERREUR: Pas de nouveau primaire après {timeout} secondes")
    print("   Vérifiez que les autres membres du replica set sont actifs")

print("\n" + "=" * 50)
print("N'oubliez pas de redémarrer le membre arrêté!")
print(f"   mongod --replSet rs0 --port {primary_port} --dbpath <chemin>")
