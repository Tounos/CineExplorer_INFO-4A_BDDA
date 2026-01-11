#!/bin/bash

# Configuration du Replica Set MongoDB
# bd-1 : port 27017 (PRIMAIRE)
# bd-2 : port 27018 (SECONDAIRE)
# bd-3 : port 27019 (SECONDAIRE)
# standalone : port 27020 (source de données)

set -e

# Obtenir le chemin absolu du répertoire du projet
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"

echo "=== Création des répertoires ==="
mkdir -p "$BASE_DIR/data/mongo/db-1"
mkdir -p "$BASE_DIR/data/mongo/db-2"
mkdir -p "$BASE_DIR/data/mongo/db-3"
mkdir -p "$BASE_DIR/data/mongo/standalone"
mkdir -p "$BASE_DIR/data/mongo/logs"
echo "Répertoires db-1, db-2, db-3 et standalone créés"

echo ""
echo "=== Arrêt des instances existantes ==="
pkill -f "mongod.*27017" 2>/dev/null && echo "Instance 27017 arrêtée" || echo "Aucune instance sur 27017"
pkill -f "mongod.*27018" 2>/dev/null && echo "Instance 27018 arrêtée" || echo "Aucune instance sur 27018"
pkill -f "mongod.*27019" 2>/dev/null && echo "Instance 27019 arrêtée" || echo "Aucune instance sur 27019"
pkill -f "mongod.*27020" 2>/dev/null && echo "Instance 27020 arrêtée" || echo "Aucune instance sur 27020"
sleep 2

echo ""
echo "=== Démarrage des instances MongoDB ==="

# bd-1 (port 27017)
echo "Démarrage de bd-1 (port 27017)..."
mongod --replSet rs0 --port 27017 --dbpath "$BASE_DIR/data/mongo/db-1" --bind_ip localhost --logpath "$BASE_DIR/data/mongo/logs/db-1.log" > /dev/null 2>&1 &

# bd-2 (port 27018)
echo "Démarrage de bd-2 (port 27018)..."
mongod --replSet rs0 --port 27018 --dbpath "$BASE_DIR/data/mongo/db-2" --bind_ip localhost --logpath "$BASE_DIR/data/mongo/logs/db-2.log" > /dev/null 2>&1 &

# bd-3 (port 27019)
echo "Démarrage de bd-3 (port 27019)..."
mongod --replSet rs0 --port 27019 --dbpath "$BASE_DIR/data/mongo/db-3" --bind_ip localhost --logpath "$BASE_DIR/data/mongo/logs/db-3.log" > /dev/null 2>&1 &

# standalone (port 27020) - source des données
echo "Démarrage de l'instance standalone (port 27020)..."
mongod --port 27020 --dbpath "$BASE_DIR/data/mongo/standalone" --bind_ip localhost --logpath "$BASE_DIR/data/mongo/logs/standalone.log" > /dev/null 2>&1 &

echo "✓ Toutes les instances sont démarrées"
echo "Attente du démarrage complet..."
sleep 5

echo ""
echo "=== Initialisation du Replica Set ==="

# Initialiser le replica set
mongosh --port 27017 <<EOF
rs.initiate({
  _id: "rs0",
  members: [
    { _id: 0, host: "localhost:27017" },
    { _id: 1, host: "localhost:27018" },
    { _id: 2, host: "localhost:27019" }
  ]
})
EOF

echo "✓ Replica Set initialisé"
echo "Attente de l'élection du PRIMARY (peut prendre 10-15 secondes)..."
sleep 10

echo ""
echo "=== Vérification du statut ==="
mongosh --port 27017 <<EOF
rs.status()
EOF

echo "=== Backup de la base standalone (port 27020) ==="
mongodump --port=27020 --out=/tmp/mongodb_backup

echo "Backup créé dans /tmp/mongodb_backup"

echo ""
echo "=== Attente que le PRIMARY soit prêt pour l'import ==="
sleep 5

echo "=== Import sur le PRIMARY (bd-1 : port 27017) ==="
# Se connecter au replica set, pas à un port spécifique
mongorestore --uri="mongodb://localhost:27017,localhost:27018,localhost:27019/?replicaSet=rs0" /tmp/mongodb_backup

echo "Données importées"
sleep 2

echo "=== Vérification des données ==="
mongosh --port 27017 <<EOF
show dbs
EOF

echo "=== Configuration du Replica Set terminée ==="
