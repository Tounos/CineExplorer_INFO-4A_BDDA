#!/bin/bash

# Configuration du Replica Set MongoDB
# bd-1 : port 27017 (PRIMAIRE)
# bd-2 : port 27018 (SECONDAIRE)
# bd-3 : port 27019 (SECONDAIRE)
# standalone : port 27020 (source de données)

set -e

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
sleep 3

echo "=== Vérification du statut ==="
mongosh --port 27017 <<EOF
rs.status()
EOF

echo "=== Backup de la base standalone (port 27020) ==="
mongodump --port=27020 --out=/tmp/mongodb_backup

echo "Backup créé dans /tmp/mongodb_backup"

echo "=== Import sur le PRIMARY (bd-1 : port 27017) ==="
mongorestore --port=27017 /tmp/mongodb_backup

echo "Données importées"
sleep 2

echo "=== Vérification des données ==="
mongosh --port 27017 <<EOF
show dbs
EOF

echo "=== Configuration du Replica Set terminée ==="
