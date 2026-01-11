# CineExplorer

Projet de Base de Données Avancées (BDA) - Semestre 7

## Description

CineExplorer est une application web de consultation de films basée sur les données IMDb. Le projet explore différentes technologies de bases de données : SQLite et MongoDB, avec une architecture en replica set.

## Structure du projet

```
cineexplorer_medium/
├── data/
│   ├── csv/          # Données CSV et base SQLite (imdb.db)
│   └── mongo/        # Données MongoDB (replica set)
├── django/           # Application web Django
│   ├── config/       # Configuration Django
│   ├── movies/       # App principale (views, services, templates)
│   └── static/       # CSS et JavaScript
├── scripts/
│   ├── phase1_sqlite/    # Scripts SQLite (schéma, import, requêtes)
│   ├── phase2_mongodb/   # Scripts MongoDB (migration, indexation)
│   └── phase3_replica/   # Scripts replica set
└── reports/          # Rapports des livrables
```

# Guide de Configuration CineExplorer

Ce document récapitule toutes les commandes nécessaires pour configurer et lancer le projet CineExplorer.

## 1. Clone du Projet

```bash
# Cloner le dépôt (si applicable)
git clone https://github.com/Tounos/CineExplorer_INFO-4A_BDDA
cd CineExplorer_INFO-4A_BDDA
```

## 2. Installation des Dépendances Python

```bash
# Installer les packages Python requis
pip install -r requirements.txt
```

Les dépendances principales :
- `django>=4.2`
- `pymongo>=4.0`

## 3. Configuration de la Base SQLite (Phase 1)

### 3.1 Création du Schéma SQLite

```bash
cd scripts/phase1_sqlite
python create_schema.py
```

Cette commande crée la base de données SQLite avec toutes les tables nécessaires.

### 3.2 Import des Données CSV

```bash
python import_data.py
```

Import des données depuis les fichiers CSV dans `data/csv/` vers SQLite.

### 3.3 Tests et Requêtes SQLite

```bash
python queries.py
python benchmark.py
```

## 4. Configuration de MongoDB (Phase 2)

### 4.1 Démarrage de MongoDB (instance standalone)

```bash
# Démarrer MongoDB via Homebrew (macOS)
brew services start mongodb-community

# OU démarrer manuellement
mongod --config /opt/homebrew/etc/mongod.conf
```

### 4.2 Migration des Données vers MongoDB

#### Structure Normalisée (Flat)

```bash
cd ../phase2_mongodb
python migrate_flat.py
```

#### Structure Dénormalisée (Structured)

```bash
python migrate_structured.py
```

### 4.3 Création des Index MongoDB

```bash
python indexation.py
```

**Important** : Créer les index AVANT d'exécuter les migrations pour améliorer les performances (10-100x plus rapide).

### 4.4 Tests de Performance

```bash
python queries_mongo.py
python compare_performance.py
```

## 5. Configuration du Replica Set MongoDB (Phase 3)

### 5.1 Arrêt de l'instance MongoDB Standalone

```bash
# Arrêter MongoDB lancé via Homebrew
brew services stop mongodb-community
```

### 5.2 Création des Répertoires pour le Replica Set

```bash
cd ../phase3_replica

# Les répertoires seront créés automatiquement par le script
# Mais vous pouvez les créer manuellement :
mkdir -p ../../data/mongo/db-1
mkdir -p ../../data/mongo/db-2
mkdir -p ../../data/mongo/db-3
mkdir -p ../../data/mongo/standalone
mkdir -p ../../data/mongo/logs
```

### 5.3 Configuration des Permissions du Script

```bash
chmod +x setup_replica.sh
```

### 5.4 Lancement du Replica Set

```bash
./setup_replica.sh
```

Ce script effectue automatiquement :
- Création des répertoires nécessaires
- Arrêt des instances MongoDB existantes
- Démarrage de 3 instances MongoDB (ports 27017, 27018, 27019)
- Démarrage d'une instance standalone (port 27020) pour la source des données
- Initialisation du replica set `rs0`
- Backup de la base standalone
- Restauration des données sur le replica set

### 5.5 Vérification du Replica Set

```bash
# Vérifier le statut du replica set
mongosh --port 27017 --eval "rs.status()"

# Voir quel nœud est PRIMARY
mongosh --port 27017 --quiet --eval "rs.status().members.forEach(m => print(m.name + ' : ' + m.stateStr))"

# Vérifier les données
mongosh --port 27019 --eval "use cineexplorer; db.movies.countDocuments({})"
```

### 5.6 Architecture du Replica Set

- **bd-1** (port 27017) : SECONDARY
- **bd-2** (port 27018) : SECONDARY
- **bd-3** (port 27019) : PRIMARY
- **standalone** (port 27020) : Source des données (peut être arrêté après l'import)

## 6. Lancement de l'Application Django

### 6.1 Navigation vers le Répertoire Django

```bash
cd ../../django
```

### 6.2 Configuration Django

Le fichier `config/settings.py` est déjà configuré pour utiliser le replica set :

```python
MONGODB_URI = "mongodb://localhost:27017,localhost:27018,localhost:27019/?replicaSet=rs0"
MONGODB_NAME = "cineexplorer"
```

### 6.3 Application des Migrations (Optionnel)

```bash
# Les migrations Django ne sont pas critiques pour MongoDB
# mais vous pouvez les appliquer si nécessaire
python manage.py migrate
```

### 6.4 Lancement du Serveur

```bash
python manage.py runserver
```

Le serveur démarre sur **http://127.0.0.1:8000/**

## 7. Commandes Utiles

### Gestion du Replica Set

```bash
# Arrêter toutes les instances MongoDB
pkill -f "mongod.*270"

# Redémarrer le replica set
cd scripts/phase3_replica
./setup_replica.sh

# Vérifier les processus MongoDB actifs
ps aux | grep mongod | grep -v grep
```

### Gestion du Serveur Django

```bash
# Arrêter le serveur Django
# Dans le terminal où le serveur tourne : Ctrl+C

# Lancer le serveur en arrière-plan
nohup python manage.py runserver &
```

### Tests de Failover

```bash
cd scripts/phase3_replica
python test_failover.py
```

## 8. Résolution de Problèmes Courants

### "Permission denied" sur les scripts bash

```bash
chmod +x setup_replica.sh
```

### "Address already in use" (port 27017)

```bash
# Arrêter MongoDB existant
brew services stop mongodb-community
pkill -f "mongod.*27017"
```

### "Connection refused" sur MongoDB

```bash
# Vérifier que MongoDB est démarré
ps aux | grep mongod

# Vérifier les logs
tail -f ../../data/mongo/logs/db-1.log
```

### "OutOfDiskSpace" lors de l'import

```bash
# Libérer de l'espace
rm -rf /tmp/mongodb_backup

# Nettoyer les fichiers temporaires
rm -rf ~/Library/Caches/*
```

### Barre de progression qui s'empile (migrate_structured.py)

Utiliser un terminal zsh normal plutôt que le terminal Python de VS Code :

```bash
cd scripts/phase2_mongodb
python migrate_structured.py
```

## 9. Structure des Données

### Collections MongoDB

- `movies` : Informations principales sur les films
- `genres` : Genres des films
- `ratings` : Notes et votes
- `directors` : Réalisateurs
- `writers` : Scénaristes
- `principals` : Acteurs principaux
- `characters` : Personnages
- `persons` : Informations sur les personnes (acteurs, réalisateurs, etc.)
- `titles` : Titres alternatifs par région
- `professions` : Professions des personnes
- `knownformovies` : Films pour lesquels les personnes sont connues
- `movies_complete` : Collection dénormalisée avec toutes les données

### Volumes de Données

- **Films** : 291,238 documents
- **Personnes** : 632,324 documents
- **Principals** : 2,745,688 documents
- **Titres** : 1,908,072 documents
- **Caractères** : 1,405,158 documents

## 10. Ports Utilisés

| Service | Port | Description |
|---------|------|-------------|
| MongoDB bd-1 | 27017 | Replica Set - SECONDARY |
| MongoDB bd-2 | 27018 | Replica Set - SECONDARY |
| MongoDB bd-3 | 27019 | Replica Set - PRIMARY |
| MongoDB standalone | 27020 | Source des données (optionnel après import) |
| Django | 8000 | Serveur web de développement |

## 11. Commandes de Vérification Rapide

```bash
# Vérifier que tout fonctionne
mongosh --port 27019 --quiet --eval "rs.isMaster().ismaster"  # Doit retourner "true"
curl http://127.0.0.1:8000/  # Doit retourner la page d'accueil Django
```

---

**Projet CineExplorer** - Base de Données Distribuées - INFO 4A


## Note

Pour la partie Django (templates, vues, services), je me suis aidé de l'IA Claude Sonnet pour les parties que je ne comprenais pas, notamment pour la gestion des templates.

## Auteur

Antoine CHIAUSA - INFO 4A FISA - 2025/2026
