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

## Installation

### Prérequis

- Python 3.10+
- MongoDB 6.0+

### Installation des dépendances

```bash
pip install -r requirements.txt
```

### Configuration MongoDB (Replica Set)

Le projet utilise un replica set MongoDB avec 3 nœuds :

```bash
# Démarrer le replica set
cd scripts/phase3_replica
./setup_replica.sh
```

Les nœuds sont sur les ports 27017, 27018 et 27019.

### Lancer l'application Django

```bash
cd django
python manage.py runserver
```

L'application est accessible sur http://localhost:8000

## Fonctionnalités

- **Accueil** : Statistiques générales (nombre de films, acteurs, etc.)
- **Films** : Liste paginée avec filtres par genre et année
- **Détail film** : Informations complètes, casting, notes
- **Recherche** : Recherche de films et personnes
- **Statistiques** : Graphiques (genres, décennies, notes, acteurs populaires)
- **Détail personne** : Filmographie complète

## Architecture technique

### Stratégie multi-bases

- **SQLite** : Utilisé pour les listes et la recherche (plus rapide pour les jointures)
- **MongoDB** : Utilisé pour les détails des films (document complet avec données imbriquées)

### Technologies utilisées

- Django 4.2+
- Bootstrap 5
- Chart.js (graphiques)
- PyMongo (driver MongoDB)

## Phases du projet

1. **Phase 1** : Import des données CSV dans SQLite, création du schéma relationnel
2. **Phase 2** : Migration vers MongoDB (structure plate et structurée), indexation
3. **Phase 3** : Mise en place du replica set, tests de failover
4. **Phase 4** : Application web Django

## Note

Pour la partie Django (templates, vues, services), je me suis aidé de l'IA Claude Sonnet pour les parties que je ne comprenais pas, notamment pour la gestion des templates.

## Auteur

Antoine CHIAUSA - INFO 4A FISA - 2025/2026
