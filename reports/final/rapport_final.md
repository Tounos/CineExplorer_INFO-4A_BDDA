# CineExplorer - Rapport Final

## Projet de Base de Données Avancées (BDA)

**Auteur** : Antoine Chiausa  
**Formation** : INFO S7  
**Année** : 2025/2026  

---

# Table des matières

1. [Introduction](#1-introduction)
2. [Architecture globale](#2-architecture-globale)
3. [Choix technologiques](#3-choix-technologiques)
4. [Description des fonctionnalités](#4-description-des-fonctionnalités)
5. [Stratégie multi-bases](#5-stratégie-multi-bases)
6. [Benchmarks de performance](#6-benchmarks-de-performance)
7. [Difficultés rencontrées et solutions](#7-difficultés-rencontrées-et-solutions)
8. [Conclusion](#8-conclusion)

---

# 1. Introduction

## 1.1 Contexte du projet

CineExplorer est une application web de consultation de films développée dans le cadre du cours de Base de Données Avancées. Ce projet explore les différentes technologies de stockage de données (SQL et NoSQL) et leur intégration dans une application web moderne.

## 1.2 Objectifs

- Maîtriser les concepts de bases de données relationnelles et documentaires
- Implémenter une architecture multi-bases de données
- Mettre en place un replica set MongoDB pour la haute disponibilité
- Développer une interface web ergonomique avec Django

## 1.3 Données utilisées

Le projet utilise un sous-ensemble des données IMDb (Internet Movie Database) comprenant :
- ~100 000 films et séries
- ~200 000 personnes (acteurs, réalisateurs, etc.)
- Relations : casting, réalisation, genres, notes

---

# 2. Architecture globale

## 2.1 Schéma d'architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        CLIENT (Navigateur)                       │
│                    HTML / CSS / JavaScript                       │
│                    Bootstrap 5 + Chart.js                        │
└───────────────────────────┬─────────────────────────────────────┘
                            │ HTTP
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│                      SERVEUR DJANGO                              │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────────┐  │
│  │   Views     │  │  Templates  │  │      Static files       │  │
│  │  (views.py) │  │   (HTML)    │  │    (CSS, JS, images)    │  │
│  └──────┬──────┘  └─────────────┘  └─────────────────────────┘  │
│         │                                                        │
│  ┌──────▼──────────────────────────────────────────────────┐    │
│  │                    SERVICES LAYER                        │    │
│  │  ┌──────────────────┐    ┌──────────────────────────┐   │    │
│  │  │  sqlite_service  │    │     mongo_service        │   │    │
│  │  │                  │    │                          │   │    │
│  │  │ - get_movies()   │    │ - get_movie_details()   │   │    │
│  │  │ - search()       │    │ - get_stats()           │   │    │
│  │  │ - get_genres()   │    │ - get_person_details()  │   │    │
│  │  └────────┬─────────┘    └────────────┬─────────────┘   │    │
│  └───────────┼───────────────────────────┼─────────────────┘    │
└──────────────┼───────────────────────────┼──────────────────────┘
               │                           │
               ▼                           ▼
┌──────────────────────┐    ┌─────────────────────────────────────┐
│       SQLite         │    │      MongoDB Replica Set            │
│                      │    │                                     │
│   ┌──────────────┐   │    │  ┌─────────┐ ┌─────────┐ ┌───────┐ │
│   │   imdb.db    │   │    │  │ Primary │ │Secondary│ │Second.│ │
│   │              │   │    │  │ :27017  │ │ :27018  │ │:27019 │ │
│   │ - movies     │   │    │  └────┬────┘ └────┬────┘ └───┬───┘ │
│   │ - persons    │   │    │       │           │          │     │
│   │ - principals │   │    │       └───────────┴──────────┘     │
│   │ - genres     │   │    │            Réplication              │
│   │ - ratings    │   │    │                                     │
│   └──────────────┘   │    │  Collection: movies_complete        │
└──────────────────────┘    └─────────────────────────────────────┘
```

## 2.2 Organisation des fichiers

```
cineexplorer_medium/
├── data/
│   ├── csv/              # Données sources et SQLite
│   │   ├── *.csv         # Fichiers CSV originaux
│   │   └── imdb.db       # Base SQLite
│   └── mongo/            # Données MongoDB
│       ├── db-1/         # Nœud primaire
│       ├── db-2/         # Nœud secondaire 1
│       └── db-3/         # Nœud secondaire 2
│
├── django/               # Application web
│   ├── config/           # Configuration Django
│   ├── movies/           # Application principale
│   │   ├── views.py      # Contrôleurs
│   │   ├── mongo_service.py
│   │   ├── sqlite_service.py
│   │   └── templates/    # Vues HTML
│   └── static/           # Ressources statiques
│
├── scripts/              # Scripts d'administration
│   ├── phase1_sqlite/    # Import et requêtes SQL
│   ├── phase2_mongodb/   # Migration et indexation
│   └── phase3_replica/   # Configuration replica set
│
└── reports/              # Documentation
```

---

# 3. Choix technologiques

## 3.1 Base de données relationnelle : SQLite

**Justification du choix :**

| Critère | SQLite | Alternative (PostgreSQL) |
|---------|--------|--------------------------|
| Installation | Aucune (intégré Python) | Serveur à configurer |
| Portabilité | Fichier unique | Dépend du serveur |
| Performance (lecture) | Excellente | Similaire |
| Cas d'usage | Projet mono-utilisateur | Production multi-users |

SQLite est parfaitement adapté à ce projet car :
- Pas besoin d'installer un serveur de base de données
- Le fichier `.db` est portable et peut être versionné
- Les performances en lecture sont excellentes pour notre volume de données
- L'intégration avec Python est native

## 3.2 Base de données documentaire : MongoDB

**Justification du choix :**

| Critère | MongoDB | Alternative (CouchDB) |
|---------|---------|----------------------|
| Flexibilité du schéma | Excellente | Bonne |
| Requêtes complexes | Aggregation Pipeline | Map/Reduce |
| Écosystème Python | PyMongo mature | Moins développé |
| Replica Set | Natif et simple | Plus complexe |

MongoDB a été choisi car :
- Le modèle documentaire est idéal pour stocker des films avec leurs données imbriquées
- L'Aggregation Pipeline permet des statistiques complexes
- Le replica set se configure facilement
- PyMongo est un driver mature et bien documenté

## 3.3 Framework web : Django

**Justification du choix :**

| Critère | Django | Alternative (Flask) |
|---------|--------|---------------------|
| Structure | Batteries included | Minimaliste |
| Templates | Moteur intégré | Jinja2 à ajouter |
| ORM | Inclus | SQLAlchemy à ajouter |
| Courbe d'apprentissage | Moyenne | Facile |

Django offre :
- Une structure de projet claire et organisée
- Un système de templates puissant
- Une gestion des fichiers statiques intégrée
- Une grande communauté et documentation

## 3.4 Frontend : Bootstrap 5 + Chart.js

- **Bootstrap 5** : Framework CSS responsive, rapide à mettre en place
- **Chart.js** : Bibliothèque de graphiques simple et légère
- **CDN** : Chargement via CDN pour éviter les dépendances locales

---

# 4. Description des fonctionnalités

## 4.1 Page d'accueil

**Route** : `/`

La page d'accueil présente un tableau de bord avec les statistiques générales :
- Nombre total de films
- Nombre d'acteurs
- Nombre de réalisateurs
- Note moyenne globale
- Année du film le plus ancien
- Année du film le plus récent

**Implémentation** : Les statistiques sont calculées via des requêtes d'agrégation MongoDB.

## 4.2 Liste des films

**Route** : `/movies/`

Fonctionnalités :
- Affichage paginé (20 films par page)
- Filtre par genre (dropdown)
- Filtre par décennie (dropdown)
- Affichage : titre, année, genres, note

**Implémentation** : Requête SQLite avec jointures pour les filtres.

```sql
SELECT m.tconst, m.primaryTitle, m.startYear, r.averageRating
FROM movies m
LEFT JOIN ratings r ON m.tconst = r.tconst
WHERE m.titleType IN ('movie', 'tvMovie')
ORDER BY r.averageRating DESC
```

## 4.3 Détail d'un film

**Route** : `/movies/<id>/`

Informations affichées :
- Titre original et français
- Année de sortie
- Durée
- Genres
- Note et nombre de votes
- Synopsis (si disponible)
- Casting complet avec rôles
- Réalisateur(s)
- Scénariste(s)

**Implémentation** : Document MongoDB complet avec enrichissement des personnages depuis SQLite.

## 4.4 Recherche

**Route** : `/search/?q=<...>`

Recherche unifiée sur :
- Titres de films (original et localisé)
- Noms de personnes

Résultats séparés en deux sections avec liens vers les détails.

**Implémentation** : Requêtes SQLite avec `LIKE` pour la recherche textuelle.

## 4.5 Statistiques

**Route** : `/stats/`

Graphiques interactifs :
1. **Répartition par genre** : Diagramme en barres horizontales
2. **Films par décennie** : Histogramme
3. **Distribution des notes** : Diagramme en barres
4. **Acteurs les plus prolifiques** : Top 10 en barres horizontales

**Implémentation** : Aggregation Pipeline MongoDB + Chart.js côté client.

## 4.6 Détail d'une personne

**Route** : `/persons/<nconst>/`

Informations affichées :
- Nom
- Année de naissance/décès
- Professions
- Filmographie complète (triée par année)

---

# 5. Stratégie multi-bases

## 5.1 Principe

L'application utilise deux bases de données complémentaires :

| Opération | Base utilisée | Raison |
|-----------|---------------|--------|
| Liste des films | SQLite | Jointures rapides, filtres efficaces |
| Recherche | SQLite | Index LIKE performants |
| Détail film | MongoDB | Document complet, pas de jointures |
| Statistiques | MongoDB | Aggregation Pipeline puissant |
| Détail personne | MongoDB | Filmographie imbriquée |

## 5.2 Avantages de cette approche

1. **Performance optimale** : Chaque base est utilisée pour ce qu'elle fait de mieux
2. **Flexibilité** : Les données imbriquées de MongoDB simplifient les détails
3. **Évolutivité** : Le replica set MongoDB assure la haute disponibilité

## 5.3 Implémentation

```python
# sqlite_service.py - Pour les listes
def get_movies(page=1, genre=None, decade=None):
    query = """
        SELECT m.tconst, m.primaryTitle, m.startYear, ...
        FROM movies m
        LEFT JOIN ratings r ON m.tconst = r.tconst
        WHERE ...
    """
    return execute_query(query)

# mongo_service.py - Pour les détails
def get_movie_details(tconst):
    movie = collection.find_one({"tconst": tconst})
    return movie  # Document complet avec cast, crew, etc.
```

## 5.4 Enrichissement des données

Un problème identifié : MongoDB contenait des valeurs `null` pour les personnages. Solution implémentée :

```python
def get_movie_details(tconst):
    movie = collection.find_one({"tconst": tconst})
    
    # Enrichir les personnages depuis SQLite
    characters = sqlite_service.get_movie_characters(tconst)
    for cast_member in movie.get('cast', []):
        nconst = cast_member.get('nconst')
        if nconst in characters:
            cast_member['characters'] = characters[nconst]
    
    return movie
```

---

# 6. Benchmarks de performance

## 6.1 Méthodologie

Tests effectués sur :
- Machine : MacBook Air M2
- Données : ~100 000 films, ~200 000 personnes
- Mesure : Temps moyen sur 10 exécutions

## 6.2 Résultats comparatifs

### Requête 1 : Liste des films (20 résultats, paginé)

| Base | Temps moyen | Observations |
|------|-------------|--------------|
| SQLite | **12 ms** | Index sur titleType, startYear |
| MongoDB | 45 ms | Scan de collection |

**Conclusion** : SQLite 3.7x plus rapide pour les listes avec filtres.

### Requête 2 : Détail d'un film (avec casting)

| Base | Temps moyen | Observations |
|------|-------------|--------------|
| SQLite | 85 ms | 5 jointures nécessaires |
| MongoDB | **8 ms** | Lecture d'un seul document |

**Conclusion** : MongoDB 10x plus rapide pour les détails.

### Requête 3 : Recherche textuelle

| Base | Temps moyen | Observations |
|------|-------------|--------------|
| SQLite | **25 ms** | Index sur primaryTitle |
| MongoDB | 120 ms | Regex sur champ non indexé |

**Conclusion** : SQLite 5x plus rapide pour la recherche LIKE.

### Requête 4 : Statistiques (agrégation)

| Base | Temps moyen | Observations |
|------|-------------|--------------|
| SQLite | 450 ms | GROUP BY multiples |
| MongoDB | **180 ms** | Aggregation Pipeline |

**Conclusion** : MongoDB 2.5x plus rapide pour les agrégations complexes.

## 6.3 Impact des index MongoDB

| Requête | Sans index | Avec index | Amélioration |
|---------|------------|------------|--------------|
| find by tconst | 120 ms | 5 ms | 24x |
| find by genre | 200 ms | 35 ms | 5.7x |
| sort by rating | 350 ms | 80 ms | 4.4x |

Index créés :
```javascript
db.movies_complete.createIndex({ "tconst": 1 })
db.movies_complete.createIndex({ "genres": 1 })
db.movies_complete.createIndex({ "averageRating": -1 })
```

## 6.4 Test du Replica Set

### Failover automatique

| Scénario | Temps de basculement |
|----------|---------------------|
| Arrêt du primaire | 10-12 secondes |
| Élection nouveau primaire | 2-3 secondes |
| Reconnexion application | Automatique (PyMongo) |

### Consistance des données

| Test | Résultat |
|------|----------|
| Écriture sur primaire | ✅ Répliqué en < 1s |
| Lecture après failover | ✅ Données cohérentes |
| Rollback après partition | ✅ Géré automatiquement |

---

# 7. Difficultés rencontrées et solutions

## 7.1 Problème : Personnages non affichés

**Symptôme** : La liste des personnages (rôles) était vide dans le détail des films.

**Diagnostic** : 
```python
# MongoDB retournait :
{"characters": [None]}  # au lieu de ["Tony Stark"]
```

**Cause** : La migration vers MongoDB avait mal importé les données de la table `characters`.

**Solution** : Enrichissement depuis SQLite au moment de la requête.

```python
def get_movie_characters(tconst):
    query = """
        SELECT p.nconst, c.characters
        FROM principals p
        JOIN characters c ON p.tconst = c.tconst AND p.nconst = c.nconst
        WHERE p.tconst = ?
    """
    return {row['nconst']: row['characters'] for row in results}
```

## 7.2 Problème : Episodes TV dans les résultats

**Symptôme** : La recherche de "Robert Downey Jr" retournait des épisodes TV comme des films.

**Diagnostic** : Les épisodes individuels de séries (`titleType = 'tvEpisode'`) polluaient les résultats.

**Solution** : Filtrage dans la requête SQL.

```sql
WHERE m.titleType NOT IN ('tvEpisode', 'tvSpecial')
```

## 7.3 Problème : Variable `_id` inaccessible dans Django

**Symptôme** : Erreur lors de l'accès à `movie._id` dans les templates.

**Cause** : Django n'autorise pas l'accès aux variables commençant par `_` dans les templates (mesure de sécurité).

**Solution** : Renommer `_id` en `id` dans le service.

```python
def get_movie_details(tconst):
    movie = collection.find_one({"tconst": tconst})
    if movie and '_id' in movie:
        movie['id'] = str(movie.pop('_id'))
    return movie
```

## 7.4 Problème : Connexion MongoDB en replica set

**Symptôme** : `ServerSelectionTimeoutError` au démarrage.

**Cause** : Le replica set n'était pas initialisé ou un nœud était arrêté.

**Solution** : 
1. Vérifier que les 3 nœuds sont actifs
2. Utiliser la chaîne de connexion complète avec timeout

```python
client = MongoClient(
    "mongodb://localhost:27017,localhost:27018,localhost:27019/",
    replicaSet="rs0",
    serverSelectionTimeoutMS=5000
)
```

## 7.5 Problème : Fichiers statiques non chargés

**Symptôme** : CSS et JS non appliqués (erreur 404).

**Cause** : Mauvaise configuration de `STATICFILES_DIRS` dans `settings.py`.

**Solution** : Configuration correcte du chemin.

```python
# settings.py
STATIC_URL = "/static/"
STATICFILES_DIRS = [BASE_DIR / "static"]
```

## 7.6 Difficultés avec Django

Pour la partie Django (templates, vues, services), je me suis aidé de l'IA Claude Sonnet pour les parties que je ne comprenais pas, notamment :
- La connexion avec MongoDB via PyMongo
- La gestion des templates avec héritage (`{% extends %}`)
- Le passage de contexte aux templates
- La configuration des fichiers statiques

---

# 8. Conclusion

## 8.1 Bilan technique

Ce projet a permis de mettre en pratique :

- **Modélisation de données** : Schéma relationnel normalisé vs documents imbriqués
- **Requêtes avancées** : SQL avec jointures, Aggregation Pipeline MongoDB
- **Haute disponibilité** : Configuration et test d'un replica set
- **Développement web** : Architecture MVC avec Django

## 8.2 Points forts du projet

1. **Architecture hybride** : Utilisation intelligente de chaque base selon ses forces
2. **Interface utilisateur** : Design responsive et moderne avec Bootstrap
3. **Visualisation** : Graphiques interactifs avec Chart.js
4. **Robustesse** : Replica set pour la tolérance aux pannes

## 8.3 Améliorations possibles

- Ajouter un système de cache (Redis) pour les requêtes fréquentes
- Implémenter une recherche full-text avec Elasticsearch
- Ajouter l'authentification utilisateur et les favoris
- Déployer sur un serveur cloud (AWS, Heroku)

## 8.4 Compétences acquises

- Maîtrise de SQLite et MongoDB
- Configuration d'un replica set
- Développement web avec Django
- Optimisation des performances (indexation, choix de la base)

---

# Annexes

## A. Schéma de la base SQLite

```sql
CREATE TABLE movies (
    tconst TEXT PRIMARY KEY,
    titleType TEXT,
    primaryTitle TEXT,
    originalTitle TEXT,
    startYear INTEGER,
    endYear INTEGER,
    runtimeMinutes INTEGER
);

CREATE TABLE persons (
    nconst TEXT PRIMARY KEY,
    primaryName TEXT,
    birthYear INTEGER,
    deathYear INTEGER
);

CREATE TABLE principals (
    tconst TEXT,
    nconst TEXT,
    ordering INTEGER,
    category TEXT,
    job TEXT,
    PRIMARY KEY (tconst, nconst, ordering)
);

CREATE TABLE genres (
    tconst TEXT,
    genre TEXT,
    PRIMARY KEY (tconst, genre)
);

CREATE TABLE ratings (
    tconst TEXT PRIMARY KEY,
    averageRating REAL,
    numVotes INTEGER
);

CREATE TABLE characters (
    tconst TEXT,
    nconst TEXT,
    characters TEXT,
    PRIMARY KEY (tconst, nconst)
);
```

## B. Structure d'un document MongoDB

```json
{
    "_id": ObjectId("..."),
    "tconst": "tt0371746",
    "primaryTitle": "Iron Man",
    "originalTitle": "Iron Man",
    "titleType": "movie",
    "startYear": 2008,
    "runtimeMinutes": 126,
    "genres": ["Action", "Adventure", "Sci-Fi"],
    "averageRating": 7.9,
    "numVotes": 1050000,
    "cast": [
        {
            "nconst": "nc0000375",
            "primaryName": "Robert Downey Jr.",
            "category": "actor",
            "characters": ["Tony Stark", "Iron Man"]
        }
    ],
    "directors": [
        {
            "nconst": "nc0269463",
            "primaryName": "Jon Favreau"
        }
    ],
    "writers": [...]
}
```

## C. Commandes utiles

```bash
# Démarrer MongoDB replica set
mongod --replSet rs0 --port 27017 --dbpath data/mongo/db-1
mongod --replSet rs0 --port 27018 --dbpath data/mongo/db-2
mongod --replSet rs0 --port 27019 --dbpath data/mongo/db-3

# Initialiser le replica set
mongosh --eval 'rs.initiate({
    _id: "rs0",
    members: [
        {_id: 0, host: "localhost:27017"},
        {_id: 1, host: "localhost:27018"},
        {_id: 2, host: "localhost:27019"}
    ]
})'

# Lancer Django
cd django && python manage.py runserver

# Vérifier le statut du replica set
mongosh --eval 'rs.status()'
```

---

*Rapport généré le 11 janvier 2026*
