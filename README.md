# Yboost Projet 25-26

Application backend en Go pour suivre les achievements Steam.

Concrètement, le projet fait 3 choses :
- expose une API HTTP
- garde les données en cache dans SQLite
- propose un CRUD avec Supabase/PostgreSQL via GORM

## Stack (simple)

- Go 1.24
- API HTTP: net/http
- Base locale: SQLite (modernc.org/sqlite)
- ORM: GORM
- Base distante: PostgreSQL (Supabase)
- Variables d'environnement: .env (godotenv)
- Front statique: dossier static

## A quoi sert chaque fichier

- main.go: démarre le serveur et déclare les routes
- http_handlers.go: logique des routes API
- steam_api.go: appels vers Steam
- sync.go: synchro Steam -> SQLite
- db.go: création des tables + requêtes SQL
- supabase.go: connexion Supabase + modèle ORM
- crud_handlers.go: routes CRUD (create/read/update/delete)

## Variables d'environnement

### Obligatoire

- STEAM_API_KEY
  - clé API Steam
  - sans cette clé, le serveur ne démarre pas

### Optionnelles

- PORT (par défaut: 8080)
- DB_PATH (par défaut: steam_achievements.db)
- SUPABASE_DB_URL (active le CRUD Supabase)
- DATABASE_URL (utilisée si SUPABASE_DB_URL est vide)

Le serveur lit .env au démarrage.

Exemple .env:

```env
STEAM_API_KEY=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
PORT=8080
DB_PATH=steam_achievements.db
SUPABASE_DB_URL=postgresql://user:pass@host:5432/dbname?sslmode=require
```

## Lancer en local

Prérequis: Go 1.24+

```bash
go mod download
go run .
```

Le serveur écoute sur http://localhost:PORT.

## Déploiement

### Option 1 (simple): binaire

```bash
go build -o yboost-projet-25-26 .
./yboost-projet-25-26
```

### Option 2: avec Procfile

Le projet contient:

```procfile
web: yboost-projet-25-26
```

## Comportement API

- CORS ouvert
- Compression gzip si le client la demande
- Cache des fichiers statiques:
  - .js, .css, images, fonts: max-age=86400
  - autres fichiers: no-cache

## Cache et synchro

- cache utilisateur: 6h
- cache mémoire des métadonnées jeux: 24h

Si le cache est expiré:
1. le serveur tente une synchro Steam
2. si Steam échoue, il renvoie les données SQLite déjà en cache
3. dans ce cas, la réponse contient le header X-Data-Stale: 1

Tu peux forcer la synchro avec refresh=1, refresh=true ou refresh=yes.

## Routes API

Base URL: /

### Lecture Steam/SQLite

### GET /api/achievements

- route historique globale (app Steam par défaut)
- trie les achievements par globalPct DESC puis name ASC

### GET /api/users/suggestions?q=texte

- renvoie des suggestions d'utilisateurs
- tri: games_count DESC, avg_completion DESC, display_name ASC
- limite: 12

### GET /api/users/profile?steamId=id-ou-pseudo

- renvoie le profil utilisateur (nom/avatar)

### GET /api/users/games?steamId=id-ou-pseudo&refresh=true

- renvoie les jeux d'un joueur avec sa progression
- tri SQL: completion_pct DESC puis name ASC

### GET /api/users/achievements?steamId=id-ou-pseudo&appId=123&refresh=true

- renvoie les achievements d'un jeu pour un joueur
- tri SQL: achieved DESC, global_pct DESC, name ASC

## Routes CRUD Supabase (GORM)

Si Supabase n'est pas configuré, ces routes renvoient 503 avec le code supabase_not_configured.

### GET /api/crud/users

- liste des utilisateurs suivis
- tri: id DESC

### POST /api/crud/users

- crée un utilisateur suivi

Exemple body JSON:

```json
{
  "steamId": "7656119xxxxxxxxxx",
  "nickname": "MonPseudo",
  "gamesCount": 12,
  "totalAchievements": 300,
  "unlockedAchievements": 120
}
```

Le champ steam_id (ancien format) est aussi accepté.

### GET /api/crud/users/{id}

- lit un utilisateur par son id interne

### PUT /api/crud/users/{id}

- met à jour un utilisateur (partiellement)
- refuse un body vide

### DELETE /api/crud/users/{id}

- supprime un utilisateur
- réponse: 204 No Content

### POST /api/crud/users/save

- fait un upsert par steamId
- si l'utilisateur existe: update
- sinon: create
- si les stats ne sont pas envoyées, elles sont calculées depuis SQLite

### GET /api/crud/leaderboard?limit=50

- renvoie le classement
- tri:
1. unlocked_achievements DESC
2. total_achievements DESC
3. games_count DESC
4. nickname ASC
- limit max: 200

### DELETE /api/crud/cache

- vide complètement la table tracked_users
- réponse: { "deleted": <rows> }

## Format des erreurs

Toutes les erreurs API sont renvoyées en JSON:

```json
{
  "error": "code_erreur",
  "details": "message"
}
```

Exemples de codes:
- invalid_user_identifier
- invalid_app_id
- private_profile
- invalid_api_key
- steam_sync_error
- db_error
- method_not_allowed

## Données stockées

SQLite:
- achievements
- global_percent
- meta
- user_games
- user_achievements
- user_meta
- user_stats

Supabase/PostgreSQL:
- tracked_users (table gérée par GORM)

## Frontend

Fichiers statiques:
- static/index.html
- static/app.js
- static/styles.css

## Résumé rapide

Ce projet récupère des données Steam, les sauvegarde en cache, et expose des routes API faciles à utiliser. Il peut aussi gérer une liste d'utilisateurs suivis (CRUD) via Supabase.
