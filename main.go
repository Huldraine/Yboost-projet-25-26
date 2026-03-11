package main

import (
	"database/sql"
	"log"
	"net/http"
	"os"
	"strings"

	"github.com/joho/godotenv"
	_ "modernc.org/sqlite"
)

func main() {
	_ = godotenv.Load() // charge .env si present

	port := getenv("PORT", "8080")
	dbPath := getenv("DB_PATH", "steam_achievements.db")
	apiKey := cleanEnvValue(os.Getenv("STEAM_API_KEY"))
	if apiKey == "" {
		log.Fatal("STEAM_API_KEY manquant (mets-le dans .env)")
	}

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		log.Fatal(err)
	}
	// SQLite is file-based; one shared connection avoids writer lock contention.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	defer db.Close()

	s := &Server{
		db:              db,
		apiKey:          apiKey,
		appSchemaCache:  make(map[int]appSchemaCacheEntry),
		appGlobalPctMap: make(map[int]appGlobalPctCacheEntry),
	}

	if err := s.initDB(); err != nil {
		log.Fatal(err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/api/achievements", s.handleAchievements)
	mux.HandleFunc("/api/users/suggestions", s.handleUserSuggestions)
	mux.HandleFunc("/api/users/profile", s.handleUserProfile)
	mux.HandleFunc("/api/users/games", s.handleUserGames)
	mux.HandleFunc("/api/users/achievements", s.handleUserAchievements)

	mux.Handle("/", http.FileServer(http.Dir("./static")))

	addr := ":" + port
	log.Printf("Listening on %s (db=%s)", addr, dbPath)
	log.Fatal(http.ListenAndServe(addr, withCORS(mux)))
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func cleanEnvValue(v string) string {
	v = strings.TrimSpace(v)
	v = strings.Trim(v, "\"'")
	return v
}
