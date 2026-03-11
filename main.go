package main

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	neturl "net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/joho/godotenv"
	_ "modernc.org/sqlite"
)

const terrariaAppID = 105600
const cacheTTL = 6 * time.Hour
const appMetaCacheTTL = 24 * time.Hour

type appSchemaCacheEntry struct {
	items     []Achievement
	fetchedAt time.Time
}

type appGlobalPctCacheEntry struct {
	items     map[string]float64
	fetchedAt time.Time
}

type Achievement struct {
	APIName     string  `json:"apiName"`
	Name        string  `json:"name"`
	Description string  `json:"description"`
	Icon        string  `json:"icon"`
	IconGray    string  `json:"iconGray"`
	Hidden      bool    `json:"hidden"`
	GlobalPct   float64 `json:"globalPct"`
	Achieved    bool    `json:"achieved,omitempty"`
	UnlockTime  int64   `json:"unlockTime,omitempty"`
}

type OwnedGame struct {
	AppID           int    `json:"appId"`
	Name            string `json:"name"`
	PlaytimeForever int    `json:"playtimeForever"`
}

type GameCompletion struct {
	AppID                int     `json:"appId"`
	Name                 string  `json:"name"`
	PlaytimeForever      int     `json:"playtimeForever"`
	TotalAchievements    int     `json:"totalAchievements"`
	UnlockedAchievements int     `json:"unlockedAchievements"`
	CompletionPct        float64 `json:"completionPct"`
}

type UserSuggestion struct {
	SteamID       string  `json:"steamId"`
	DisplayName   string  `json:"displayName"`
	GamesCount    int     `json:"gamesCount"`
	AvgCompletion float64 `json:"avgCompletion"`
}

type Server struct {
	db              *sql.DB
	apiKey          string
	cacheMu         sync.RWMutex
	appSchemaCache  map[int]appSchemaCacheEntry
	appGlobalPctMap map[int]appGlobalPctCacheEntry
}

var errProfilePrivate = errors.New("steam profile is private or stats unavailable")
var errInvalidSteamAPIKey = errors.New("invalid steam api key")

func main() {
	_ = godotenv.Load() // charge .env si présent

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
	mux.HandleFunc("/api/users/games", s.handleUserGames)
	mux.HandleFunc("/api/users/achievements", s.handleUserAchievements)

	// tes fichiers statiques si tu veux
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

func withCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) initDB() error {
	stmts := []string{
		`PRAGMA journal_mode=WAL;`,
		`CREATE TABLE IF NOT EXISTS achievements (
			api_name   TEXT PRIMARY KEY,
			name       TEXT NOT NULL,
			description TEXT NOT NULL,
			icon       TEXT NOT NULL,
			icon_gray  TEXT NOT NULL,
			hidden     INTEGER NOT NULL
		);`,
		`CREATE TABLE IF NOT EXISTS global_percent (
			api_name   TEXT PRIMARY KEY,
			percent    REAL NOT NULL,
			updated_at INTEGER NOT NULL
		);`,
		`CREATE TABLE IF NOT EXISTS meta (
			key   TEXT PRIMARY KEY,
			value TEXT NOT NULL
		);`,
		`CREATE TABLE IF NOT EXISTS user_games (
			steam_id TEXT NOT NULL,
			app_id INTEGER NOT NULL,
			name TEXT NOT NULL,
			playtime_forever INTEGER NOT NULL DEFAULT 0,
			total_achievements INTEGER NOT NULL DEFAULT 0,
			unlocked_achievements INTEGER NOT NULL DEFAULT 0,
			completion_pct REAL NOT NULL DEFAULT 0,
			updated_at INTEGER NOT NULL,
			PRIMARY KEY(steam_id, app_id)
		);`,
		`CREATE TABLE IF NOT EXISTS user_achievements (
			steam_id TEXT NOT NULL,
			app_id INTEGER NOT NULL,
			api_name TEXT NOT NULL,
			name TEXT NOT NULL,
			description TEXT NOT NULL,
			icon TEXT NOT NULL,
			icon_gray TEXT NOT NULL,
			hidden INTEGER NOT NULL,
			achieved INTEGER NOT NULL,
			unlock_time INTEGER NOT NULL,
			global_pct REAL NOT NULL DEFAULT 0,
			updated_at INTEGER NOT NULL,
			PRIMARY KEY(steam_id, app_id, api_name)
		);`,
		`CREATE TABLE IF NOT EXISTS user_meta (
			steam_id TEXT NOT NULL,
			key TEXT NOT NULL,
			value TEXT NOT NULL,
			PRIMARY KEY(steam_id, key)
		);`,
		`CREATE INDEX IF NOT EXISTS idx_user_games_steam_id ON user_games(steam_id);`,
		`CREATE INDEX IF NOT EXISTS idx_user_achievements_steam_app ON user_achievements(steam_id, app_id);`,
	}
	for _, q := range stmts {
		if _, err := s.db.Exec(q); err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) handleUserGames(w http.ResponseWriter, r *http.Request) {
	steamID := strings.TrimSpace(r.URL.Query().Get("steamId"))
	if !isValidSteamID64(steamID) {
		writeError(w, http.StatusBadRequest, "invalid_steam_id", "steamId must be a valid SteamID64")
		return
	}

	forceRefresh := shouldForceRefresh(r)
	expired, err := s.isUserCacheExpired(steamID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "db_error", err.Error())
		return
	}

	if forceRefresh || expired {
		if err := s.syncUserData(steamID, "french"); err != nil {
			cachedGames, readErr := s.readUserGamesFromDB(steamID)
			if readErr == nil && len(cachedGames) > 0 {
				log.Printf("steam sync warning (games, steamID=%s): %v (serving cached data)", steamID, err)
				w.Header().Set("X-Data-Stale", "1")
				writeJSON(w, cachedGames)
				return
			}

			if errors.Is(err, errProfilePrivate) {
				writeError(w, http.StatusForbidden, "private_profile", "Profil prive ou statistiques inaccessibles pour ce SteamID")
				return
			}
			if errors.Is(err, errInvalidSteamAPIKey) {
				writeError(w, http.StatusBadGateway, "invalid_api_key", "Cle Steam API invalide ou mal configuree cote serveur")
				return
			}
			log.Printf("steam sync error (games, steamID=%s): %v", steamID, err)
			writeError(w, http.StatusBadGateway, "steam_sync_error", "Echec de synchronisation avec Steam")
			return
		}
	}

	games, err := s.readUserGamesFromDB(steamID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "db_error", err.Error())
		return
	}

	writeJSON(w, games)
}

func (s *Server) handleUserSuggestions(w http.ResponseWriter, r *http.Request) {
	q := strings.TrimSpace(r.URL.Query().Get("q"))
	suggestions, err := s.readUserSuggestionsFromDB(q, 12)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "db_error", err.Error())
		return
	}

	writeJSON(w, suggestions)
}

func (s *Server) handleUserAchievements(w http.ResponseWriter, r *http.Request) {
	steamID := strings.TrimSpace(r.URL.Query().Get("steamId"))
	if !isValidSteamID64(steamID) {
		writeError(w, http.StatusBadRequest, "invalid_steam_id", "steamId must be a valid SteamID64")
		return
	}

	appID, err := strconv.Atoi(strings.TrimSpace(r.URL.Query().Get("appId")))
	if err != nil || appID <= 0 {
		writeError(w, http.StatusBadRequest, "invalid_app_id", "appId must be a positive integer")
		return
	}

	forceRefresh := shouldForceRefresh(r)
	expired, err := s.isUserCacheExpired(steamID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "db_error", err.Error())
		return
	}

	if forceRefresh || expired {
		if err := s.syncUserData(steamID, "french"); err != nil {
			cachedItems, readErr := s.readUserAchievementsFromDB(steamID, appID)
			if readErr == nil && len(cachedItems) > 0 {
				log.Printf("steam sync warning (achievements, steamID=%s, appID=%d): %v (serving cached data)", steamID, appID, err)
				w.Header().Set("X-Data-Stale", "1")
				writeJSON(w, cachedItems)
				return
			}

			if errors.Is(err, errProfilePrivate) {
				writeError(w, http.StatusForbidden, "private_profile", "Profil prive ou statistiques inaccessibles pour ce SteamID")
				return
			}
			if errors.Is(err, errInvalidSteamAPIKey) {
				writeError(w, http.StatusBadGateway, "invalid_api_key", "Cle Steam API invalide ou mal configuree cote serveur")
				return
			}
			log.Printf("steam sync error (achievements, steamID=%s, appID=%d): %v", steamID, appID, err)
			writeError(w, http.StatusBadGateway, "steam_sync_error", "Echec de synchronisation avec Steam")
			return
		}
	}

	items, err := s.readUserAchievementsFromDB(steamID, appID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "db_error", err.Error())
		return
	}

	writeJSON(w, items)
}

func (s *Server) handleAchievements(w http.ResponseWriter, r *http.Request) {
	// 1) si cache expiré -> sync steam -> DB
	expired, err := s.isCacheExpired()
	if err != nil {
		http.Error(w, "DB error: "+err.Error(), 500)
		return
	}
	if expired {
		if err := s.syncFromSteam("french"); err != nil {
			// On n’échoue pas forcément si on a déjà des données, mais on le signale.
			log.Printf("sync error: %v", err)
		}
	}

	// 2) lire depuis DB
	items, err := s.readAchievementsFromDB()
	if err != nil {
		http.Error(w, "DB error: "+err.Error(), 500)
		return
	}

	// tri par % desc puis nom
	sort.Slice(items, func(i, j int) bool {
		if items[i].GlobalPct == items[j].GlobalPct {
			return items[i].Name < items[j].Name
		}
		return items[i].GlobalPct > items[j].GlobalPct
	})

	writeJSON(w, items)
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}

func writeError(w http.ResponseWriter, status int, code string, message string) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	writeJSON(w, map[string]any{
		"error":   code,
		"details": message,
	})
}

func isValidSteamID64(v string) bool {
	if len(v) != 17 {
		return false
	}
	_, err := strconv.ParseUint(v, 10, 64)
	return err == nil
}

func shouldForceRefresh(r *http.Request) bool {
	v := strings.TrimSpace(strings.ToLower(r.URL.Query().Get("refresh")))
	return v == "1" || v == "true" || v == "yes"
}

func (s *Server) isCacheExpired() (bool, error) {
	var v string
	err := s.db.QueryRow(`SELECT value FROM meta WHERE key='last_sync'`).Scan(&v)
	if errors.Is(err, sql.ErrNoRows) {
		return true, nil
	}
	if err != nil {
		return true, err
	}

	sec, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return true, nil
	}
	last := time.Unix(sec, 0)
	return time.Since(last) > cacheTTL, nil
}

func (s *Server) setLastSync(t time.Time) error {
	_, err := s.db.Exec(`
		INSERT INTO meta(key,value) VALUES('last_sync', ?)
		ON CONFLICT(key) DO UPDATE SET value=excluded.value
	`, strconv.FormatInt(t.Unix(), 10))
	return err
}

func (s *Server) isUserCacheExpired(steamID string) (bool, error) {
	var v string
	err := s.db.QueryRow(`SELECT value FROM user_meta WHERE steam_id=? AND key='last_sync'`, steamID).Scan(&v)
	if errors.Is(err, sql.ErrNoRows) {
		return true, nil
	}
	if err != nil {
		return true, err
	}

	sec, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return true, nil
	}

	return time.Since(time.Unix(sec, 0)) > cacheTTL, nil
}

func (s *Server) setUserLastSync(steamID string, t time.Time) error {
	_, err := s.db.Exec(`
		INSERT INTO user_meta(steam_id,key,value) VALUES(?,?,?)
		ON CONFLICT(steam_id,key) DO UPDATE SET value=excluded.value
	`, steamID, "last_sync", strconv.FormatInt(t.Unix(), 10))
	return err
}

func (s *Server) readAchievementsFromDB() ([]Achievement, error) {
	rows, err := s.db.Query(`
		SELECT a.api_name, a.name, a.description, a.icon, a.icon_gray, a.hidden,
		       COALESCE(g.percent, 0.0) as percent
		FROM achievements a
		LEFT JOIN global_percent g ON g.api_name = a.api_name
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []Achievement
	for rows.Next() {
		var a Achievement
		var hiddenInt int
		if err := rows.Scan(&a.APIName, &a.Name, &a.Description, &a.Icon, &a.IconGray, &hiddenInt, &a.GlobalPct); err != nil {
			return nil, err
		}
		a.Hidden = hiddenInt == 1
		out = append(out, a)
	}
	return out, rows.Err()
}

func (s *Server) readUserGamesFromDB(steamID string) ([]GameCompletion, error) {
	rows, err := s.db.Query(`
		SELECT app_id, name, playtime_forever, total_achievements, unlocked_achievements, completion_pct
		FROM user_games
		WHERE steam_id=?
		ORDER BY completion_pct DESC, name ASC
	`, steamID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]GameCompletion, 0)
	for rows.Next() {
		var g GameCompletion
		if err := rows.Scan(&g.AppID, &g.Name, &g.PlaytimeForever, &g.TotalAchievements, &g.UnlockedAchievements, &g.CompletionPct); err != nil {
			return nil, err
		}
		out = append(out, g)
	}

	return out, rows.Err()
}

func (s *Server) readUserAchievementsFromDB(steamID string, appID int) ([]Achievement, error) {
	rows, err := s.db.Query(`
		SELECT api_name, name, description, icon, icon_gray, hidden, global_pct, achieved, unlock_time
		FROM user_achievements
		WHERE steam_id=? AND app_id=?
		ORDER BY achieved DESC, global_pct DESC, name ASC
	`, steamID, appID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]Achievement, 0)
	for rows.Next() {
		var a Achievement
		var hiddenInt, achievedInt int
		if err := rows.Scan(&a.APIName, &a.Name, &a.Description, &a.Icon, &a.IconGray, &hiddenInt, &a.GlobalPct, &achievedInt, &a.UnlockTime); err != nil {
			return nil, err
		}
		a.Hidden = hiddenInt == 1
		a.Achieved = achievedInt == 1
		out = append(out, a)
	}

	return out, rows.Err()
}

func (s *Server) readUserSuggestionsFromDB(query string, limit int) ([]UserSuggestion, error) {
	if limit <= 0 {
		limit = 12
	}

	like := "%"
	if query != "" {
		like = "%" + strings.ToLower(query) + "%"
	}

	rows, err := s.db.Query(`
		SELECT
			u.steam_id,
			COALESCE(pname.value, u.steam_id) AS display_name,
			COUNT(*) AS games_count,
			COALESCE(AVG(u.completion_pct), 0.0) AS avg_completion
		FROM user_games u
		LEFT JOIN user_meta pname
			ON pname.steam_id = u.steam_id AND pname.key = 'profile_name'
		WHERE (? = '%' OR LOWER(u.steam_id) LIKE ? OR LOWER(COALESCE(pname.value, '')) LIKE ?)
		GROUP BY u.steam_id, display_name
		ORDER BY games_count DESC, avg_completion DESC, display_name ASC
		LIMIT ?
	`, like, like, like, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]UserSuggestion, 0, limit)
	for rows.Next() {
		var s UserSuggestion
		if err := rows.Scan(&s.SteamID, &s.DisplayName, &s.GamesCount, &s.AvgCompletion); err != nil {
			return nil, err
		}
		out = append(out, s)
	}

	return out, rows.Err()
}

func (s *Server) syncUserData(steamID string, lang string) error {
	profileName, profileErr := fetchPlayerSummaryName(s.apiKey, steamID)
	if profileErr != nil {
		log.Printf("profile summary warning (steamID=%s): %v", steamID, profileErr)
		profileName = ""
	}

	games, err := fetchOwnedGames(s.apiKey, steamID)
	if err != nil {
		if errors.Is(err, errProfilePrivate) {
			return err
		}
		return fmt.Errorf("owned games fetch: %w", err)
	}

	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.Exec(`DELETE FROM user_achievements WHERE steam_id=?`, steamID); err != nil {
		return err
	}
	if _, err := tx.Exec(`DELETE FROM user_games WHERE steam_id=?`, steamID); err != nil {
		return err
	}

	gameStmt, err := tx.Prepare(`
		INSERT INTO user_games(steam_id, app_id, name, playtime_forever, total_achievements, unlocked_achievements, completion_pct, updated_at)
		VALUES(?,?,?,?,?,?,?,?)
	`)
	if err != nil {
		return err
	}
	defer gameStmt.Close()

	achStmt, err := tx.Prepare(`
		INSERT INTO user_achievements(steam_id, app_id, api_name, name, description, icon, icon_gray, hidden, achieved, unlock_time, global_pct, updated_at)
		VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
	`)
	if err != nil {
		return err
	}
	defer achStmt.Close()

	now := time.Now().Unix()
	for _, game := range games {
		schema, err := s.fetchSchemaForGameCached(game.AppID, lang)
		if err != nil {
			log.Printf("skip schema app %d (%s): %v", game.AppID, game.Name, err)
			continue
		}
		if len(schema) == 0 {
			continue
		}

		pcts, err := s.fetchGlobalPercentagesCached(game.AppID)
		if err != nil {
			log.Printf("skip global pct app %d (%s): %v", game.AppID, game.Name, err)
			pcts = map[string]float64{}
		}

		userStats, err := fetchUserAchievementStats(s.apiKey, steamID, game.AppID)
		if err != nil {
			if errors.Is(err, errProfilePrivate) {
				return err
			}
			log.Printf("skip user stats app %d (%s): %v", game.AppID, game.Name, err)
			continue
		}

		unlockedCount := 0
		for _, a := range schema {
			st, ok := userStats[a.APIName]
			if st.Achieved {
				unlockedCount++
			}

			hidden := 0
			if a.Hidden {
				hidden = 1
			}
			achieved := 0
			if ok && st.Achieved {
				achieved = 1
			}

			if _, err := achStmt.Exec(
				steamID,
				game.AppID,
				a.APIName,
				a.Name,
				a.Description,
				a.Icon,
				a.IconGray,
				hidden,
				achieved,
				st.UnlockTime,
				pcts[a.APIName],
				now,
			); err != nil {
				return err
			}
		}

		completion := 0.0
		if len(schema) > 0 {
			completion = float64(unlockedCount) * 100.0 / float64(len(schema))
		}

		if _, err := gameStmt.Exec(
			steamID,
			game.AppID,
			game.Name,
			game.PlaytimeForever,
			len(schema),
			unlockedCount,
			completion,
			now,
		); err != nil {
			return err
		}
	}

	if _, err := tx.Exec(`
		INSERT INTO user_meta(steam_id,key,value) VALUES(?,?,?)
		ON CONFLICT(steam_id,key) DO UPDATE SET value=excluded.value
	`, steamID, "last_sync", strconv.FormatInt(time.Now().Unix(), 10)); err != nil {
		return err
	}

	if profileName != "" {
		if _, err := tx.Exec(`
			INSERT INTO user_meta(steam_id,key,value) VALUES(?,?,?)
			ON CONFLICT(steam_id,key) DO UPDATE SET value=excluded.value
		`, steamID, "profile_name", profileName); err != nil {
			return err
		}
	}

	return tx.Commit()
}

func (s *Server) syncFromSteam(lang string) error {
	// schema + global pcts
	schema, err := fetchSchemaForGame(s.apiKey, terrariaAppID, lang)
	if err != nil {
		return err
	}
	pcts, err := fetchGlobalPercentages(terrariaAppID)
	if err != nil {
		return err
	}

	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// upsert achievements
	achStmt, err := tx.Prepare(`
		INSERT INTO achievements(api_name, name, description, icon, icon_gray, hidden)
		VALUES(?,?,?,?,?,?)
		ON CONFLICT(api_name) DO UPDATE SET
			name=excluded.name,
			description=excluded.description,
			icon=excluded.icon,
			icon_gray=excluded.icon_gray,
			hidden=excluded.hidden
	`)
	if err != nil {
		return err
	}
	defer achStmt.Close()

	for _, a := range schema {
		hidden := 0
		if a.Hidden {
			hidden = 1
		}
		if _, err := achStmt.Exec(a.APIName, a.Name, a.Description, a.Icon, a.IconGray, hidden); err != nil {
			return err
		}
	}

	// upsert global %
	now := time.Now().Unix()
	pctStmt, err := tx.Prepare(`
		INSERT INTO global_percent(api_name, percent, updated_at)
		VALUES(?,?,?)
		ON CONFLICT(api_name) DO UPDATE SET
			percent=excluded.percent,
			updated_at=excluded.updated_at
	`)
	if err != nil {
		return err
	}
	defer pctStmt.Close()

	for apiName, pct := range pcts {
		if _, err := pctStmt.Exec(apiName, pct, now); err != nil {
			return err
		}
	}

	if err := s.setLastSync(time.Now()); err != nil {
		return err
	}

	return tx.Commit()
}

func (s *Server) fetchSchemaForGameCached(appID int, lang string) ([]Achievement, error) {
	now := time.Now()

	s.cacheMu.RLock()
	entry, ok := s.appSchemaCache[appID]
	s.cacheMu.RUnlock()
	if ok && now.Sub(entry.fetchedAt) <= appMetaCacheTTL {
		return entry.items, nil
	}

	items, err := fetchSchemaForGame(s.apiKey, appID, lang)
	if err != nil {
		return nil, err
	}

	s.cacheMu.Lock()
	s.appSchemaCache[appID] = appSchemaCacheEntry{items: items, fetchedAt: now}
	s.cacheMu.Unlock()

	return items, nil
}

func (s *Server) fetchGlobalPercentagesCached(appID int) (map[string]float64, error) {
	now := time.Now()

	s.cacheMu.RLock()
	entry, ok := s.appGlobalPctMap[appID]
	s.cacheMu.RUnlock()
	if ok && now.Sub(entry.fetchedAt) <= appMetaCacheTTL {
		return entry.items, nil
	}

	items, err := fetchGlobalPercentages(appID)
	if err != nil {
		return nil, err
	}

	s.cacheMu.Lock()
	s.appGlobalPctMap[appID] = appGlobalPctCacheEntry{items: items, fetchedAt: now}
	s.cacheMu.Unlock()

	return items, nil
}

/***************
 * Steam calls
 ***************/
func fetchSchemaForGame(apiKey string, appid int, lang string) ([]Achievement, error) {
	url := fmt.Sprintf("https://api.steampowered.com/ISteamUserStats/GetSchemaForGame/v2/?key=%s&appid=%d&l=%s&format=json",
		apiKey, appid, lang)

	body, err := httpGET(url)
	if err != nil {
		return nil, err
	}

	var resp struct {
		Game struct {
			AvailableGameStats struct {
				Achievements []struct {
					Name        string `json:"name"`
					DisplayName string `json:"displayName"`
					Description string `json:"description"`
					Icon        string `json:"icon"`
					IconGray    string `json:"icongray"`
					Hidden      int    `json:"hidden"`
				} `json:"achievements"`
			} `json:"availableGameStats"`
		} `json:"game"`
	}

	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, fmt.Errorf("schema json parse: %w", err)
	}

	out := make([]Achievement, 0, len(resp.Game.AvailableGameStats.Achievements))
	for _, a := range resp.Game.AvailableGameStats.Achievements {
		out = append(out, Achievement{
			APIName:     a.Name,
			Name:        a.DisplayName,
			Description: a.Description,
			Icon:        a.Icon,
			IconGray:    a.IconGray,
			Hidden:      a.Hidden == 1,
		})
	}
	return out, nil
}

func fetchGlobalPercentages(appid int) (map[string]float64, error) {
	url := fmt.Sprintf("https://api.steampowered.com/ISteamUserStats/GetGlobalAchievementPercentagesForApp/v0002/?gameid=%d&format=json", appid)

	body, err := httpGET(url)
	if err != nil {
		return nil, err
	}

	var resp struct {
		AchievementPercentages struct {
			Achievements []struct {
				Name    string  `json:"name"`
				Percent float64 `json:"percent"`
			} `json:"achievements"`
		} `json:"achievementpercentages"`
	}

	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, fmt.Errorf("global pct json parse: %w", err)
	}

	out := make(map[string]float64, len(resp.AchievementPercentages.Achievements))
	for _, a := range resp.AchievementPercentages.Achievements {
		out[a.Name] = a.Percent
	}
	return out, nil
}

func fetchOwnedGames(apiKey string, steamID string) ([]OwnedGame, error) {
	url := fmt.Sprintf("https://api.steampowered.com/IPlayerService/GetOwnedGames/v0001/?key=%s&steamid=%s&include_appinfo=1&include_played_free_games=1&format=json", apiKey, steamID)

	body, status, err := httpGETWithStatus(url)
	if err != nil {
		if status == http.StatusUnauthorized || status == http.StatusForbidden {
			return nil, errInvalidSteamAPIKey
		}
		return nil, err
	}

	var resp struct {
		Response struct {
			Games []struct {
				AppID           int    `json:"appid"`
				Name            string `json:"name"`
				PlaytimeForever int    `json:"playtime_forever"`
			} `json:"games"`
		} `json:"response"`
	}

	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, fmt.Errorf("owned games json parse: %w", err)
	}

	out := make([]OwnedGame, 0, len(resp.Response.Games))
	for _, g := range resp.Response.Games {
		out = append(out, OwnedGame{AppID: g.AppID, Name: g.Name, PlaytimeForever: g.PlaytimeForever})
	}

	return out, nil
}

func fetchPlayerSummaryName(apiKey string, steamID string) (string, error) {
	url := fmt.Sprintf("https://api.steampowered.com/ISteamUser/GetPlayerSummaries/v0002/?key=%s&steamids=%s&format=json", apiKey, steamID)

	body, err := httpGET(url)
	if err != nil {
		return "", err
	}

	var resp struct {
		Response struct {
			Players []struct {
				PersonaName string `json:"personaname"`
			} `json:"players"`
		} `json:"response"`
	}

	if err := json.Unmarshal(body, &resp); err != nil {
		return "", fmt.Errorf("player summary json parse: %w", err)
	}
	if len(resp.Response.Players) == 0 {
		return "", nil
	}

	return strings.TrimSpace(resp.Response.Players[0].PersonaName), nil
}

type userAchievementState struct {
	Achieved   bool
	UnlockTime int64
}

func fetchUserAchievementStats(apiKey string, steamID string, appID int) (map[string]userAchievementState, error) {
	url := fmt.Sprintf("https://api.steampowered.com/ISteamUserStats/GetUserStatsForGame/v0002/?key=%s&steamid=%s&appid=%d&format=json", apiKey, steamID, appID)

	body, status, err := httpGETWithStatus(url)
	if err != nil {
		if status == http.StatusForbidden {
			return nil, errProfilePrivate
		}
		return nil, err
	}

	var resp struct {
		PlayerStats struct {
			Error        string `json:"error"`
			Achievements []struct {
				Name       string `json:"name"`
				Achieved   int    `json:"achieved"`
				UnlockTime int64  `json:"unlocktime"`
			} `json:"achievements"`
		} `json:"playerstats"`
	}

	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, fmt.Errorf("user stats json parse: %w", err)
	}

	if resp.PlayerStats.Error != "" {
		msg := strings.ToLower(resp.PlayerStats.Error)
		if strings.Contains(msg, "private") || strings.Contains(msg, "forbidden") {
			return nil, errProfilePrivate
		}
		return nil, fmt.Errorf("user stats steam error: %s", resp.PlayerStats.Error)
	}

	out := make(map[string]userAchievementState, len(resp.PlayerStats.Achievements))
	for _, a := range resp.PlayerStats.Achievements {
		out[a.Name] = userAchievementState{Achieved: a.Achieved == 1, UnlockTime: a.UnlockTime}
	}

	return out, nil
}

func httpGET(url string) ([]byte, error) {
	body, _, err := httpGETWithStatus(url)
	return body, err
}

func httpGETWithStatus(url string) ([]byte, int, error) {
	client := &http.Client{Timeout: 12 * time.Second}
	res, err := client.Get(url)
	if err != nil {
		return nil, 0, err
	}
	defer res.Body.Close()

	if res.StatusCode < 200 || res.StatusCode > 299 {
		b, _ := io.ReadAll(io.LimitReader(res.Body, 4096))
		safeURL := url
		if u, parseErr := neturl.Parse(url); parseErr == nil {
			safeURL = u.Scheme + "://" + u.Host + u.Path
		}
		return nil, res.StatusCode, fmt.Errorf("GET %s -> %d: %s", safeURL, res.StatusCode, strconv.Quote(string(b)))
	}

	b, err := io.ReadAll(res.Body)
	return b, res.StatusCode, err
}
