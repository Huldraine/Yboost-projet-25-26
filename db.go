package main

import (
	"database/sql"
	"errors"
	"strconv"
	"strings"
	"time"
)

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
		`CREATE TABLE IF NOT EXISTS user_stats (
			steam_id TEXT PRIMARY KEY,
			games_count INTEGER NOT NULL DEFAULT 0,
			avg_completion REAL NOT NULL DEFAULT 0,
			updated_at INTEGER NOT NULL
		);`,
		`CREATE INDEX IF NOT EXISTS idx_user_games_steam_id ON user_games(steam_id);`,
		`CREATE INDEX IF NOT EXISTS idx_user_achievements_steam_app ON user_achievements(steam_id, app_id);`,
		`CREATE INDEX IF NOT EXISTS idx_user_meta_key_value ON user_meta(key, value);`,
		`CREATE INDEX IF NOT EXISTS idx_user_stats_rank ON user_stats(games_count DESC, avg_completion DESC);`,
		`INSERT INTO user_stats(steam_id, games_count, avg_completion, updated_at)
		 SELECT steam_id, COUNT(*), COALESCE(AVG(completion_pct), 0.0), strftime('%s','now')
		 FROM user_games
		 GROUP BY steam_id
		 ON CONFLICT(steam_id) DO UPDATE SET
		 	games_count=excluded.games_count,
		 	avg_completion=excluded.avg_completion,
		 	updated_at=excluded.updated_at;`,
	}
	for _, q := range stmts {
		if _, err := s.db.Exec(q); err != nil {
			return err
		}
	}
	return nil
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

func (s *Server) isUserGameCacheExpired(steamID string, appID int) (bool, error) {
	if appID <= 0 {
		return true, nil
	}

	key := "last_sync_app_" + strconv.Itoa(appID)
	var v string
	err := s.db.QueryRow(`SELECT value FROM user_meta WHERE steam_id=? AND key=?`, steamID, key).Scan(&v)
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

	q := strings.ToLower(strings.TrimSpace(query))
	like := "%"
	if q != "" {
		like = "%" + q + "%"
	}

	rows, err := s.db.Query(`
		SELECT
			s.steam_id,
			COALESCE(pname.value, s.steam_id) AS display_name,
			s.games_count,
			s.avg_completion
		FROM user_stats s
		LEFT JOIN user_meta pname
			ON pname.steam_id = s.steam_id AND pname.key = 'profile_name'
		WHERE (? = '%' OR LOWER(s.steam_id) LIKE ? OR LOWER(COALESCE(pname.value, '')) LIKE ?)
		ORDER BY s.games_count DESC, s.avg_completion DESC, display_name ASC
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

func (s *Server) readUserProfileFromDB(steamID string) (UserProfile, error) {
	profile := UserProfile{SteamID: steamID, DisplayName: steamID}

	rows, err := s.db.Query(`
		SELECT key, value
		FROM user_meta
		WHERE steam_id=? AND key IN ('profile_name', 'profile_avatar')
	`, steamID)
	if err != nil {
		return profile, err
	}
	defer rows.Close()

	for rows.Next() {
		var key, value string
		if err := rows.Scan(&key, &value); err != nil {
			return profile, err
		}
		switch key {
		case "profile_name":
			if strings.TrimSpace(value) != "" {
				profile.DisplayName = strings.TrimSpace(value)
			}
		case "profile_avatar":
			profile.AvatarURL = strings.TrimSpace(value)
		}
	}

	return profile, rows.Err()
}

func (s *Server) upsertUserMetaValue(steamID string, key string, value string) error {
	_, err := s.db.Exec(`
		INSERT INTO user_meta(steam_id,key,value) VALUES(?,?,?)
		ON CONFLICT(steam_id,key) DO UPDATE SET value=excluded.value
	`, steamID, key, value)
	return err
}

func (s *Server) resolveSteamIDInput(raw string) (string, error) {
	v := strings.TrimSpace(raw)
	if v == "" {
		return "", errors.New("empty user identifier")
	}
	if isValidSteamID64(v) {
		return v, nil
	}

	rows, err := s.db.Query(`
		SELECT steam_id
		FROM user_meta
		WHERE key='profile_name' AND LOWER(value)=LOWER(?)
		LIMIT 2
	`, v)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	matched := make([]string, 0, 2)
	for rows.Next() {
		var id string
		if scanErr := rows.Scan(&id); scanErr != nil {
			return "", scanErr
		}
		matched = append(matched, id)
	}
	if err := rows.Err(); err != nil {
		return "", err
	}

	if len(matched) == 1 {
		return matched[0], nil
	}
	if len(matched) > 1 {
		return "", errors.New("ambiguous profile name")
	}

	return "", errors.New("unknown profile name")
}
