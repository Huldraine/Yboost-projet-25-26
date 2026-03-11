package main

import (
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"
)

func (s *Server) syncUserData(steamID string, lang string) error {
	summary, profileErr := fetchPlayerSummary(s.apiKey, steamID)
	if profileErr != nil {
		log.Printf("profile summary warning (steamID=%s): %v", steamID, profileErr)
		summary = UserProfile{}
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

	if strings.TrimSpace(summary.DisplayName) != "" {
		if _, err := tx.Exec(`
			INSERT INTO user_meta(steam_id,key,value) VALUES(?,?,?)
			ON CONFLICT(steam_id,key) DO UPDATE SET value=excluded.value
		`, steamID, "profile_name", summary.DisplayName); err != nil {
			return err
		}
	}

	if strings.TrimSpace(summary.AvatarURL) != "" {
		if _, err := tx.Exec(`
			INSERT INTO user_meta(steam_id,key,value) VALUES(?,?,?)
			ON CONFLICT(steam_id,key) DO UPDATE SET value=excluded.value
		`, steamID, "profile_avatar", summary.AvatarURL); err != nil {
			return err
		}
	}

	return tx.Commit()
}

func (s *Server) syncFromSteam(lang string) error {
	schema, err := fetchSchemaForGame(s.apiKey, defaultGlobalAppID, lang)
	if err != nil {
		return err
	}
	pcts, err := fetchGlobalPercentages(defaultGlobalAppID)
	if err != nil {
		return err
	}

	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

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
