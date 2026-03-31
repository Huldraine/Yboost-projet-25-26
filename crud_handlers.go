package main

import (
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type trackedUserInput struct {
	SteamID              string `json:"steamId"`
	SteamIDLegacy        string `json:"steam_id"`
	Nickname             string `json:"nickname"`
	GamesCount           *int   `json:"gamesCount"`
	TotalAchievements    *int   `json:"totalAchievements"`
	UnlockedAchievements *int   `json:"unlockedAchievements"`
}

type saveTrackedUserInput struct {
	SteamID              string `json:"steamId"`
	SteamIDLegacy        string `json:"steam_id"`
	Nickname             string `json:"nickname"`
	GamesCount           *int   `json:"gamesCount"`
	TotalAchievements    *int   `json:"totalAchievements"`
	UnlockedAchievements *int   `json:"unlockedAchievements"`
}

func (s *Server) handleTrackedUsers(w http.ResponseWriter, r *http.Request) {
	if !s.isSupabaseReady(w) {
		return
	}

	switch r.Method {
	case http.MethodGet:
		var items []TrackedUser
		if err := s.supabase.db.Order("id DESC").Find(&items).Error; err != nil {
			writeError(w, http.StatusInternalServerError, "db_error", err.Error())
			return
		}
		writeJSON(w, items)
	case http.MethodPost:
		var payload trackedUserInput
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			writeError(w, http.StatusBadRequest, "invalid_json", "Corps JSON invalide")
			return
		}

		payload.SteamID = normalizeSteamID(payload.SteamID, payload.SteamIDLegacy)
		payload.Nickname = strings.TrimSpace(payload.Nickname)
		if !isValidSteamID64(payload.SteamID) {
			writeError(w, http.StatusBadRequest, "invalid_steam_id", "steamId doit contenir 17 chiffres")
			return
		}
		if payload.Nickname == "" {
			payload.Nickname = payload.SteamID
		}

		item := TrackedUser{
			SteamID:              payload.SteamID,
			Nickname:             payload.Nickname,
			GamesCount:           sanitizeNonNegative(payload.GamesCount),
			TotalAchievements:    sanitizeNonNegative(payload.TotalAchievements),
			UnlockedAchievements: sanitizeNonNegative(payload.UnlockedAchievements),
		}
		if err := s.supabase.db.Create(&item).Error; err != nil {
			writeError(w, http.StatusConflict, "create_error", err.Error())
			return
		}

		w.WriteHeader(http.StatusCreated)
		writeJSON(w, item)
	default:
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "Method non autorisee")
	}
}

func (s *Server) handleTrackedUserByID(w http.ResponseWriter, r *http.Request) {
	if !s.isSupabaseReady(w) {
		return
	}

	id, err := parseIDFromPath(r.URL.Path, "/api/crud/users/")
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_id", err.Error())
		return
	}

	switch r.Method {
	case http.MethodGet:
		var item TrackedUser
		err := s.supabase.db.First(&item, id).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			writeError(w, http.StatusNotFound, "not_found", "Utilisateur introuvable")
			return
		}
		if err != nil {
			writeError(w, http.StatusInternalServerError, "db_error", err.Error())
			return
		}
		writeJSON(w, item)
	case http.MethodPut:
		var payload trackedUserInput
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			writeError(w, http.StatusBadRequest, "invalid_json", "Corps JSON invalide")
			return
		}

		updates := map[string]any{}
		if v := normalizeSteamID(payload.SteamID, payload.SteamIDLegacy); v != "" {
			if !isValidSteamID64(v) {
				writeError(w, http.StatusBadRequest, "invalid_steam_id", "steamId doit contenir 17 chiffres")
				return
			}
			updates["steam_id"] = v
		}
		if v := strings.TrimSpace(payload.Nickname); v != "" {
			updates["nickname"] = v
		}
		if payload.GamesCount != nil {
			updates["games_count"] = sanitizeNonNegative(payload.GamesCount)
		}
		if payload.TotalAchievements != nil {
			updates["total_achievements"] = sanitizeNonNegative(payload.TotalAchievements)
		}
		if payload.UnlockedAchievements != nil {
			updates["unlocked_achievements"] = sanitizeNonNegative(payload.UnlockedAchievements)
		}
		if len(updates) == 0 {
			writeError(w, http.StatusBadRequest, "empty_update", "Aucune valeur a mettre a jour")
			return
		}

		res := s.supabase.db.Model(&TrackedUser{}).Where("id = ?", id).Updates(updates)
		if res.Error != nil {
			writeError(w, http.StatusConflict, "update_error", res.Error.Error())
			return
		}
		if res.RowsAffected == 0 {
			writeError(w, http.StatusNotFound, "not_found", "Utilisateur introuvable")
			return
		}

		var item TrackedUser
		if err := s.supabase.db.First(&item, id).Error; err != nil {
			writeError(w, http.StatusInternalServerError, "db_error", err.Error())
			return
		}
		writeJSON(w, item)
	case http.MethodDelete:
		res := s.supabase.db.Delete(&TrackedUser{}, id)
		if res.Error != nil {
			writeError(w, http.StatusInternalServerError, "delete_error", res.Error.Error())
			return
		}
		if res.RowsAffected == 0 {
			writeError(w, http.StatusNotFound, "not_found", "Utilisateur introuvable")
			return
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "Method non autorisee")
	}
}

func (s *Server) handleSaveTrackedUser(w http.ResponseWriter, r *http.Request) {
	if !s.isSupabaseReady(w) {
		return
	}
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "Method non autorisee")
		return
	}

	var payload saveTrackedUserInput
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_json", "Corps JSON invalide")
		return
	}

	steamID := normalizeSteamID(payload.SteamID, payload.SteamIDLegacy)
	if !isValidSteamID64(steamID) {
		writeError(w, http.StatusBadRequest, "invalid_steam_id", "steamId doit contenir 17 chiffres")
		return
	}

	nickname := strings.TrimSpace(payload.Nickname)
	if nickname == "" {
		nickname = steamID
	}

	stats, err := s.resolveAchievementStats(steamID, payload)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "db_error", err.Error())
		return
	}

	item := TrackedUser{
		SteamID:              steamID,
		Nickname:             nickname,
		GamesCount:           stats.gamesCount,
		TotalAchievements:    stats.totalAchievements,
		UnlockedAchievements: stats.unlockedAchievements,
	}

	if err := s.supabase.db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "steam_id"}},
		DoUpdates: clause.AssignmentColumns([]string{"nickname", "games_count", "total_achievements", "unlocked_achievements", "updated_at"}),
	}).Create(&item).Error; err != nil {
		writeError(w, http.StatusConflict, "save_error", err.Error())
		return
	}

	if err := s.supabase.db.Where("steam_id = ?", steamID).First(&item).Error; err != nil {
		writeError(w, http.StatusInternalServerError, "db_error", err.Error())
		return
	}

	writeJSON(w, item)
}

func (s *Server) handleTrackedLeaderboard(w http.ResponseWriter, r *http.Request) {
	if !s.isSupabaseReady(w) {
		return
	}
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "Method non autorisee")
		return
	}

	limit := 50
	if raw := strings.TrimSpace(r.URL.Query().Get("limit")); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err == nil && parsed > 0 {
			if parsed > 200 {
				parsed = 200
			}
			limit = parsed
		}
	}

	var items []TrackedUser
	if err := s.supabase.db.
		Order("unlocked_achievements DESC").
		Order("total_achievements DESC").
		Order("games_count DESC").
		Order("nickname ASC").
		Limit(limit).
		Find(&items).Error; err != nil {
		writeError(w, http.StatusInternalServerError, "db_error", err.Error())
		return
	}

	writeJSON(w, items)
}

func (s *Server) handleSupabaseCache(w http.ResponseWriter, r *http.Request) {
	if !s.isSupabaseReady(w) {
		return
	}
	if r.Method != http.MethodDelete {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "Method non autorisee")
		return
	}

	res := s.supabase.db.Session(&gorm.Session{AllowGlobalUpdate: true}).Delete(&TrackedUser{})
	if res.Error != nil {
		writeError(w, http.StatusInternalServerError, "db_error", res.Error.Error())
		return
	}

	writeJSON(w, map[string]any{
		"deleted": res.RowsAffected,
	})
}

type resolvedStats struct {
	gamesCount           int
	totalAchievements    int
	unlockedAchievements int
}

func (s *Server) resolveAchievementStats(steamID string, payload saveTrackedUserInput) (resolvedStats, error) {
	gamesCount := sanitizeNonNegative(payload.GamesCount)
	totalAchievements := sanitizeNonNegative(payload.TotalAchievements)
	unlockedAchievements := sanitizeNonNegative(payload.UnlockedAchievements)

	if payload.GamesCount != nil || payload.TotalAchievements != nil || payload.UnlockedAchievements != nil {
		return resolvedStats{
			gamesCount:           gamesCount,
			totalAchievements:    totalAchievements,
			unlockedAchievements: unlockedAchievements,
		}, nil
	}

	var count sql.NullInt64
	var total sql.NullInt64
	var unlocked sql.NullInt64
	err := s.db.QueryRow(`
		SELECT COUNT(*), COALESCE(SUM(total_achievements), 0), COALESCE(SUM(unlocked_achievements), 0)
		FROM user_games
		WHERE steam_id=?
	`, steamID).Scan(&count, &total, &unlocked)
	if err != nil {
		return resolvedStats{}, err
	}

	return resolvedStats{
		gamesCount:           nullIntToNonNegative(count),
		totalAchievements:    nullIntToNonNegative(total),
		unlockedAchievements: nullIntToNonNegative(unlocked),
	}, nil
}

func nullIntToNonNegative(v sql.NullInt64) int {
	if !v.Valid || v.Int64 < 0 {
		return 0
	}
	return int(v.Int64)
}

func sanitizeNonNegative(v *int) int {
	if v == nil || *v < 0 {
		return 0
	}
	return *v
}

func normalizeSteamID(primary string, legacy string) string {
	v := strings.TrimSpace(primary)
	if v != "" {
		return v
	}
	return strings.TrimSpace(legacy)
}

func (s *Server) isSupabaseReady(w http.ResponseWriter) bool {
	if s.supabase == nil || !s.supabase.isEnabled() {
		writeError(w, http.StatusServiceUnavailable, "supabase_not_configured", "Supabase n'est pas configure (DATABASE_URL ou SUPABASE_DB_URL manquant)")
		return false
	}
	return true
}

func parseIDFromPath(path string, prefix string) (uint, error) {
	if !strings.HasPrefix(path, prefix) {
		return 0, errors.New("route invalide")
	}
	idRaw := strings.Trim(strings.TrimPrefix(path, prefix), "/")
	if idRaw == "" {
		return 0, errors.New("id manquant")
	}

	id, err := strconv.ParseUint(idRaw, 10, 64)
	if err != nil || id == 0 {
		return 0, errors.New("id invalide")
	}

	return uint(id), nil
}
