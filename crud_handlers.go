package main

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"

	"gorm.io/gorm"
)

type trackedUserInput struct {
	SteamID  string `json:"steamId"`
	Nickname string `json:"nickname"`
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

		payload.SteamID = strings.TrimSpace(payload.SteamID)
		payload.Nickname = strings.TrimSpace(payload.Nickname)
		if !isValidSteamID64(payload.SteamID) {
			writeError(w, http.StatusBadRequest, "invalid_steam_id", "steamId doit contenir 17 chiffres")
			return
		}
		if payload.Nickname == "" {
			payload.Nickname = payload.SteamID
		}

		item := TrackedUser{SteamID: payload.SteamID, Nickname: payload.Nickname}
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
		if v := strings.TrimSpace(payload.SteamID); v != "" {
			if !isValidSteamID64(v) {
				writeError(w, http.StatusBadRequest, "invalid_steam_id", "steamId doit contenir 17 chiffres")
				return
			}
			updates["steam_id"] = v
		}
		if v := strings.TrimSpace(payload.Nickname); v != "" {
			updates["nickname"] = v
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
