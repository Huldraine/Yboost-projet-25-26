package main

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"sort"
	"strconv"
	"strings"
)

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

func (s *Server) handleUserGames(w http.ResponseWriter, r *http.Request) {
	identifier := strings.TrimSpace(r.URL.Query().Get("steamId"))
	steamID, err := s.resolveSteamIDInput(identifier)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_user_identifier", "Entre un SteamID64 valide ou un pseudo deja present en base")
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

func (s *Server) handleUserProfile(w http.ResponseWriter, r *http.Request) {
	identifier := strings.TrimSpace(r.URL.Query().Get("steamId"))
	steamID, err := s.resolveSteamIDInput(identifier)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_user_identifier", "Entre un SteamID64 valide ou un pseudo deja present en base")
		return
	}

	profile, err := s.readUserProfileFromDB(steamID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "db_error", err.Error())
		return
	}

	if profile.DisplayName == "" || profile.AvatarURL == "" {
		summary, summaryErr := fetchPlayerSummary(s.apiKey, steamID)
		if summaryErr == nil {
			_ = s.upsertUserMetaValue(steamID, "profile_name", summary.DisplayName)
			_ = s.upsertUserMetaValue(steamID, "profile_avatar", summary.AvatarURL)
			profile = summary
		}
	}

	profile.SteamID = steamID
	if profile.DisplayName == "" {
		profile.DisplayName = steamID
	}

	writeJSON(w, profile)
}

func (s *Server) handleUserAchievements(w http.ResponseWriter, r *http.Request) {
	identifier := strings.TrimSpace(r.URL.Query().Get("steamId"))
	steamID, err := s.resolveSteamIDInput(identifier)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_user_identifier", "Entre un SteamID64 valide ou un pseudo deja present en base")
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
	expired, err := s.isCacheExpired()
	if err != nil {
		http.Error(w, "DB error: "+err.Error(), http.StatusInternalServerError)
		return
	}
	if expired {
		if err := s.syncFromSteam("french"); err != nil {
			log.Printf("sync error: %v", err)
		}
	}

	items, err := s.readAchievementsFromDB()
	if err != nil {
		http.Error(w, "DB error: "+err.Error(), http.StatusInternalServerError)
		return
	}

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
