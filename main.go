package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"
)

const terrariaAppID = 105600

type Achievement struct {
	APIName     string  `json:"apiName"`
	Name        string  `json:"name"`
	Description string  `json:"description"`
	Icon        string  `json:"icon"`
	IconGray    string  `json:"iconGray"`
	Hidden      bool    `json:"hidden"`
	GlobalPct   float64 `json:"globalPct"`
}

type cacheState struct {
	mu      sync.Mutex
	expires time.Time
	data    []Achievement
}

var cache cacheState

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	mux := http.NewServeMux()

	// API
	mux.HandleFunc("/api/achievements", achievementsHandler)

	// Static files
	fs := http.FileServer(http.Dir("./static"))
	mux.Handle("/", fs)

	addr := ":" + port
	log.Printf("Listening on %s", addr)
	log.Fatal(http.ListenAndServe(addr, withCORS(mux)))
}

func withCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Suffisant pour un petit projet local. Ajuste si besoin.
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

func achievementsHandler(w http.ResponseWriter, r *http.Request) {
	// Cache 6h pour éviter de spam l’API
	if data, ok := getCached(); ok {
		writeJSON(w, data)
		return
	}

	data, err := fetchTerrariaAchievements()
	if err != nil {
		http.Error(w, "Failed to fetch achievements: "+err.Error(), http.StatusBadGateway)
		return
	}

	setCached(data, 6*time.Hour)
	writeJSON(w, data)
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}

func getCached() ([]Achievement, bool) {
	cache.mu.Lock()
	defer cache.mu.Unlock()
	if time.Now().Before(cache.expires) && len(cache.data) > 0 {
		return cache.data, true
	}
	return nil, false
}

func setCached(data []Achievement, ttl time.Duration) {
	cache.mu.Lock()
	defer cache.mu.Unlock()
	cache.data = data
	cache.expires = time.Now().Add(ttl)
}

func fetchTerrariaAchievements() ([]Achievement, error) {
	// 1) Schema (requires key)
	schema, err := fetchSchemaForGame(terrariaAppID, "french")
	if err != nil {
		return nil, err
	}

	// 2) Global percentages (no key)
	pcts, err := fetchGlobalPercentages(terrariaAppID)
	if err != nil {
		return nil, err
	}

	// Merge
	out := make([]Achievement, 0, len(schema))
	for _, a := range schema {
		a.GlobalPct = pcts[a.APIName]
		out = append(out, a)
	}

	// Tri par % décroissant (puis nom)
	sort.Slice(out, func(i, j int) bool {
		if out[i].GlobalPct == out[j].GlobalPct {
			return out[i].Name < out[j].Name
		}
		return out[i].GlobalPct > out[j].GlobalPct
	})

	return out, nil
}

/***************
 * Steam calls
 ***************/

func fetchSchemaForGame(appid int, lang string) ([]Achievement, error) {
	key := os.Getenv("STEAM_API_KEY")
	if key == "" {
		return nil, errors.New("missing STEAM_API_KEY env var (required for GetSchemaForGame)")
	}

	url := fmt.Sprintf("https://api.steampowered.com/ISteamUserStats/GetSchemaForGame/v2/?key=%s&appid=%d&l=%s&format=json",
		key, appid, lang)

	body, err := httpGET(url)
	if err != nil {
		return nil, err
	}

	// Partial schema for achievements
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

	achs := resp.Game.AvailableGameStats.Achievements
	out := make([]Achievement, 0, len(achs))
	for _, a := range achs {
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
	// Valve doc uses "gameid" parameter for this method
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
		// percent peut être float style 12.3456
		out[a.Name] = a.Percent
	}
	return out, nil
}

func httpGET(url string) ([]byte, error) {
	client := &http.Client{Timeout: 12 * time.Second}
	res, err := client.Get(url)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	if res.StatusCode < 200 || res.StatusCode > 299 {
		b, _ := io.ReadAll(io.LimitReader(res.Body, 4096))
		return nil, fmt.Errorf("GET %s -> %d: %s", url, res.StatusCode, strconv.Quote(string(b)))
	}

	return io.ReadAll(res.Body)
}