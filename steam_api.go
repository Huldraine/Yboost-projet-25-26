package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	neturl "net/url"
	"strconv"
	"strings"
	"time"
)

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

func fetchPlayerSummary(apiKey string, steamID string) (UserProfile, error) {
	url := fmt.Sprintf("https://api.steampowered.com/ISteamUser/GetPlayerSummaries/v0002/?key=%s&steamids=%s&format=json", apiKey, steamID)

	body, err := httpGET(url)
	if err != nil {
		return UserProfile{}, err
	}

	var resp struct {
		Response struct {
			Players []struct {
				PersonaName string `json:"personaname"`
				AvatarFull  string `json:"avatarfull"`
			} `json:"players"`
		} `json:"response"`
	}

	if err := json.Unmarshal(body, &resp); err != nil {
		return UserProfile{}, fmt.Errorf("player summary json parse: %w", err)
	}
	if len(resp.Response.Players) == 0 {
		return UserProfile{}, nil
	}

	player := resp.Response.Players[0]
	return UserProfile{
		SteamID:     steamID,
		DisplayName: strings.TrimSpace(player.PersonaName),
		AvatarURL:   strings.TrimSpace(player.AvatarFull),
	}, nil
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
