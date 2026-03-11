package main

import (
	"database/sql"
	"errors"
	"sync"
	"time"
)

const defaultGlobalAppID = 105600 // Steam app ID (Terraria), used by /api/achievements legacy endpoint.
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

type UserProfile struct {
	SteamID     string `json:"steamId"`
	DisplayName string `json:"displayName"`
	AvatarURL   string `json:"avatarUrl"`
}

type Server struct {
	db              *sql.DB
	apiKey          string
	cacheMu         sync.RWMutex
	appSchemaCache  map[int]appSchemaCacheEntry
	appGlobalPctMap map[int]appGlobalPctCacheEntry
}

type userAchievementState struct {
	Achieved   bool
	UnlockTime int64
}

var errProfilePrivate = errors.New("steam profile is private or stats unavailable")
var errInvalidSteamAPIKey = errors.New("invalid steam api key")
