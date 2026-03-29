package main

import (
	"fmt"
	"strings"
	"time"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

type SupabaseStore struct {
	db *gorm.DB
}

type TrackedUser struct {
	ID        uint      `json:"id" gorm:"primaryKey"`
	SteamID   string    `json:"steamId" gorm:"size:17;not null;uniqueIndex"`
	Nickname  string    `json:"nickname" gorm:"size:120;not null"`
	CreatedAt time.Time `json:"createdAt"`
	UpdatedAt time.Time `json:"updatedAt"`
}

func newSupabaseStore(dsn string) (*SupabaseStore, error) {
	dsn = strings.TrimSpace(dsn)
	if dsn == "" {
		return nil, nil
	}

	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		return nil, fmt.Errorf("open supabase postgres: %w", err)
	}

	if err := db.AutoMigrate(&TrackedUser{}); err != nil {
		return nil, fmt.Errorf("automigrate supabase tables: %w", err)
	}

	return &SupabaseStore{db: db}, nil
}

func (s *SupabaseStore) isEnabled() bool {
	return s != nil && s.db != nil
}
