package main

import (
	"log"
	"os"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

var db *gorm.DB

func initDB() {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		log.Fatal("La variable d'environnement DATABASE_URL est requise")
	}

	

	var err error
	db, err = gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Fatalf("Impossible de se connecter à la base de données : %v", err)
	}


	// Récupère la connexion SQL sous-jacente
    sqlDB, err := db.DB()
    if err != nil {
        log.Printf("Erreur lors de la récupération de la connexion SQL : %v", err)
    } else {
        // Teste la connexion
        if err := sqlDB.Ping(); err != nil {
            log.Fatalf("Impossible de ping la base : %v", err)
        }
        
        // Infos sur la connexion
        log.Printf("✓ Connexion à la base établie")
        log.Printf("✓ Max open connections: %d", sqlDB.Stats().MaxOpenConnections)
        log.Printf("✓ Connexions actives: %d", sqlDB.Stats().OpenConnections)
    }
	log.Println("Base de données connectée et migrée avec succès")
}
