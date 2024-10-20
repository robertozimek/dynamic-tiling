package main

import (
	"flag"
	"fmt"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/cors"
	"github.com/joho/godotenv"
	"github.com/robertozimek/dynamic-tiling/cache"
	"github.com/robertozimek/dynamic-tiling/internal/utils"
	"github.com/robertozimek/dynamic-tiling/postgres"
	"github.com/robertozimek/dynamic-tiling/tiling"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"net/http"
	"os"
	"strings"
)

func init() {
	environment := flag.String("env", "", "environment to use")
	flag.Parse()
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	zerolog.SetGlobalLevel(zerolog.InfoLevel)
	if utils.IsDebug() {
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	}

	exists := utils.IsFlagSet("env")

	environmentFile := ".env"
	if exists {
		environmentFile = fmt.Sprintf(".env.%s", environment)
	}

	err := godotenv.Load(environmentFile)
	if err != nil {
		log.Debug().Err(err).Msg("Error loading .env file")
	}
}

type ErrorResponse struct {
	StatusCode int      `json:"status_code"`
	Messages   []string `json:"messages"`
}

func main() {
	router := chi.NewRouter()

	allowedOrigins := strings.Split(utils.GetEnvOrDefault("ALLOWED_ORIGINS", "https://* http://*"), " ")

	router.Use(cors.Handler(cors.Options{
		AllowedOrigins:   allowedOrigins,
		AllowedMethods:   []string{"GET", "OPTIONS"},
		AllowedHeaders:   []string{"Accept", "Authorization", "Content-Type", "X-CSRF-Token"},
		ExposedHeaders:   []string{"Link"},
		AllowCredentials: false,
		MaxAge:           300,
	}))

	redisCacheDuration := utils.GetEnvOrDefault("REDIS_CACHE_DURATION", "1h")
	tileCacheProvider := cache.NewTileCacheProvider(os.Getenv("REDIS_URL"), redisCacheDuration)

	db := postgres.NewDBConnection(&postgres.DBConnectionOptions{
		Host:     os.Getenv("POSTGRES_HOST"),
		Port:     os.Getenv("POSTGRES_PORT"),
		User:     os.Getenv("POSTGRES_USER"),
		Pass:     os.Getenv("POSTGRES_PASS"),
		Database: os.Getenv("POSTGRES_DB_NAME"),
		SSLMode:  utils.GetEnvOrDefault("POSTGRES_SSL_MODE", "require"),
		ReadOnly: true,
	})

	tileService := tiling.NewService(db)

	router.Get("/mvt/{x:[0-9]+}/{y:[0-9]+}/{z:[0-9]+}", GetMvtTile(tileCacheProvider, tileService))

	port := utils.GetEnvOrDefault("PORT", "8095")
	log.Info().Msg("Dynamic Tiling Server Started on PORT: " + port)
	err := http.ListenAndServe(fmt.Sprintf(":%s", port), router)
	if err != nil {
		panic(err)
	}

}
