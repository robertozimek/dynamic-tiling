package main

import (
	"context"
	"fmt"
	"github.com/go-chi/chi/v5"
	"github.com/robertozimek/dynamic-tiling/cache"
	"github.com/robertozimek/dynamic-tiling/internal/utils"
	"github.com/robertozimek/dynamic-tiling/postgres"
	"github.com/robertozimek/dynamic-tiling/tiling"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"net/http"
	"strconv"
)

func GetMvtTile(tileCacheProvider *cache.TileCacheProvider, tileService *tiling.TileService) func(http.ResponseWriter, *http.Request) {
	redisContext := context.Background()
	disableCompression, err := strconv.ParseBool(utils.GetEnvOrDefault("DISABLE_GZIP", "false"))
	if err != nil {
		log.Error().Err(err).Msg("Error parsing DISABLE_GZIP")
	}
	return func(w http.ResponseWriter, r *http.Request) {
		x, err := strconv.Atoi(chi.URLParam(r, "x"))
		y, err := strconv.Atoi(chi.URLParam(r, "y"))
		z, err := strconv.Atoi(chi.URLParam(r, "z"))
		if err != nil {
			log.Error().Err(err).Msg("Failed to parse url parameter")
			http.Error(w, http.StatusText(400), 400)
			return
		}

		query := r.URL.Query().Get("query")
		geoCol := r.URL.Query().Get("geoCol")
		srid := utils.GetValueOrDefault(r.URL.Query(), "srid", []string{"4326"})[0]

		cacheKey := utils.Hash(fmt.Sprintf("/mvt/%s/%s/%s?query=%s&geoCol=%s&srid=%s", x, y, z, query, geoCol, srid))
		cachedValue, err := tileCacheProvider.GetBytes(redisContext, cacheKey, func() ([]byte, error) {
			return tileService.GetTile(r.Context(), &tiling.TileQuery{
				X:        uint32(x),
				Y:        uint32(y),
				Z:        uint32(z),
				Query:    query,
				GeoCol:   geoCol,
				Srid:     srid,
				Compress: !disableCompression,
			})
		})

		if err != nil {
			errorLogLevel := zerolog.ErrorLevel
			if postgres.IsCancellationError(err) {
				errorLogLevel = zerolog.DebugLevel
			}

			log.WithLevel(errorLogLevel).Err(err).Msg("Failed to get tile")
			http.Error(w, http.StatusText(500), 500)
			return
		}

		cacheControlHeader := utils.GetEnvOrDefault("CACHE_CONTROL_HEADER", "private, max-age=300")
		w.Header().Set("Content-Type", "application/x-protobuf")
		w.Header().Set("Cache-Control", cacheControlHeader)
		if !disableCompression {
			w.Header().Set("Content-Encoding", "gzip")
		}

		_, err = w.Write(cachedValue)
		if err != nil {
			log.Err(err).Msg("Error writing response")
			return
		}
	}
}
