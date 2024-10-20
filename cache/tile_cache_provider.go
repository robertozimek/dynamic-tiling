package cache

import (
	"context"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
	"time"
)

type TileCacheProvider struct {
	redisClient   *redis.Client
	cacheDuration time.Duration
}

func NewTileCacheProvider(url string, cacheDuration string) *TileCacheProvider {
	options, err := redis.ParseURL(url)

	if err != nil {
		log.Err(err).Msg("Failed to parse redis url")
		return &TileCacheProvider{}
	}

	duration, err := time.ParseDuration(cacheDuration)
	if err != nil {
		duration = time.Hour
	}

	return &TileCacheProvider{
		redisClient:   redis.NewClient(options),
		cacheDuration: duration,
	}
}

func (cache *TileCacheProvider) set(ctx context.Context, key string, value interface{}) {
	if cache.redisClient != nil {
		cache.redisClient.Set(ctx, key, value, cache.cacheDuration)
	}
}

func (cache *TileCacheProvider) GetBytes(ctx context.Context, key string, fallback func() ([]byte, error)) ([]byte, error) {
	fallbackAndSet := func() ([]byte, error) {
		value, err := fallback()
		if err != nil {
			return nil, err
		}

		go func() {
			cache.set(ctx, key, value)
		}()
		return value, nil
	}

	if cache.redisClient != nil {
		value, err := cache.redisClient.Get(ctx, key).Bytes()
		if err == redis.Nil {
			return fallbackAndSet()
		}
		return value, err
	}
	return fallbackAndSet()
}
