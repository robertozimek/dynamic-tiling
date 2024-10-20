package utils

import (
	"github.com/markphelps/optional"
	"os"
	"strconv"
)

func GetEnvOrDefault(key string, fallback string) string {
	value, exists := os.LookupEnv(key)
	if !exists {
		value = fallback
	}
	return value
}

var debugMode optional.Bool

func IsDebug() bool {
	if debugMode.Present() {
		return debugMode.MustGet()
	}

	debug, err := strconv.ParseBool(GetEnvOrDefault("DEBUG", "false"))
	if err != nil {
		panic(err)
	}

	debugMode.Set(debug)
	return debugMode.MustGet()
}
