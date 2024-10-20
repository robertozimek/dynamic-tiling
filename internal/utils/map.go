package utils

func CopyExcludingKeys[M ~map[K]V, K comparable, V any](src M, dst M, omitKeys []K) {
	omitKeysMap := make(map[K]bool)
	for _, k := range omitKeys {
		omitKeysMap[k] = true
	}

	for k, v := range src {
		if !omitKeysMap[k] {
			dst[k] = v
		}
	}
}

func GetValueOrDefault[M ~map[K]V, K comparable, V any](m M, key K, defaultValue V) V {
	value, ok := m[key]
	if !ok {
		return defaultValue
	}

	return value
}
