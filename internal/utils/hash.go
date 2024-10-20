package utils

import (
	"crypto/sha1"
	"encoding/hex"
)

func Hash(s string) string {
	h := sha1.New()
	h.Write([]byte(s))
	sha1Hash := hex.EncodeToString(h.Sum(nil))
	return sha1Hash
}
