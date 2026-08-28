// Package auth implements the service's bearer-token authentication.
package auth

import (
	"crypto/sha256"
	"crypto/subtle"
	"strings"
)

// Authenticator checks bearer tokens without comparing the supplied token
// directly with the configured secret. Hashing both values first also keeps
// the comparison length-independent.
type Authenticator struct {
	keyDigest [sha256.Size]byte
}

// New returns an authenticator for apiKey. An empty key never authenticates.
func New(apiKey string) *Authenticator {
	return &Authenticator{keyDigest: sha256.Sum256([]byte(apiKey))}
}

// ValidBearer reports whether authorization is a valid Bearer credential.
func (a *Authenticator) ValidBearer(authorization string) bool {
	const scheme = "Bearer"
	if a == nil || len(authorization) <= len(scheme) || authorization[len(scheme)] != ' ' ||
		!strings.EqualFold(authorization[:len(scheme)], scheme) {
		return false
	}

	token := strings.TrimSpace(authorization[len(scheme)+1:])
	if token == "" {
		return false
	}
	digest := sha256.Sum256([]byte(token))
	return subtle.ConstantTimeCompare(digest[:], a.keyDigest[:]) == 1
}
