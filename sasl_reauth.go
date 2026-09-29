package kafka

import (
	"math/rand"
	"time"
)

// Re-authenticate at a random point in the last part of the session, like the
// Java client, so many connections opened together do not re-authenticate at once.
const (
	saslReauthWindowMin = 0.85
	saslReauthWindowMax = 0.95
)

// saslReauthTimeout bounds a re-authentication on a Conn, whose own deadlines
// may be unset (partition readers) or stale (readOffsets) at that moment.
const saslReauthTimeout = 10 * time.Second

// Re-authentication times are measured on the monotonic clock from this point,
// so a wall-clock step cannot move them past the broker's session expiry.
var saslReauthEpoch = time.Now()

// nextSASLReauth returns the monotonic time (nanoseconds since saslReauthEpoch)
// at which a connection with the given SASL session lifetime must re-authenticate,
// or 0 for an unbounded session.
func nextSASLReauth(lifetime time.Duration) int64 {
	if lifetime <= 0 {
		return 0
	}

	frac := saslReauthWindowMin + rand.Float64()*(saslReauthWindowMax-saslReauthWindowMin)
	at := int64(time.Since(saslReauthEpoch) + time.Duration(float64(lifetime)*frac))
	if at == 0 {
		at = 1
	}
	return at
}

// saslReauthDue reports whether the re-authentication time returned by
// nextSASLReauth has been reached.
func saslReauthDue(at int64) bool {
	return at != 0 && int64(time.Since(saslReauthEpoch)) >= at
}
