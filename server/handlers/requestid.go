package handlers

import (
	"context"
	"fmt"
	"net/http"

	"github.com/gofrs/uuid"
	"github.com/meshery/meshery/server/models"
)

// RequestIDMiddleware generates or propagates a unique request ID for every
// incoming HTTP request.  The ID is:
//  1. Read from the X-Request-ID header if present (for distributed tracing
//     where an upstream proxy or ingress already assigned one).
//  2. Generated as a v4 UUID otherwise.
//
// The ID is stored in the request context (models.RequestIDCtxKey) and set
// on the response via the X-Request-ID header so clients and downstream
// middleware can correlate error bodies, log entries, and traces.
func (h *Handler) RequestIDMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqID := r.Header.Get("X-Request-ID")
		if reqID == "" {
			uid, err := uuid.NewV4()
			if err != nil {
				h.log.Warn(fmt.Errorf("failed to generate request ID, falling back to empty"))
				reqID = ""
			} else {
				reqID = uid.String()
			}
		}

		// Set the response header early so all downstream writes carry it.
		w.Header().Set("X-Request-ID", reqID)

		// Store in context.
		ctx := context.WithValue(r.Context(), models.RequestIDCtxKey, reqID)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}
