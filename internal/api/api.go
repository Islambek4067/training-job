// Package api is the HTTP front-end. It holds no state; every request is one or two
// SQL statements in internal/store.
package api

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"

	"soupq/internal/store"
)

type Server struct {
	Store       *store.Store
	MaxAttempts int
	Log         *slog.Logger
}

func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /jobs", s.create)
	mux.HandleFunc("GET /jobs", s.list)
	mux.HandleFunc("GET /jobs/{id}", s.get)
	mux.HandleFunc("POST /jobs/{id}/cancel", s.cancel)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		if err := s.Store.Pool.Ping(r.Context()); err != nil {
			writeErr(w, http.StatusServiceUnavailable, err.Error())
			return
		}
		w.Write([]byte("ok"))
	})
	return mux
}

type createReq struct {
	Payload json.RawMessage `json:"payload"`
}

func (s *Server) create(w http.ResponseWriter, r *http.Request) {
	key := r.Header.Get("Idempotency-Key")
	if key == "" || len(key) > 255 {
		writeErr(w, http.StatusBadRequest, "Idempotency-Key header is required (1..255 chars)")
		return
	}
	var req createReq
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	if err := dec.Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return
	}
	if len(req.Payload) == 0 || string(req.Payload) == "null" {
		req.Payload = json.RawMessage(`{}`)
	}
	job, created, err := s.Store.CreateJob(r.Context(), key, req.Payload, s.MaxAttempts)
	switch {
	case errors.Is(err, store.ErrIdempotencyReuse):
		writeErr(w, http.StatusConflict, err.Error())
	case err != nil:
		s.internal(w, "create", err)
	case created:
		writeJSON(w, http.StatusCreated, job)
	default:
		w.Header().Set("Idempotent-Replayed", "true")
		writeJSON(w, http.StatusOK, job)
	}
}

func (s *Server) get(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	job, err := s.Store.GetJob(r.Context(), id)
	switch {
	case errors.Is(err, store.ErrNotFound):
		writeErr(w, http.StatusNotFound, err.Error())
	case err != nil:
		s.internal(w, "get", err)
	default:
		writeJSON(w, http.StatusOK, job)
	}
}

var validStatus = map[string]bool{
	"": true, store.StatusQueued: true, store.StatusRunning: true,
	store.StatusSucceeded: true, store.StatusFailed: true, store.StatusCancelled: true,
}

func (s *Server) list(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	status := q.Get("status")
	if !validStatus[status] {
		writeErr(w, http.StatusBadRequest, "unknown status")
		return
	}
	limit := 50
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > 500 {
			writeErr(w, http.StatusBadRequest, "limit must be 1..500")
			return
		}
		limit = n
	}
	var cursor int64
	if v := q.Get("cursor"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil || n < 1 {
			writeErr(w, http.StatusBadRequest, "bad cursor")
			return
		}
		cursor = n
	}
	jobs, next, err := s.Store.ListJobs(r.Context(), status, cursor, limit)
	if err != nil {
		s.internal(w, "list", err)
		return
	}
	resp := map[string]any{"jobs": jobs, "next_cursor": nil}
	if next != 0 {
		resp["next_cursor"] = strconv.FormatInt(next, 10)
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) cancel(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	job, err := s.Store.CancelJob(r.Context(), id)
	switch {
	case errors.Is(err, store.ErrNotFound):
		writeErr(w, http.StatusNotFound, err.Error())
	case errors.Is(err, store.ErrAlreadyTerminal):
		writeJSON(w, http.StatusConflict, map[string]any{"error": err.Error(), "job": job})
	case err != nil:
		s.internal(w, "cancel", err)
	default:
		writeJSON(w, http.StatusOK, job)
	}
}

func pathID(w http.ResponseWriter, r *http.Request) (int64, bool) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil || id < 1 {
		writeErr(w, http.StatusBadRequest, "bad job id")
		return 0, false
	}
	return id, true
}

func (s *Server) internal(w http.ResponseWriter, op string, err error) {
	s.Log.Error("request failed", "op", op, "err", err)
	writeErr(w, http.StatusInternalServerError, "internal error")
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}
