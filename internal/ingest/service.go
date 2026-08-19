// Package ingest accepts call-completion webhooks and processes them.
package ingest

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/convin/webhook-ingest/internal/stats"
	"github.com/convin/webhook-ingest/internal/store"
)

const (
	// recordingWork stands in for downloading and transcoding a recording.
	recordingWork = 50 * time.Millisecond

	// recordingTimeout bounds one recording-processing attempt.
	recordingTimeout = time.Second
)

// Service ingests webhook deliveries.
type Service struct {
	store *store.Store
	cache *stats.Cache
	rdb   *redis.Client
	log   *slog.Logger
}

// New builds a Service.
func New(s *store.Store, c *stats.Cache, rdb *redis.Client, log *slog.Logger) *Service {
	return &Service{store: s, cache: c, rdb: rdb, log: log}
}

// Stats returns the cached totals for an account.
func (s *Service) Stats(accountID string) stats.AccountStats {
	return s.cache.Get(accountID)
}

// Ingest stores a delivery and kicks off processing. Processing runs
// asynchronously so the provider gets a fast acknowledgement.
func (s *Service) Ingest(ctx context.Context, evt Event) error {
	payload, err := json.Marshal(evt)
	if err != nil {
		return err
	}

	rec := store.Event{
		EventID:      evt.EventID,
		CallID:       evt.CallID,
		AccountID:    evt.AccountID,
		Status:       evt.Status,
		DurationSec:  evt.DurationSec,
		RecordingURL: evt.RecordingURL,
		OccurredAt:   evt.OccurredAt,
		Payload:      payload,
	}
	inserted, err := s.store.IngestEvent(ctx, rec)
	if err != nil {
		return err
	}
	if !inserted {
		s.log.Info("duplicate delivery ignored", "event_id", evt.EventID)
		return nil
	}
	s.cache.Record(rec.AccountID, rec.DurationSec)

	// Recordings are slow to fetch, so that part does not block the provider.
	if rec.RecordingURL != "" {
		s.startRecordingProcessing(rec)
	}

	return nil
}

// processRecording downloads and transcodes the call recording, then marks
// the call as done.
func (s *Service) processRecording(ctx context.Context, rec store.Event) error {
	time.Sleep(recordingWork)
	return s.store.MarkRecordingProcessed(ctx, rec.CallID)
}

// RecoverPendingRecordings restarts recording work left unfinished by a prior process.
func (s *Service) RecoverPendingRecordings(ctx context.Context) error {
	callIDs, err := s.store.PendingRecordingCallIDs(ctx)
	if err != nil {
		return err
	}
	for _, callID := range callIDs {
		s.startRecordingProcessing(store.Event{CallID: callID})
	}
	return nil
}

func (s *Service) startRecordingProcessing(rec store.Event) {
	go func() {
		recordingCtx, cancel := context.WithTimeout(context.Background(), recordingTimeout)
		defer cancel()

		if err := s.processRecording(recordingCtx, rec); err != nil {
			s.log.Error("process recording failed", "event_id", rec.EventID, "call_id", rec.CallID, "err", err)
		}
	}()
}
