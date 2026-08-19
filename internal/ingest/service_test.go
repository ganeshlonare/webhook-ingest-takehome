package ingest_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/convin/webhook-ingest/internal/ingest"
	"github.com/convin/webhook-ingest/internal/stats"
	"github.com/convin/webhook-ingest/internal/store"
	"github.com/convin/webhook-ingest/internal/testutil"
)

// eventJSON builds a well-formed call-completion payload.
func eventJSON(eventID, callID, accountID string) string {
	return fmt.Sprintf(`{
	  "event_id":      %q,
	  "call_id":       %q,
	  "account_id":    %q,
	  "status":        "completed",
	  "duration_sec":  143,
	  "recording_url": "https://recordings.example.com/%s.wav",
	  "occurred_at":   "2026-08-13T09:12:00Z"
	}`, eventID, callID, accountID, callID)
}

func post(t *testing.T, url, body string) *http.Response {
	t.Helper()
	resp, err := http.Post(url, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	return resp
}

func TestWebhookStoresEventAndCall(t *testing.T) {
	srv, st := testutil.NewServer(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	body := eventJSON(eventID, callID, accountID)
	if resp := post(t, srv.URL+"/webhooks/calls", body); resp.StatusCode != http.StatusOK {
		t.Fatalf("got %d, want 200", resp.StatusCode)
	}

	exists, err := st.EventExists(ctx, eventID)
	if err != nil {
		t.Fatalf("EventExists: %v", err)
	}
	if !exists {
		t.Fatal("expected the event to be stored")
	}

	var gotAccount string
	row := st.Pool().QueryRow(ctx, `SELECT account_id FROM calls WHERE call_id = $1`, callID)
	if err := row.Scan(&gotAccount); err != nil {
		t.Fatalf("expected a call record for %s: %v", callID, err)
	}
	if gotAccount != accountID {
		t.Fatalf("call belongs to %q, want %q", gotAccount, accountID)
	}
}

func TestWebhookWithRecordingMarksRecordingProcessed(t *testing.T) {
	srv, st := testutil.NewServer(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	if resp := post(t, srv.URL+"/webhooks/calls", eventJSON(eventID, callID, accountID)); resp.StatusCode != http.StatusOK {
		t.Fatalf("got %d, want 200", resp.StatusCode)
	}

	deadline := time.Now().Add(time.Second)
	for {
		var processed bool
		if err := st.Pool().QueryRow(ctx,
			`SELECT recording_processed FROM calls WHERE call_id = $1`, callID).Scan(&processed); err != nil {
			t.Fatalf("read recording state: %v", err)
		}
		if processed {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("recording was not marked processed")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestServiceRecoversPendingRecordings(t *testing.T) {
	st := testutil.NewStore(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()
	emptyRecordingCallID := callID + "_empty_recording"

	if err := st.UpsertCall(ctx, store.Event{
		EventID: eventID, CallID: callID, AccountID: accountID,
		Status: "completed", DurationSec: 143,
		RecordingURL: "https://recordings.example.com/" + callID + ".wav",
	}); err != nil {
		t.Fatalf("UpsertCall: %v", err)
	}
	if err := st.UpsertCall(ctx, store.Event{
		EventID: eventID + "_empty_recording", CallID: emptyRecordingCallID, AccountID: accountID,
		Status: "completed", DurationSec: 143,
	}); err != nil {
		t.Fatalf("UpsertCall without recording URL: %v", err)
	}

	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	svc := ingest.New(st, stats.NewCache(), nil, log)
	if err := svc.RecoverPendingRecordings(ctx); err != nil {
		t.Fatalf("RecoverPendingRecordings: %v", err)
	}

	deadline := time.Now().Add(time.Second)
	for {
		var processed bool
		if err := st.Pool().QueryRow(ctx,
			`SELECT recording_processed FROM calls WHERE call_id = $1`, callID).Scan(&processed); err != nil {
			t.Fatalf("read recording state: %v", err)
		}
		if processed {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("pending recording was not recovered")
		}
		time.Sleep(10 * time.Millisecond)
	}

	var emptyRecordingProcessed bool
	if err := st.Pool().QueryRow(ctx,
		`SELECT recording_processed FROM calls WHERE call_id = $1`, emptyRecordingCallID).Scan(&emptyRecordingProcessed); err != nil {
		t.Fatalf("read empty recording state: %v", err)
	}
	if emptyRecordingProcessed {
		t.Fatal("call without a recording URL was incorrectly marked processed")
	}
}

func TestDuplicateDeliveryIsIgnored(t *testing.T) {
	srv, st := testutil.NewServer(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	body := eventJSON(eventID, callID, accountID)
	for i := 0; i < 3; i++ {
		if resp := post(t, srv.URL+"/webhooks/calls", body); resp.StatusCode != http.StatusOK {
			t.Fatalf("delivery %d: got %d, want 200", i, resp.StatusCode)
		}
	}

	var n int
	row := st.Pool().QueryRow(ctx, `SELECT count(*) FROM events WHERE event_id = $1`, eventID)
	if err := row.Scan(&n); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if n != 1 {
		t.Fatalf("stored %d copies of %s, want 1", n, eventID)
	}

	stats, err := st.AccountStats(ctx, accountID)
	if err != nil {
		t.Fatalf("AccountStats: %v", err)
	}
	if stats.CallCount != 1 || stats.TotalDurationSec != 143 {
		t.Fatalf("got %+v, want CallCount=1 TotalDurationSec=143", stats)
	}
}

func TestConcurrentDuplicateDeliveriesAreIdempotent(t *testing.T) {
	srv, st := testutil.NewServer(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	const deliveries = 10
	body := eventJSON(eventID, callID, accountID)
	errs := make(chan error, deliveries)
	var wg sync.WaitGroup
	for range deliveries {
		wg.Add(1)
		go func() {
			defer wg.Done()

			resp, err := http.Post(srv.URL+"/webhooks/calls", "application/json", strings.NewReader(body))
			if err != nil {
				errs <- fmt.Errorf("post webhook: %w", err)
				return
			}
			defer func() { _ = resp.Body.Close() }()
			if resp.StatusCode != http.StatusOK {
				errs <- fmt.Errorf("got status %d, want 200", resp.StatusCode)
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}

	var eventCount int
	if err := st.Pool().QueryRow(ctx,
		`SELECT count(*) FROM events WHERE event_id = $1`, eventID).Scan(&eventCount); err != nil {
		t.Fatalf("count events: %v", err)
	}
	if eventCount != 1 {
		t.Fatalf("stored %d events, want 1", eventCount)
	}

	stats, err := st.AccountStats(ctx, accountID)
	if err != nil {
		t.Fatalf("AccountStats: %v", err)
	}
	if stats.CallCount != 1 || stats.TotalDurationSec != 143 {
		t.Fatalf("got %+v, want CallCount=1 TotalDurationSec=143", stats)
	}
}
