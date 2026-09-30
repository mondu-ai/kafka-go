package kafka

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/plain"
)

// The integration tests need the SASL listener to run with
// connections.max.reauth.ms=5000; they skip when the session is unbounded.
const (
	saslReauthTestAddr     = "127.0.0.1:9093"
	saslReauthTestLifetime = 5 * time.Second
)

func TestSaslReauthAt_UnboundedSession(t *testing.T) {
	for _, lifetime := range []time.Duration{0, -time.Second} {
		if at := nextSASLReauth(lifetime); at != 0 {
			t.Errorf("lifetime %v: expected 0, got %d", lifetime, at)
		}
	}

	if saslReauthDue(0) {
		t.Error("an unbounded session must never be due for re-authentication")
	}
}

func TestSaslReauthAt_Window(t *testing.T) {
	const lifetime = 10 * time.Second

	for range 100 {
		before := time.Since(saslReauthEpoch)
		at := time.Duration(nextSASLReauth(lifetime))
		after := time.Since(saslReauthEpoch)

		if at < before+8500*time.Millisecond || at > after+9500*time.Millisecond {
			t.Fatalf("re-authentication scheduled %v after now, want within 85%%-95%% of %v", at-before, lifetime)
		}
	}
}

func TestSaslReauthDue(t *testing.T) {
	now := int64(time.Since(saslReauthEpoch))

	if !saslReauthDue(now - int64(time.Millisecond)) {
		t.Error("a past re-authentication time must be due")
	}

	if saslReauthDue(now + int64(time.Hour)) {
		t.Error("a future re-authentication time must not be due")
	}
}

// Requests keep flowing on a single Conn past the broker's session lifetime,
// including with several goroutines pipelining requests on it.
func TestConnSASLReauthenticate(t *testing.T) {
	requireBoundedSASLSession(t)
	mech := newCountingPlainMechanism()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	conn, err := (&Dialer{SASLMechanism: mech}).DialContext(ctx, "tcp", saslReauthTestAddr)
	if err != nil {
		t.Fatal("failed to open a new kafka connection:", err)
	}
	defer conn.Close()

	deadline := time.Now().Add(2*saslReauthTestLifetime + time.Second)
	errs := make(chan error, 4)
	var wg sync.WaitGroup
	for range cap(errs) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for time.Now().Before(deadline) {
				conn.SetDeadline(time.Now().Add(5 * time.Second))
				if _, err := conn.Brokers(); err != nil {
					errs <- err
					return
				}
				time.Sleep(100 * time.Millisecond)
			}
		}()
	}
	wg.Wait()
	close(errs)

	for err := range errs {
		t.Fatalf("request failed on a connection older than the session lifetime: %v", err)
	}

	if n := mech.starts.Load(); n < 2 {
		t.Errorf("expected the connection to re-authenticate, the SASL exchange ran %d time(s)", n)
	}
}

// A pooled Transport connection keeps serving requests past the broker's
// session lifetime. ApiVersions is used because Metadata is answered from cache.
func TestTransportSASLReauthenticate(t *testing.T) {
	requireBoundedSASLSession(t)
	mech := newCountingPlainMechanism()
	transport := &Transport{SASL: mech, IdleTimeout: time.Minute}
	defer transport.CloseIdleConnections()

	client := &Client{Addr: TCP(saslReauthTestAddr), Transport: transport, Timeout: 5 * time.Second}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	deadline := time.Now().Add(2*saslReauthTestLifetime + time.Second)
	for time.Now().Before(deadline) {
		if _, err := client.ApiVersions(ctx, &ApiVersionsRequest{}); err != nil {
			t.Fatalf("request failed on a pooled connection older than the session lifetime: %v", err)
		}
		time.Sleep(100 * time.Millisecond)
	}

	if n := mech.starts.Load(); n < 2 {
		t.Errorf("expected the pooled connection to re-authenticate, the SASL exchange ran %d time(s)", n)
	}
}

// A consumer group member outlives the broker's session lifetime in the same
// generation: coordinator and fetch connections re-authenticate in place.
func TestReaderConsumerGroupSurvivesSASLReauthentication(t *testing.T) {
	requireBoundedSASLSession(t)
	mech := newCountingPlainMechanism()
	dialer := &Dialer{SASLMechanism: mech}
	transport := &Transport{SASL: mech}
	defer transport.CloseIdleConnections()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	topic := makeTopic()
	createTopicWithDialer(t, ctx, dialer, topic)
	defer deleteTopicWithDialer(t, dialer, topic)

	writer := &Writer{Addr: TCP(saslReauthTestAddr), Topic: topic, Transport: transport}
	defer writer.Close()

	if err := writer.WriteMessages(ctx, Message{Value: []byte("before")}); err != nil {
		t.Fatal("failed to write the first message:", err)
	}

	reader := NewReader(ReaderConfig{
		Brokers:           []string{saslReauthTestAddr},
		Dialer:            dialer,
		GroupID:           makeGroupID(),
		Topic:             topic,
		HeartbeatInterval: time.Second,
		Logger:            newTestKafkaLogger(t, "reader"),
		ErrorLogger:       newTestKafkaLogger(t, "reader-error"),
		MinBytes:          1,
		MaxBytes:          10e6,
		MaxWait:           time.Second,
	})
	defer reader.Close()

	first, err := reader.FetchMessage(ctx)
	if err != nil {
		t.Fatal("failed to fetch the first message:", err)
	}

	// Reset the counters: only what happens past the session lifetime matters below.
	reader.Stats()

	time.Sleep(2*saslReauthTestLifetime + time.Second)

	if err := reader.CommitMessages(ctx, first); err != nil {
		t.Fatal("commit failed after the session lifetime:", err)
	}

	if err := writer.WriteMessages(ctx, Message{Value: []byte("after")}); err != nil {
		t.Fatal("failed to write the second message:", err)
	}

	second, err := reader.FetchMessage(ctx)
	if err != nil {
		t.Fatal("failed to fetch after the session lifetime:", err)
	}
	if string(second.Value) != "after" {
		t.Fatalf("expected the second message, got %q at offset %d", second.Value, second.Offset)
	}

	stats := reader.Stats()
	if stats.Rebalances != 0 {
		t.Errorf("expected the member to stay in its generation, got %d rebalances", stats.Rebalances)
	}
	if stats.Errors != 0 {
		t.Errorf("expected no reader errors, got %d", stats.Errors)
	}
}

// countingMechanism counts SASL exchanges so tests can tell a re-authentication
// from the initial one, and can make the next exchange fail once.
type countingMechanism struct {
	sasl.Mechanism
	starts   atomic.Int32
	failNext atomic.Bool
}

func (m *countingMechanism) Start(ctx context.Context) (sasl.StateMachine, []byte, error) {
	m.starts.Add(1)
	if m.failNext.CompareAndSwap(true, false) {
		return nil, nil, errors.New("token source unavailable")
	}
	return m.Mechanism.Start(ctx)
}

func newCountingPlainMechanism() *countingMechanism {
	return &countingMechanism{Mechanism: plain.Mechanism{Username: "adminplain", Password: "admin-secret"}}
}

// requireBoundedSASLSession skips the test when the broker does not announce a
// session lifetime, since nothing would ever trigger a re-authentication.
func requireBoundedSASLSession(t *testing.T) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, err := (&Dialer{SASLMechanism: newCountingPlainMechanism()}).DialContext(ctx, "tcp", saslReauthTestAddr)
	if err != nil {
		t.Fatal("failed to open a new kafka connection:", err)
	}
	defer conn.Close()

	if conn.saslReauthAt.Load() == 0 {
		t.Skip("broker leaves SASL sessions unbounded (connections.max.reauth.ms unset)")
	}
}

func createTopicWithDialer(t *testing.T, ctx context.Context, dialer *Dialer, topic string) {
	t.Helper()

	conn, err := dialer.DialContext(ctx, "tcp", saslReauthTestAddr)
	if err != nil {
		t.Fatal("createTopicWithDialer, dial:", err)
	}
	defer conn.Close()

	conn.SetDeadline(time.Now().Add(10 * time.Second))
	if err := conn.CreateTopics(TopicConfig{Topic: topic, NumPartitions: 1, ReplicationFactor: 1}); err != nil {
		t.Fatal("createTopicWithDialer, create:", err)
	}

	for {
		partitions, err := conn.ReadPartitions(topic)
		if err == nil && len(partitions) == 1 && partitions[0].Leader.Host != "" {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("createTopicWithDialer, no leader for %s: %v", topic, err)
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func deleteTopicWithDialer(t *testing.T, dialer *Dialer, topic string) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, err := dialer.DialContext(ctx, "tcp", saslReauthTestAddr)
	if err != nil {
		t.Fatal("deleteTopicWithDialer, dial:", err)
	}
	defer conn.Close()

	conn.SetDeadline(time.Now().Add(10 * time.Second))
	if err := conn.DeleteTopics(topic); err != nil {
		t.Fatal("deleteTopicWithDialer, delete:", err)
	}
}

// A failed re-authentication closes the connection and reports the failure to
// the caller instead of leaving a connection the broker is about to drop.
func TestConnSASLReauthenticate_Failure(t *testing.T) {
	requireBoundedSASLSession(t)
	mech := newCountingPlainMechanism()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	conn, err := (&Dialer{SASLMechanism: mech}).DialContext(ctx, "tcp", saslReauthTestAddr)
	if err != nil {
		t.Fatal("failed to open a new kafka connection:", err)
	}
	defer conn.Close()

	mech.failNext.Store(true)
	time.Sleep(saslReauthTestLifetime)

	conn.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := conn.Brokers(); err == nil || !strings.Contains(err.Error(), "SASL re-authentication failed") {
		t.Fatalf("expected the failed re-authentication to be reported, got %v", err)
	}

	if _, err := conn.Brokers(); err == nil {
		t.Fatal("expected the connection to be closed after a failed re-authentication")
	}

	if n := mech.starts.Load(); n != 2 {
		t.Errorf("expected one dial and one failed re-authentication, the SASL exchange ran %d time(s)", n)
	}
}

// A partition reader whose connection fails to re-authenticate reconnects and
// keeps reading.
func TestReaderReconnectsAfterFailedSASLReauthentication(t *testing.T) {
	requireBoundedSASLSession(t)
	mech := newCountingPlainMechanism()
	dialer := &Dialer{SASLMechanism: mech}
	transport := &Transport{SASL: newCountingPlainMechanism()}
	defer transport.CloseIdleConnections()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	topic := makeTopic()
	createTopicWithDialer(t, ctx, dialer, topic)
	defer deleteTopicWithDialer(t, dialer, topic)

	writer := &Writer{Addr: TCP(saslReauthTestAddr), Topic: topic, Transport: transport}
	defer writer.Close()

	if err := writer.WriteMessages(ctx, Message{Value: []byte("before")}); err != nil {
		t.Fatal("failed to write the first message:", err)
	}

	reader := NewReader(ReaderConfig{
		Brokers:   []string{saslReauthTestAddr},
		Dialer:    dialer,
		Topic:     topic,
		Partition: 0,
		MinBytes:  1,
		MaxBytes:  10e6,
		MaxWait:   time.Second,
	})
	defer reader.Close()

	if _, err := reader.ReadMessage(ctx); err != nil {
		t.Fatal("failed to read the first message:", err)
	}
	reader.Stats()

	mech.failNext.Store(true)
	time.Sleep(saslReauthTestLifetime + time.Second)

	if err := writer.WriteMessages(ctx, Message{Value: []byte("after")}); err != nil {
		t.Fatal("failed to write the second message:", err)
	}

	msg, err := reader.ReadMessage(ctx)
	if err != nil {
		t.Fatal("failed to read after the failed re-authentication:", err)
	}
	if string(msg.Value) != "after" {
		t.Fatalf("expected the second message, got %q", msg.Value)
	}

	if mech.failNext.Load() {
		t.Error("expected a re-authentication to be attempted")
	}
	if stats := reader.Stats(); stats.Dials == 0 {
		t.Error("expected the reader to reconnect after the failed re-authentication")
	}
}

// A re-authentication that comes due while a Batch is still being read waits
// for the batch to close instead of interleaving with its bytes.
func TestConnSASLReauthenticate_OpenBatch(t *testing.T) {
	requireBoundedSASLSession(t)
	mech := newCountingPlainMechanism()
	dialer := &Dialer{SASLMechanism: mech}
	transport := &Transport{SASL: newCountingPlainMechanism()}
	defer transport.CloseIdleConnections()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	topic := makeTopic()
	createTopicWithDialer(t, ctx, dialer, topic)
	defer deleteTopicWithDialer(t, dialer, topic)

	writer := &Writer{Addr: TCP(saslReauthTestAddr), Topic: topic, Transport: transport}
	if err := writer.WriteMessages(ctx, Message{Value: []byte("held")}); err != nil {
		t.Fatal("failed to write the message:", err)
	}
	writer.Close()

	conn, err := dialer.DialLeader(ctx, "tcp", saslReauthTestAddr, topic, 0)
	if err != nil {
		t.Fatal("failed to dial the partition leader:", err)
	}
	defer conn.Close()

	conn.SetDeadline(time.Now().Add(30 * time.Second))
	batch := conn.ReadBatch(1, 10e6)
	if msg, err := batch.ReadMessage(); err != nil || string(msg.Value) != "held" {
		t.Fatalf("expected to read the message from the open batch, got %q, %v", msg.Value, err)
	}
	startsBefore := mech.starts.Load()

	time.Sleep(saslReauthTestLifetime)

	done := make(chan error, 1)
	go func() {
		_, err := conn.Brokers()
		done <- err
	}()

	select {
	case err := <-done:
		t.Fatalf("a request completed while the batch was still open: %v", err)
	case <-time.After(500 * time.Millisecond):
	}

	if err := batch.Close(); err != nil {
		t.Fatal("failed to close the batch:", err)
	}

	if err := <-done; err != nil {
		t.Fatalf("request after the batch closed failed: %v", err)
	}

	if n := mech.starts.Load(); n != startsBefore+1 {
		t.Errorf("expected exactly one re-authentication after the batch closed, the SASL exchange ran %d more time(s)", n-startsBefore)
	}
}
