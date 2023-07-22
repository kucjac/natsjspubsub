// Copyright 2019 The Go Cloud Development Kit Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package natsjspubsub

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/nats-io/nats.go"
	"gocloud.dev/gcerrors"
	"gocloud.dev/pubsub"
	"gocloud.dev/pubsub/driver"
	"gocloud.dev/pubsub/drivertest"

	"github.com/nats-io/nats-server/v2/server"
	gnatsd "github.com/nats-io/nats-server/v2/test"
)

const (
	benchPort = 9222
)

type harness struct {
	s    *server.Server
	nc   *nats.Conn
	js   nats.JetStreamContext
	mode SubscriptionMode
}

const (
	testPort = 11223
)

var streamTopicNameReplacer = strings.NewReplacer("/", "")

func newHarness(ctx context.Context, t *testing.T) (drivertest.Harness, error) {
	opts := gnatsd.DefaultTestOptions
	opts.Port = testPort
	opts.NoLog = false
	opts.JetStream = true
	opts.Debug = true
	opts.Trace = true
	s := gnatsd.RunServer(&opts)

	nc, err := nats.Connect(fmt.Sprintf("nats://127.0.0.1:%d", testPort))
	if err != nil {
		return nil, err
	}

	js, err := nc.JetStream()
	if err != nil {
		return nil, err
	}

	streamName := streamTopicNameReplacer.Replace(t.Name())
	_, err = js.AddStream(&nats.StreamConfig{Name: streamName, Subjects: []string{streamName}, Storage: nats.MemoryStorage})
	if err != nil {
		t.Logf("creating stream failed: %v - %s", err, streamName)
		return nil, err
	}
	_, err = js.AddConsumer(streamName, &nats.ConsumerConfig{
		Durable:         streamName,
		MaxRequestBatch: 10,
	})

	return &harness{s: s, js: js}, nil
}

func newHarnessPush(ctx context.Context, t *testing.T) (drivertest.Harness, error) {
	opts := gnatsd.DefaultTestOptions
	opts.Port = testPort
	opts.NoLog = false
	opts.JetStream = true
	opts.Debug = true
	opts.Trace = true
	s := gnatsd.RunServer(&opts)

	nc, err := nats.Connect(fmt.Sprintf("nats://127.0.0.1:%d", testPort))
	if err != nil {
		return nil, err
	}

	js, err := nc.JetStream()
	if err != nil {
		return nil, err
	}

	streamName := streamTopicNameReplacer.Replace(t.Name())
	_, err = js.AddStream(&nats.StreamConfig{Name: streamName, Subjects: []string{streamName}, Storage: nats.MemoryStorage})
	if err != nil {
		t.Logf("creating stream failed: %v - %s", err, streamName)
		return nil, err
	}

	return &harness{s: s, js: js, mode: SubscriptionModePush}, nil
}

func (h *harness) CreateTopic(ctx context.Context, testName string) (driver.Topic, func(), error) {
	cleanup := func() {}
	dt, err := openTopic(h.js, streamTopicNameReplacer.Replace(testName))
	if err != nil {
		return nil, nil, err
	}
	return dt, cleanup, nil
}

func (h *harness) MakeNonexistentTopic(ctx context.Context) (driver.Topic, error) {
	// A nil *topic behaves like a nonexistent topic.
	return (*topic)(nil), nil
}

func (h *harness) CreateSubscription(ctx context.Context, dt driver.Topic, testName string) (driver.Subscription, func(), error) {
	streamName := streamTopicNameReplacer.Replace(testName)
	ds, err := openSubscription(h.js, streamName, &SubscriptionOptions{
		Mode: h.mode,
	})
	if err != nil {
		return nil, nil, err
	}

	cleanup := func() {
		var sub *nats.Subscription
		if ds.As(&sub) {
			sub.Unsubscribe()
		}
	}
	return ds, cleanup, nil
}

func (h *harness) CreateDeliveryGroupSubscription(ctx context.Context, dt driver.Topic, testName string) (driver.Subscription, func(), error) {
	ds, err := openSubscription(h.js, streamTopicNameReplacer.Replace(testName), &SubscriptionOptions{
		PushOptions: PushSubscriptionOptions{
			DeliveryGroup: testName,
		},
		Mode: SubscriptionModePush,
	})
	if err != nil {
		return nil, nil, err
	}
	cleanup := func() {
		var sub *nats.Subscription
		if ds.As(&sub) {
			sub.Unsubscribe()
		}
	}
	return ds, cleanup, nil
}

func (h *harness) MakeNonexistentSubscription(ctx context.Context) (driver.Subscription, func(), error) {
	return (*subscription)(nil), func() {}, nil
}

func (h *harness) Close() {
	h.nc.Close()
	h.s.Shutdown()
}

func (h *harness) MaxBatchSizes() (int, int) { return 1, 1 }

func (*harness) SupportsMultipleSubscriptions() bool { return true }

type natsPullAsTest struct{}

func (natsPullAsTest) Name() string {
	return "nats:js test"
}

func (natsPullAsTest) TopicCheck(topic *pubsub.Topic) error {
	var c2 nats.JetStream
	if !topic.As(&c2) {
		return fmt.Errorf("cast succeeded for %T, want failure", &c2)
	}
	var c3 *nats.JetStream
	if topic.As(&c3) {
		return fmt.Errorf("cast failed for %T", &c3)
	}
	return nil
}

func (natsPullAsTest) SubscriptionCheck(sub *pubsub.Subscription) error {
	var c2 nats.Subscription
	if sub.As(&c2) {
		return fmt.Errorf("cast succeeded for %T, want failure", &c2)
	}
	var c3 *nats.Subscription
	if !sub.As(&c3) {
		return fmt.Errorf("cast failed for %T", &c3)
	}
	return nil
}

func (natsPullAsTest) TopicErrorCheck(t *pubsub.Topic, err error) error {
	var dummy string
	if t.ErrorAs(err, &dummy) {
		return fmt.Errorf("cast succeeded for %T, want failure", &dummy)
	}
	return nil
}

func (natsPullAsTest) SubscriptionErrorCheck(s *pubsub.Subscription, err error) error {
	var dummy string
	if s.ErrorAs(err, &dummy) {
		return fmt.Errorf("cast succeeded for %T, want failure", &dummy)
	}
	return nil
}

func (natsPullAsTest) MessageCheck(m *pubsub.Message) error {
	var pm nats.Msg
	if m.As(&pm) {
		return fmt.Errorf("cast succeeded for %T, want failure", &pm)
	}
	var ppm *nats.Msg
	if !m.As(&ppm) {
		return fmt.Errorf("cast failed for %T", &ppm)
	}
	return nil
}

func (natsPullAsTest) BeforeSend(as func(interface{}) bool) error {
	var pm *nats.Msg
	if !as(&pm) {
		return fmt.Errorf("cast failed for %T", &pm)
	}
	return nil
}

func (natsPullAsTest) AfterSend(as func(interface{}) bool) error {
	var ack *nats.PubAck
	if !as(&ack) {
		return fmt.Errorf("cast failed for %T", &ack)
	}
	return nil
}

func TestConformancePull(t *testing.T) {
	asTests := []drivertest.AsTest{natsPullAsTest{}}
	drivertest.RunConformanceTests(t, newHarness, asTests)
}

func TestConformancePush(t *testing.T) {
	asTests := []drivertest.AsTest{natsPullAsTest{}}
	drivertest.RunConformanceTests(t, newHarnessPush, asTests)
}

// These are natspubsub specific to increase coverage.

// If we only send a body we should be able to get that from a direct NATS subscriber.
func TestInteropWithDirectNATS(t *testing.T) {
	ctx := context.Background()
	dh, err := newHarness(ctx, t)
	if err != nil {
		t.Fatal(err)
	}
	defer dh.Close()
	conn := dh.(*harness).js

	const topic = "foo"
	body := []byte("hello")

	js := dh.(*harness).js

	const consumer = "consumer"

	js.AddStream(&nats.StreamConfig{Name: topic, Subjects: []string{topic}})

	// Send a message using Go CDK and receive it using NATS directly.
	pt, err := OpenTopic(conn, topic, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer pt.Shutdown(ctx)
	nsub, _ := conn.SubscribeSync(topic)
	if err = pt.Send(ctx, &pubsub.Message{Body: body}); err != nil {
		t.Fatal(err)
	}
	m, err := nsub.NextMsgWithContext(ctx)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if !bytes.Equal(m.Data, body) {
		t.Fatalf("Data did not match. %q vs %q\n", m.Data, body)
	}

	// Send a message using NATS JetStream directly and receive it using Go CDK.
	ps, err := OpenPullSubscription(conn, topic, consumer, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ps.Shutdown(ctx)
	_, err = conn.Publish(topic, body)
	if err != nil {
		t.Fatal(err)
	}
	msg, err := ps.Receive(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer msg.Ack()
	if !bytes.Equal(msg.Body, body) {
		t.Fatalf("Data did not match. %q vs %q\n", m.Data, body)
	}
}

func TestErrorCode(t *testing.T) {
	ctx := context.Background()
	dh, err := newHarness(ctx, t)
	if err != nil {
		t.Fatal(err)
	}
	defer dh.Close()
	h := dh.(*harness)

	// Topics
	const subject = "bar"

	if err = createStreamIfNotExists(h.js, subject); err != nil {
		t.Fatalf("creating stream failed: %v", err)
	}

	dt, err := openTopic(h.js, subject)
	if err != nil {
		t.Fatal(err)
	}

	if gce := dt.ErrorCode(nil); gce != gcerrors.OK {
		t.Fatalf("Expected %v, got %v", gcerrors.OK, gce)
	}
	if gce := dt.ErrorCode(context.Canceled); gce != gcerrors.Canceled {
		t.Fatalf("Expected %v, got %v", gcerrors.Canceled, gce)
	}
	if gce := dt.ErrorCode(nats.ErrBadSubject); gce != gcerrors.FailedPrecondition {
		t.Fatalf("Expected %v, got %v", gcerrors.FailedPrecondition, gce)
	}
	if gce := dt.ErrorCode(nats.ErrAuthorization); gce != gcerrors.PermissionDenied {
		t.Fatalf("Expected %v, got %v", gcerrors.PermissionDenied, gce)
	}
	if gce := dt.ErrorCode(nats.ErrMaxPayload); gce != gcerrors.ResourceExhausted {
		t.Fatalf("Expected %v, got %v", gcerrors.ResourceExhausted, gce)
	}
	if gce := dt.ErrorCode(nats.ErrReconnectBufExceeded); gce != gcerrors.ResourceExhausted {
		t.Fatalf("Expected %v, got %v", gcerrors.ResourceExhausted, gce)
	}

	// Subscriptions
	ds, err := openSubscription(h.js, subject, &SubscriptionOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if gce := ds.ErrorCode(nil); gce != gcerrors.OK {
		t.Fatalf("Expected %v, got %v", gcerrors.OK, gce)
	}
	if gce := ds.ErrorCode(context.Canceled); gce != gcerrors.Canceled {
		t.Fatalf("Expected %v, got %v", gcerrors.Canceled, gce)
	}
	if gce := ds.ErrorCode(nats.ErrBadSubject); gce != gcerrors.FailedPrecondition {
		t.Fatalf("Expected %v, got %v", gcerrors.FailedPrecondition, gce)
	}
	if gce := ds.ErrorCode(nats.ErrBadSubscription); gce != gcerrors.NotFound {
		t.Fatalf("Expected %v, got %v", gcerrors.NotFound, gce)
	}
	if gce := ds.ErrorCode(nats.ErrTypeSubscription); gce != gcerrors.FailedPrecondition {
		t.Fatalf("Expected %v, got %v", gcerrors.FailedPrecondition, gce)
	}
	if gce := ds.ErrorCode(nats.ErrAuthorization); gce != gcerrors.PermissionDenied {
		t.Fatalf("Expected %v, got %v", gcerrors.PermissionDenied, gce)
	}
	if gce := ds.ErrorCode(nats.ErrMaxMessages); gce != gcerrors.ResourceExhausted {
		t.Fatalf("Expected %v, got %v", gcerrors.ResourceExhausted, gce)
	}
	if gce := ds.ErrorCode(nats.ErrSlowConsumer); gce != gcerrors.ResourceExhausted {
		t.Fatalf("Expected %v, got %v", gcerrors.ResourceExhausted, gce)
	}
	if gce := ds.ErrorCode(nats.ErrTimeout); gce != gcerrors.DeadlineExceeded {
		t.Fatalf("Expected %v, got %v", gcerrors.DeadlineExceeded, gce)
	}

	// Queue Subscription
	qs, err := openSubscription(h.js, subject, &SubscriptionOptions{PushOptions: PushSubscriptionOptions{DeliveryGroup: t.Name()}, Mode: SubscriptionModePush})
	if err != nil {
		t.Fatal(err)
	}
	if gce := qs.ErrorCode(nil); gce != gcerrors.OK {
		t.Fatalf("Expected %v, got %v", gcerrors.OK, gce)
	}
	if gce := qs.ErrorCode(context.Canceled); gce != gcerrors.Canceled {
		t.Fatalf("Expected %v, got %v", gcerrors.Canceled, gce)
	}
	if gce := qs.ErrorCode(nats.ErrBadSubject); gce != gcerrors.FailedPrecondition {
		t.Fatalf("Expected %v, got %v", gcerrors.FailedPrecondition, gce)
	}
	if gce := qs.ErrorCode(nats.ErrBadSubscription); gce != gcerrors.NotFound {
		t.Fatalf("Expected %v, got %v", gcerrors.NotFound, gce)
	}
	if gce := qs.ErrorCode(nats.ErrTypeSubscription); gce != gcerrors.FailedPrecondition {
		t.Fatalf("Expected %v, got %v", gcerrors.FailedPrecondition, gce)
	}
	if gce := qs.ErrorCode(nats.ErrAuthorization); gce != gcerrors.PermissionDenied {
		t.Fatalf("Expected %v, got %v", gcerrors.PermissionDenied, gce)
	}
	if gce := qs.ErrorCode(nats.ErrMaxMessages); gce != gcerrors.ResourceExhausted {
		t.Fatalf("Expected %v, got %v", gcerrors.ResourceExhausted, gce)
	}
	if gce := qs.ErrorCode(nats.ErrSlowConsumer); gce != gcerrors.ResourceExhausted {
		t.Fatalf("Expected %v, got %v", gcerrors.ResourceExhausted, gce)
	}
	if gce := qs.ErrorCode(nats.ErrTimeout); gce != gcerrors.DeadlineExceeded {
		t.Fatalf("Expected %v, got %v", gcerrors.DeadlineExceeded, gce)
	}
}

func BenchmarkNatsPubSub(b *testing.B) {
	ctx := context.Background()

	opts := gnatsd.DefaultTestOptions
	opts.Port = benchPort
	s := gnatsd.RunServer(&opts)
	defer s.Shutdown()

	nc, err := nats.Connect(fmt.Sprintf("nats://127.0.0.1:%d", benchPort))
	if err != nil {
		b.Fatal(err)
	}
	defer nc.Close()

	js, err := nc.JetStream()

	h := &harness{s: s, js: js}
	dt, cleanup, err := h.CreateTopic(ctx, b.Name())
	if err != nil {
		b.Fatal(err)
	}
	defer cleanup()
	ds, cleanup, err := h.CreateSubscription(ctx, dt, b.Name())
	if err != nil {
		b.Fatal(err)
	}
	defer cleanup()

	topic := pubsub.NewTopic(dt, nil)
	defer topic.Shutdown(ctx)
	sub := pubsub.NewSubscription(ds, recvBatcherOpts, nil)
	defer sub.Shutdown(ctx)

	drivertest.RunBenchmarks(b, topic, sub)
}

func fakeConnectionStringInEnv() func() {
	oldEnvVal := os.Getenv("NATS_SERVER_URL")
	os.Setenv("NATS_SERVER_URL", fmt.Sprintf("nats://localhost:%d", testPort))
	return func() {
		os.Setenv("NATS_SERVER_URL", oldEnvVal)
	}
}

func TestOpenTopicFromURL(t *testing.T) {
	ctx := context.Background()
	dh, err := newHarness(ctx, t)
	if err != nil {
		t.Fatal(err)
	}
	defer dh.Close()

	cleanup := fakeConnectionStringInEnv()
	defer cleanup()

	tests := []struct {
		URL     string
		WantErr bool
	}{
		// OK.
		{"natsjs://mytopic", false},
		// Invalid parameter.
		{"natsjs://mytopic?param=value", true},
	}

	for _, test := range tests {
		topic, err := pubsub.OpenTopic(ctx, test.URL)
		if (err != nil) != test.WantErr {
			t.Errorf("%s: got error %v, want error %v", test.URL, err, test.WantErr)
		}
		if topic != nil {
			topic.Shutdown(ctx)
		}
	}
}

func TestOpenSubscriptionFromURL(t *testing.T) {
	ctx := context.Background()
	dh, err := newHarness(ctx, t)
	if err != nil {
		t.Fatal(err)
	}
	defer dh.Close()

	cleanup := fakeConnectionStringInEnv()
	defer cleanup()

	tests := []struct {
		URL     string
		WantErr bool
	}{
		// OK.
		{URL: "natsjs://mytopic"},
		// Invalid parameter.
		{URL: "natsjs://mytopic?param=value", WantErr: true},
		// DurableName URL Parameter.
		{URL: "natsjs://mytopic?durablename=durable1"},
	}

	if err = createStreamIfNotExists(dh.(*harness).js, "mytopic"); err != nil {
		t.Fatalf("creating stream failed: %v", err)
	}
	for _, test := range tests {
		sub, err := pubsub.OpenSubscription(ctx, test.URL)
		if (err != nil) != test.WantErr {
			t.Errorf("%s: got error %v, want error %v", test.URL, err, test.WantErr)
		}
		if sub != nil {
			sub.Shutdown(ctx)
		}
	}
}

func createStreamIfNotExists(js nats.JetStreamContext, testName string) error {
	_, err := js.StreamInfo(testName)
	if err != nil {
		if !errors.Is(err, nats.ErrStreamNotFound) {
			return err
		}
		_, err = js.AddStream(&nats.StreamConfig{Name: testName, Subjects: []string{testName}, Storage: nats.MemoryStorage})
		if err != nil {
			return err
		}
	}
	return nil
}
