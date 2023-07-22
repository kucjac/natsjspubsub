package natsjspubsub

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"gocloud.dev/gcerrors"
	"gocloud.dev/pubsub"
	"gocloud.dev/pubsub/batcher"
	"gocloud.dev/pubsub/driver"
)

const (
	noMessagesPollDuration = 250 * time.Millisecond
)

func init() {
	o := new(defaultUrlOpener)
	pubsub.DefaultURLMux().RegisterTopic(Scheme, o)
	pubsub.DefaultURLMux().RegisterSubscription(Scheme, o)
}

// defaultDialer dials a default NATS server based on the environment
// variable "NATS_SERVER_URL".
type defaultDialer struct {
	init sync.Once
	conn *nats.Conn
	err  error
}

func (o *defaultDialer) defaultConn() error {
	o.init.Do(func() {
		serverURL := os.Getenv("NATS_SERVER_URL")
		if serverURL == "" {
			o.err = errors.New("NATS_SERVER_URL environment variable not set")
			return
		}
		o.conn, o.err = nats.Connect(serverURL)
		if o.err != nil {
			o.err = fmt.Errorf("failed to dial NATS_SERVER_URL %q: %v", serverURL, o.err)
			return
		}
	})
	return o.err
}

type defaultUrlOpener struct {
	init sync.Once
	defaultDialer
	opener *URLOpener
	err    error
}

func (o *defaultUrlOpener) defaultOpener() (*URLOpener, error) {
	o.init.Do(func() {
		if err := o.defaultConn(); err != nil {
			o.err = err
			return
		}
		js, err := o.conn.JetStream()
		if err != nil {
			o.err = fmt.Errorf("failed to establish JetStream connection: %v", err)
		}
		o.opener = &URLOpener{JetStream: js}
	})
	return o.opener, o.err
}

// OpenTopicURL implements pubsub.TopicURLOpener.OpenTopicURL for the natsjs scheme.
func (o *defaultUrlOpener) OpenTopicURL(ctx context.Context, u *url.URL) (*pubsub.Topic, error) {
	opener, err := o.defaultOpener()
	if err != nil {
		return nil, fmt.Errorf("open topic %v: failed to open default connection: %v", u, err)
	}
	return opener.OpenTopicURL(ctx, u)
}

// OpenSubscriptionURL implements pubsub.SubscriptionURLOpener.OpenSubscriptionURL for the natsjs scheme.
func (o *defaultUrlOpener) OpenSubscriptionURL(ctx context.Context, u *url.URL) (*pubsub.Subscription, error) {
	opener, err := o.defaultOpener()
	if err != nil {
		return nil, fmt.Errorf("open subscription %v: failed to open default connection: %v", u, err)
	}
	return opener.OpenSubscriptionURL(ctx, u)
}

// Scheme is the URL scheme natspubsub registers its URLOpeners under on pubsub.DefaultMux.
const Scheme = "natsjs"

// URLOpener opens NATS URLs like "natsjs://mysubject".
//
// The URL host+path is used as the subject.
//
// No query parameters are supported.
type URLOpener struct {
	// JetStream is the nats jetstream context used for jetstream communication with the server.
	JetStream nats.JetStreamContext
	// TopicOptions specifies the options to pass to OpenTopic.
	TopicOptions TopicOptions
	// SubscriptionOptions specifies the options to pass to OpenPullSubscription.
	SubscriptionOptions SubscriptionOptions
}

// OpenTopicURL opens a pubsub.Topic based on u.
func (o *URLOpener) OpenTopicURL(ctx context.Context, u *url.URL) (*pubsub.Topic, error) {
	for param := range u.Query() {
		return nil, fmt.Errorf("open topic %v: invalid query parameter %s", u, param)
	}
	subject := path.Join(u.Host, u.Path)

	return OpenTopic(o.JetStream, subject, &o.TopicOptions)
}

// OpenSubscriptionURL opens a pubsub.Subscription based on u.
func (o *URLOpener) OpenSubscriptionURL(ctx context.Context, u *url.URL) (*pubsub.Subscription, error) {
	opts := o.SubscriptionOptions
	for param, values := range u.Query() {
		switch strings.ToLower(param) {
		case "ackwait":
			if len(values) != 1 {
				return nil, fmt.Errorf("open subscription %v: invalid ackwait", u)
			}
			d, err := time.ParseDuration(values[0])
			if err != nil {
				return nil, fmt.Errorf("open subscription %v: invalid ackwait %q: %v", u, values[0], err)
			}
			opts.AckWait = d
		case "durablename":
			if len(values) != 1 {
				return nil, fmt.Errorf("open subscription %v: invalid durable", u)
			}
			opts.DurableName = values[0]
		case "mode":
			if len(values) != 1 {
				return nil, fmt.Errorf("open subscription %v: invalid mode", u)
			}
			if err := opts.Mode.FromString(values[0]); err != nil {
				return nil, fmt.Errorf("open subscription %v: invalid mode %q: %v", u, values[0], err)
			}
		case "startat":
			if len(values) != 1 {
				return nil, fmt.Errorf("open subscription %v: invalid startat", u)
			}
			switch strings.ToLower(values[0]) {
			case "newonly":
				opts.StartAt = StartPositionNewOnly
			case "lastreceived":
				opts.StartAt = StartPositionLastReceived
			case "first":
				opts.StartAt = StartPositionFirst
			case "time":
				opts.StartAt = StartPositionTimeStart
			case "sequence":
				opts.StartAt = StartPositionSequenceStart
			default:
				return nil, fmt.Errorf("open subscription %v: invalid startat %q", u, values[0])
			}
		case "startsequence":
			if len(values) != 1 {
				return nil, fmt.Errorf("open subscription %v: invalid startsequence", u)
			}
			seq, err := strconv.ParseUint(values[0], 10, 64)
			if err != nil {
				return nil, fmt.Errorf("open subscription %v: invalid startsequence %q: %v", u, values[0], err)
			}
			opts.StartSequence = seq
		case "starttime":
			if len(values) != 1 {
				return nil, fmt.Errorf("open subscription %v: invalid starttime", u)
			}
			ts, err := time.Parse(time.RFC3339, values[0])
			if err != nil {
				return nil, fmt.Errorf("open subscription %v: invalid starttime %q: %v", u, values[0], err)
			}
			opts.StartTime = ts
		case "deliverysubject":
			if len(values) != 1 {
				return nil, fmt.Errorf("open subscription %v: invalid deliverysubject", u)
			}
			opts.PushOptions.DeliverySubject = values[0]
		case "deliverygroup":
			if len(values) != 1 {
				return nil, fmt.Errorf("open subscription %v: invalid deliverygroup", u)
			}
			opts.PushOptions.DeliveryGroup = values[0]
		case "idleheartbeat":
			if len(values) != 1 {
				return nil, fmt.Errorf("open subscription %v: invalid idleheartbeat", u)
			}
			d, err := time.ParseDuration(values[0])
			if err != nil {
				return nil, fmt.Errorf("open subscription %v: invalid idleheartbeat %q: %v", u, values[0], err)
			}
			opts.PushOptions.IdleHeartbeat = d
		case "ratelimit":
			if len(values) != 1 {
				return nil, fmt.Errorf("open subscription %v: invalid ratelimit", u)
			}
			rate, err := strconv.ParseUint(values[0], 10, 64)
			if err != nil {
				return nil, fmt.Errorf("open subscription %v: invalid ratelimit %q: %v", u, values[0], err)
			}
			opts.PushOptions.RateLimit = rate
		case "headersonly":
			if len(values) > 1 {
				return nil, fmt.Errorf("open subscription %v: invalid headersonly", u)
			}
			if len(values) == 0 {
				opts.PushOptions.HeadersOnly = true
				break
			}
			b, err := strconv.ParseBool(values[0])
			if err != nil {
				return nil, fmt.Errorf("open subscription %v: invalid headersonly %q: %v", u, values[0], err)
			}
			opts.PushOptions.HeadersOnly = b
		case "maxrequestbatch":
			if len(values) != 1 {
				return nil, fmt.Errorf("open subscription %v: invalid maxrequestbatch", u)
			}
			batch, err := strconv.ParseInt(values[0], 10, 64)
			if err != nil {
				return nil, fmt.Errorf("open subscription %v: invalid maxrequestbatch %q: %v", u, values[0], err)
			}
			opts.PullOptions.MaxRequestBatch = int(batch)
		case "bindstream":
			if len(values) > 1 {
				return nil, fmt.Errorf("open subscription %v: invalid bind_stream", u)
			}
			var stream string
			if len(values) == 1 {
				stream = values[0]
			}
			opts.BindStream = &BindStreamOption{Stream: stream}
		case "bind":
			// The bind query parameter is used to bind the subscription to a stream or consumer.
			// The stream is always the first value and the consumer is the second value.
			if len(values) != 2 {
				return nil, fmt.Errorf("open subscription %v: invalid bind", u)
			}
			// The bind query parameter is used to bind the subscription to a stream or consumer.
			opts.Bind = &BindOption{
				Stream:   values[0],
				Consumer: values[1],
			}
		default:
			return nil, fmt.Errorf("open subscription %v: invalid query parameter %s", u, param)
		}
	}

	subject := path.Join(u.Host, u.Path)
	switch opts.Mode {
	case SubscriptionModePull:
		return OpenPullSubscription(o.JetStream, subject, opts.DurableName, &opts)
	case SubscriptionModePush:
		return OpenPushSubscription(o.JetStream, subject, &opts)
	default:
		return nil, fmt.Errorf("open subscription %v: invalid mode %q", u, opts.Mode)
	}
}

type topic struct {
	isClosed atomic.Bool
	js       nats.JetStream
	subj     string
}

// OpenTopic returns a *pubsub.Topic for use with NATS.
// The subject is the NATS JetStream Subject.
func OpenTopic(js nats.JetStream, subject string, _ *TopicOptions) (*pubsub.Topic, error) {
	dt, err := openTopic(js, subject)
	if err != nil {
		return nil, err
	}
	return pubsub.NewTopic(dt, sendJSBatcherOpts), nil
}

// openTopic returns the driver for OpenTopic. This function exists so the test
// harness can get the driver interface implementation if it needs to.
func openTopic(nc nats.JetStream, subject string) (driver.Topic, error) {
	if nc == nil {
		return nil, errors.New("natspubsub: nats.JetStreamContext is required")
	}
	return &topic{js: nc, subj: subject}, nil
}

var errNotInitialized = errors.New("natspubsubjs: topic not initialized")

// SendBatch implements driver.Topic.SendBatch.
func (t *topic) SendBatch(ctx context.Context, msgs []*driver.Message) error {
	if t == nil || t.js == nil {
		return errNotInitialized
	}

	if t.isClosed.Load() {
		return errors.New("natspubsub: topic is closed")
	}

	for _, m := range msgs {
		if err := ctx.Err(); err != nil {
			return err
		}

		nm := encodeJSMessage(t.subj, m)
		if m.BeforeSend != nil {
			if err := m.BeforeSend(messageAsFunc(nm)); err != nil {
				return err
			}
		}

		ack, err := t.js.PublishMsg(nm, nats.Context(ctx))
		if err != nil {
			return err
		}

		if m.AfterSend != nil {
			asFunc := func(i any) bool {
				pubAck, ok := i.(**nats.PubAck)
				if !ok {
					return false
				}
				*pubAck = ack
				return true
			}
			if err = m.AfterSend(asFunc); err != nil {
				return err
			}
		}
	}
	return nil
}

// IsRetryable implements driver.Topic.IsRetryable.
func (*topic) IsRetryable(error) bool { return false }

// As implements driver.Topic.As.
func (t *topic) As(i any) bool {
	c, ok := i.(*nats.JetStream)
	if !ok {
		return false
	}
	*c = t.js
	return true
}

// ErrorAs implements driver.Topic.ErrorAs
func (*topic) ErrorAs(error, interface{}) bool {
	return false
}

// ErrorCode implements driver.Topic.ErrorCode
func (*topic) ErrorCode(err error) gcerrors.ErrorCode {
	switch err {
	case nil:
		return gcerrors.OK
	case context.Canceled:
		return gcerrors.Canceled
	case nats.ErrInvalidMsg:
		return gcerrors.InvalidArgument
	case errNotInitialized:
		return gcerrors.NotFound
	case nats.ErrBadSubject:
		return gcerrors.FailedPrecondition
	case nats.ErrAuthorization:
		return gcerrors.PermissionDenied
	case nats.ErrMaxPayload, nats.ErrReconnectBufExceeded:
		return gcerrors.ResourceExhausted
	}
	return gcerrors.Unknown
}

// Close implements driver.Topic.Close.
func (t *topic) Close() error {
	if t == nil || t.js == nil {
		return nil
	}

	if !t.isClosed.CompareAndSwap(false, true) {
		return errors.New("natspubsub: topic already closed")
	}

	return nil
}

type subscription struct {
	mode         SubscriptionMode
	sub          *nats.Subscription
	maxBatchPull int
}

var sendJSBatcherOpts = &batcher.Options{
	MaxHandlers:  10,
	MaxBatchSize: 1,
}

var recvBatcherOpts = &batcher.Options{
	MaxHandlers:  1,
	MaxBatchSize: 1,
}

var ackJSBatcherOpts = &batcher.Options{
	MaxHandlers:  1,
	MaxBatchSize: 1,
}

// OpenPullSubscription returns a based *pubsub.Subscription representing pull based NATS JetStream subscription.
func OpenPullSubscription(nc nats.JetStream, subject, durableName string, opts *SubscriptionOptions) (*pubsub.Subscription, error) {
	if opts == nil {
		opts = &SubscriptionOptions{}
	}
	opts.DurableName = durableName
	ds, err := openSubscription(nc, subject, opts)
	if err != nil {
		return nil, err
	}

	bo := recvBatcherOpts
	if opts.Mode == SubscriptionModePull {
		if opts.PullOptions.MaxRequestBatch == 0 {
			if bo == recvBatcherOpts {
				bo = &batcher.Options{}
				*bo = *recvBatcherOpts
			}

			bo.MaxBatchSize = opts.PullOptions.MaxRequestBatch
		}
		if opts.PullOptions.MaxRequestBytes == 0 {
			if bo == recvBatcherOpts {
				bo = &batcher.Options{}
				*bo = *recvBatcherOpts
			}
			bo.MaxBatchByteSize = opts.PullOptions.MaxRequestBytes
		}
	}
	return pubsub.NewSubscription(ds, bo, ackJSBatcherOpts), nil
}

// OpenPushSubscription creates a new jetstream push subscription that returns a driver.Subscription.
func OpenPushSubscription(nc nats.JetStream, subject string, opts *SubscriptionOptions) (*pubsub.Subscription, error) {
	if opts == nil {
		opts = &SubscriptionOptions{}
	}
	opts.Mode = SubscriptionModePush
	if err := opts.Validate(); err != nil {
		return nil, err
	}

	ds, err := openSubscription(nc, subject, opts)
	if err != nil {
		return nil, err
	}
	return pubsub.NewSubscription(ds, recvBatcherOpts, ackJSBatcherOpts), nil
}

func openSubscription(nc nats.JetStream, subject string, opts *SubscriptionOptions) (driver.Subscription, error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}

	s := &subscription{
		mode:         opts.Mode,
		maxBatchPull: 1,
	}
	if opts.PullOptions.MaxRequestBatch > 0 {
		s.maxBatchPull = opts.PullOptions.MaxRequestBatch
	}
	var err error
	switch opts.Mode {
	case SubscriptionModePull:
		s.sub, err = nc.PullSubscribe(subject, opts.DurableName, extractSubscriptionOptions(opts)...)
	case SubscriptionModePush:
		if opts.PushOptions.DeliveryGroup == "" {
			s.sub, err = nc.SubscribeSync(subject, extractSubscriptionOptions(opts)...)
		} else {
			s.sub, err = nc.QueueSubscribeSync(subject, opts.PushOptions.DeliveryGroup, extractSubscriptionOptions(opts)...)
		}
	}
	if err != nil {
		return nil, err
	}
	return s, nil
}

// ReceiveBatch implements driver.ReceiveBatch.
func (s *subscription) ReceiveBatch(ctx context.Context, maxMessages int) ([]*driver.Message, error) {
	if s == nil || s.sub == nil {
		return nil, nats.ErrBadSubscription
	}

	switch s.mode {
	case SubscriptionModePull:
		return s.receiveBatchPull(ctx, maxMessages)
	case SubscriptionModePush:
		return s.receiveBatchPush(ctx, maxMessages)
	default:
		return nil, fmt.Errorf("unknown subscription mode %q", s.mode)
	}
}

// SendAcks implements driver.Subscription.SendAcks.
func (s *subscription) SendAcks(ctx context.Context, ackIDs []driver.AckID) error {
	for _, ai := range ackIDs {
		msg, ok := ai.(*nats.Msg)
		if !ok {
			return fmt.Errorf("send acks - unexpected message ackId type: %T", ai)
		}
		if err := msg.Ack(nats.Context(ctx)); err != nil {
			if errors.Is(err, nats.ErrMsgAlreadyAckd) {
				continue
			}
			return err
		}
	}
	return nil
}

// CanNack implements driver.CanNack.
func (s *subscription) CanNack() bool {
	if s == nil {
		return false
	}
	return true
}

// SendNacks implements driver.Subscription.SendNacks. It should never be called
// because we return false for CanNack.
func (s *subscription) SendNacks(ctx context.Context, nackIDs []driver.AckID) error {
	for _, ai := range nackIDs {
		msg, ok := ai.(*nats.Msg)
		if !ok {
			return fmt.Errorf("send acks - unexpected message ackId type: %T", ai)
		}
		if err := msg.Nak(nats.Context(ctx)); err != nil {
			if errors.Is(err, nats.ErrMsgAlreadyAckd) {
				continue
			}
			return err
		}
	}
	return nil
}

// IsRetryable implements driver.Subscription.IsRetryable.
func (s *subscription) IsRetryable(error) bool { return false }

// As implements driver.Subscription.As.
func (s *subscription) As(i interface{}) bool {
	c, ok := i.(**nats.Subscription)
	if !ok {
		return false
	}
	*c = s.sub
	return true
}

// ErrorAs implements driver.Subscription.ErrorAs
func (*subscription) ErrorAs(error, interface{}) bool { return false }

// ErrorCode implements driver.Subscription.ErrorCode
func (*subscription) ErrorCode(err error) gcerrors.ErrorCode {
	switch err {
	case nil:
		return gcerrors.OK
	case context.Canceled:
		return gcerrors.Canceled
	case errNotInitialized, nats.ErrBadSubscription:
		return gcerrors.NotFound
	case nats.ErrBadSubject, nats.ErrTypeSubscription, nats.ErrHeadersNotSupported,
		nats.ErrJetStreamNotEnabled, nats.ErrBadHeaderMsg:
		return gcerrors.FailedPrecondition
	case nats.ErrAuthorization:
		return gcerrors.PermissionDenied
	case nats.ErrMaxMessages, nats.ErrSlowConsumer:
		return gcerrors.ResourceExhausted
	case nats.ErrTimeout:
		return gcerrors.DeadlineExceeded
	}
	return gcerrors.Unknown
}

// Close implements driver.Subscription.
func (s *subscription) Close() error {
	if s == nil {
		return nil
	}
	return nil
}

// Convert NATS msgs to *driver.Message.
func (s *subscription) decode(msg *nats.Msg) (*driver.Message, error) {
	if msg == nil {
		return nil, nats.ErrInvalidMsg
	}
	dm, err := decodeJSMessage(msg)
	if err != nil {
		return nil, err
	}
	dm.AckID = msg
	dm.AsFunc = messageAsFunc(msg)
	return dm, nil
}

func (s *subscription) receiveBatchPull(ctx context.Context, maxMessages int) ([]*driver.Message, error) {
	var cf context.CancelFunc
	ctx, cf = context.WithTimeout(ctx, noMessagesPollDuration)
	defer cf()

	if maxMessages > s.maxBatchPull {
		maxMessages = s.maxBatchPull
	}
	batch, err := s.sub.FetchBatch(maxMessages, nats.Context(ctx))
	if err != nil {
		if errors.Is(err, nats.ErrTimeout) {
			return nil, nil
		}
		if errors.Is(err, context.DeadlineExceeded) {
			return nil, nil
		}
		return nil, err
	}
	var ms []*driver.Message
	for {
		select {
		case <-ctx.Done():
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				return ms, nil
			}
			return nil, ctx.Err()
		case <-batch.Done():
			return ms, nil
		case msg, ok := <-batch.Messages():
			if !ok {
				return ms, nil
			}
			var dm *driver.Message
			dm, err = s.decode(msg)
			if err != nil {
				return nil, err
			}
			ms = append(ms, dm)
		}
	}
}

func (s *subscription) receiveBatchPush(ctx context.Context, _ int) ([]*driver.Message, error) {
	msg, err := s.sub.NextMsgWithContext(ctx)
	if err != nil {
		return nil, err
	}
	dm, err := s.decode(msg)
	if err != nil {
		return nil, err
	}
	time.Sleep(noMessagesPollDuration)
	return []*driver.Message{dm}, nil
}

func encodeJSMessage(sub string, m *driver.Message) *nats.Msg {
	var header nats.Header
	if m.Metadata != nil {
		header = nats.Header{}
		for k, v := range m.Metadata {
			header[url.QueryEscape(k)] = []string{url.QueryEscape(v)}
		}
	}
	nm := &nats.Msg{
		Subject: sub,
		Data:    m.Body,
		Header:  header,
	}
	return nm
}

// Convert NATS JetStream msgs to *driver.Message.
func decodeJSMessage(msg *nats.Msg) (*driver.Message, error) {
	if msg == nil {
		return nil, nats.ErrInvalidMsg
	}

	dm := driver.Message{
		AckID:  -1,
		AsFunc: messageAsFunc(msg),
		Body:   msg.Data,
	}
	meta, err := msg.Metadata()
	if err != nil {
		return nil, err
	}
	dm.LoggableID = strconv.FormatUint(meta.Sequence.Stream, 16)

	if msg.Header != nil {
		dm.Metadata = map[string]string{}
		for k, v := range msg.Header {
			var sv string
			if len(v) > 0 {
				sv = v[0]
			}
			var kb string
			kb, err = url.QueryUnescape(k)
			if err != nil {
				return nil, err
			}
			var vb string
			vb, err = url.QueryUnescape(sv)
			if err != nil {
				return nil, err
			}
			dm.Metadata[kb] = vb
		}
	}
	return &dm, nil
}

func extractSubscriptionOptions(o *SubscriptionOptions) (options []nats.SubOpt) {
	// ManualAck gives the pubsub.Subscription ack control of messages.
	options = append(options, nats.ManualAck())
	if o.AckWait != 0 {
		options = append(options, nats.AckWait(o.AckWait))
	}
	switch o.StartAt {
	case StartPositionSequenceStart:
		options = append(options, nats.StartSequence(o.StartSequence))
	case StartPositionFirst:
		options = append(options, nats.DeliverAll())
	case StartPositionLastReceived:
		options = append(options, nats.DeliverLast())
	case StartPositionTimeStart:
		options = append(options, nats.StartTime(o.StartTime))
	}

	if o.BindStream != nil {
		options = append(options, nats.BindStream(o.BindStream.Stream))
	}
	if o.Bind != nil {
		options = append(options, nats.Bind(o.Bind.Stream, o.Bind.Consumer))
	}

	if o.Mode == SubscriptionModePull {
		if o.PullOptions.MaxRequestBatch != 0 {
			options = append(options, nats.MaxRequestBatch(o.PullOptions.MaxRequestBatch))
		}
	}

	if o.Mode == SubscriptionModePush {
		if o.DurableName != "" {
			options = append(options, nats.Durable(o.DurableName))
		}
		if o.PushOptions.DeliverySubject != "" {
			options = append(options, nats.DeliverSubject(o.PushOptions.DeliverySubject))
		}
		if o.PushOptions.IdleHeartbeat != 0 {
			options = append(options, nats.IdleHeartbeat(o.PushOptions.IdleHeartbeat))
		}
		if o.PushOptions.RateLimit != 0 {
			options = append(options, nats.RateLimit(o.PushOptions.RateLimit))
		}
		if o.PushOptions.HeadersOnly {
			options = append(options, nats.HeadersOnly())
		}
	}
	return options
}

func messageAsFunc(msg *nats.Msg) func(interface{}) bool {
	return func(i interface{}) bool {
		p, ok := i.(**nats.Msg)
		if !ok {
			return false
		}
		*p = msg
		return true
	}
}
