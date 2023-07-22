package natsjspubsub

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

// SubscriptionMode is a mode of the subscription.
type SubscriptionMode int

const (
	// SubscriptionModePull is the default subscription mode which pulls messages from the server.
	SubscriptionModePull SubscriptionMode = iota
	// SubscriptionModePush is the subscription mode which relies on messages pushed to the subscriber.
	SubscriptionModePush
)

// String returns the string representation of the SubscriptionMode.
func (m SubscriptionMode) String() string {
	switch m {
	case SubscriptionModePull:
		return "pull"
	case SubscriptionModePush:
		return "push"
	}
	return "unknown"
}

// FromString returns the SubscriptionMode from the string.
func (m *SubscriptionMode) FromString(s string) error {
	switch strings.ToLower(s) {
	case "pull":
		*m = SubscriptionModePull
	case "push":
		*m = SubscriptionModePush
	default:
		return fmt.Errorf("unknown subscription mode %q", s)
	}
	return nil
}

// TopicOptions sets options for constructing a *pubsub.Topic backed by NATS.
type TopicOptions struct {
}

type (
	// SubscriptionOptions sets options for constructing a *pubsub.Subscription backed by NATS Streaming.
	// The options are a combination of common subscription configuration for
	// both pull and push subscriptions.
	// Only one of the PushOptions or PullOptions can be set, depending on the subscription Mode.
	SubscriptionOptions struct {
		// DurableName defines the durable subscription name.
		// For pull subscriptions in order to create an ephemeral subscription, set the DurableName to an empty string.
		DurableName string
		// AckWait defines the duration the cluster will wait for an ACK for a given message.
		AckWait time.Duration
		// StartPosition defines the position where the subscription would start.
		// By default, it sets the position to take only new messages.
		StartAt StartPosition
		// StartSequence is an option used together with the StartPositionSequenceStart
		// which defines what is the sequence start position.
		StartSequence uint64
		// StartTime is the option used together with the StartPositionTimeStart
		// which sets the desired start time position and state.
		StartTime time.Time
		// Mode defines the subscription mode.
		// By default it sets the mode to pull.
		Mode SubscriptionMode
		// BindStream binds a consumer to a stream explicitly based on a name.
		// When a stream name is not specified, the library uses the subscribe
		// subject as a way to find the stream name. It is done by making a request
		// to the server to get list of stream names that have a filter for this
		// subject. If the returned list contains a single stream, then this
		// stream name will be used, otherwise the `ErrNoMatchingStream` is returned.
		// To avoid the stream lookup, provide the stream name with this function.
		// See nats.BindStream for more information.
		BindStream *BindStreamOption
		// Bind binds a subscription to an existing consumer from a stream without attempting to create.
		// See nats.Bind for more information.
		Bind *BindOption
		// PullOptions defines the options for the pull subscription.
		PullOptions PullSubscriptionOptions
		// PushOptions defines the options for the push subscription.
		PushOptions PushSubscriptionOptions
	}
	// BindStreamOption is an option of SubscriptionOptions that defines the stream name to bind to.
	BindStreamOption struct {
		// Stream defines the name of a stream to bind to.
		// If the option is not nil and the stream name is empty, the
		// subscription subject is used to find the stream name.
		// See nats.BindStream for more information.
		Stream string
	}
	// BindOption is an option of SubscriptionOptions that defines the consumer name to bind to.
	BindOption struct {
		// Stream defines the name of a stream to bind to.
		Stream string
		// Consumer defines the name of a consumer to bind to.
		Consumer string
	}
)

// Validate checks if the subscription options are valid.
func (s *SubscriptionOptions) Validate() error {
	if s == nil {
		return nil
	}
	if s.StartSequence != 0 && s.StartAt != StartPositionSequenceStart {
		return errors.New("natsjspubsub: subscription option StartSequence should be paired with the StartAt = StartPositionSequenceStart")
	}
	if !s.StartTime.IsZero() && s.StartAt != StartPositionTimeStart {
		return errors.New("natsjspubsub: subscription option StartTime should be paired with the StartAt = StartPositionTimeDeltaStart")
	}

	if s.DurableName != "" {
		if strings.ContainsAny(s.DurableName, ".*>") {
			return errors.New("natsjspubsub: subscription option DurableName cannot contain '.', '*', '>'")
		}
	}

	if s.BindStream != nil {
		if s.BindStream.Stream != "" && strings.ContainsAny(s.BindStream.Stream, ".*>") {
			return errors.New("natsjspubsub: pull subscription option BindStream cannot contain '.', '*', '>'")
		}
	}
	if s.Bind != nil {
		if s.BindStream != nil {
			return errors.New("natsjspubsub: subscription options BindStream and Bind cannot be used together")
		}
		if s.Bind.Stream == "" || s.Bind.Consumer == "" {
			return errors.New("natsjspubsub: subscription option Bind requires both Stream and Consumer")
		}
		if strings.ContainsAny(s.Bind.Stream, ".*>") {
			return errors.New("natsjspubsub: subscription option Bind cannot contain '.', '*', '>'")
		}
		if strings.ContainsAny(s.Bind.Consumer, ".*>") {
			return errors.New("natsjspubsub: subscription option Bind cannot contain '.', '*', '>'")
		}
		if s.DurableName != "" && s.Bind.Consumer != s.DurableName {
			return errors.New("natsjspubsub: subscription option Bind Consumer should be the same as DurableName if it is provided")
		}
	}

	switch s.Mode {
	case SubscriptionModePull:
		if s.PushOptions != (PushSubscriptionOptions{}) {
			return errors.New("natsjspubsub: subscription option PushOptions is not allowed for pull subscription")
		}
		if err := s.PullOptions.validate(); err != nil {
			return err
		}
	case SubscriptionModePush:
		if s.PullOptions != (PullSubscriptionOptions{}) {
			return errors.New("natsjspubsub: subscription option PullOptions is not allowed for push subscription")
		}
		if err := s.PushOptions.validate(); err != nil {
			return err
		}
	default:
		return errors.New("natsjspubsub: subscription option Mode should be either JSSubscriptionModePull or JSSubscriptionModePush")
	}
	return nil
}

// PullSubscriptionOptions sets options for constructing a *pubsub.Subscription backed by NATS Streaming.
type PullSubscriptionOptions struct {
	// MaxRequestBatch is the maximum number of messages that will be delivered to the client in a single request.
	// This value must be smaller than the consumer's MaxRequestBatch value.
	MaxRequestBatch int
	// MaxRequestBytes is the maximum number of bytes that will be delivered to the client in a single request.
	MaxRequestBytes int
}

// Validate checks if the subscription options are valid.
func (s *PullSubscriptionOptions) validate() error {
	if s.MaxRequestBatch < 0 {
		return errors.New("natsjspubsub: subscription option MaxRequestBatch should be greater than 0")
	}
	if s.MaxRequestBytes < 0 {
		return errors.New("natsjspubsub: subscription option MaxRequestBytes should be greater than 0")
	}
	return nil
}

// PushSubscriptionOptions sets options for constructing a *pubsub.Subscription backed by NATS Streaming.
type PushSubscriptionOptions struct {
	// The subject to deliver messages to. With a deliver subject, the server will push messages to client
	// subscribed to this subject.
	DeliverySubject string
	// DeliveryGroup is the queue group name which, if specified, is then used to distribute the messages between
	// the subscribers to the consumer. This is analogous to a queue group in NATS core.
	DeliveryGroup string
	// IdleHeartbeat is the interval at which the server will send a heartbeat to the client if there are no messages
	// available to deliver.
	IdleHeartbeat time.Duration
	// RateLimit is used to throttle the delivery of messages to the consumer, in bits per second.
	RateLimit uint64
	// HeadersOnly indicates that only message headers should be delivered to the consumer.
	HeadersOnly bool
}

func (o *PushSubscriptionOptions) validate() error {
	if o.IdleHeartbeat < 0 {
		return errors.New("natsjspubsub: subscription option IdleHeartbeat should be greater than 0")
	}
	return nil
}

// StartWithLastReceived sets start position to last received.
func (s *SubscriptionOptions) StartWithLastReceived() {
	s.StartAt = StartPositionLastReceived
}

// DeliverAllAvailable sets the start subscription position to the first available message.
// This should result in delivery of all available messages.
func (s *SubscriptionOptions) DeliverAllAvailable() {
	s.StartAt = StartPositionFirst
}

// StartAtTime sets the desired start time position and state.
func (s *SubscriptionOptions) StartAtTime(ts time.Time) {
	s.StartAt = StartPositionTimeStart
	s.StartTime = ts
}

// StartAtSequence starts the subscription at given sequence number.
func (s *SubscriptionOptions) StartAtSequence(sequence uint64) {
	s.StartAt = StartPositionSequenceStart
	s.StartSequence = sequence
}

// StartPosition is the enum that defines start position of subscription.
type StartPosition int

const (
	// StartPositionNewOnly is the default subscription start position which delivers only new messages.
	StartPositionNewOnly StartPosition = 0
	// StartPositionLastReceived sets the subscription start position to the last received message.
	StartPositionLastReceived StartPosition = 1
	// StartPositionTimeStart  sets the subscription start position at given StartTime option.
	StartPositionTimeStart StartPosition = 2
	// StartPositionSequenceStart sets subscription start position to provided StartSequence option.
	StartPositionSequenceStart StartPosition = 3
	// StartPositionFirst sets subscription start position to the first available message, which results in delivery of all available messages..
	StartPositionFirst StartPosition = 4
)
