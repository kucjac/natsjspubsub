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

package natsjspubsub_test

import (
	"context"
	"log"

	"github.com/nats-io/nats.go"
	"gocloud.dev/pubsub"

	"github.com/kucjac/natsjspubsub"
)

func ExampleOpenTopic() {
	// PRAGMA: This example is used on gocloud.dev; PRAGMA comments adjust how it is shown and can be ignored.
	// PRAGMA: On gocloud.dev, hide lines until the next blank line.
	ctx := context.Background()

	natsConn, err := nats.Connect("nats://nats.example.com")
	if err != nil {
		log.Fatal(err)
	}
	defer natsConn.Close()

	js, err := natsConn.JetStream()
	if err != nil {
		log.Fatal(err)
	}

	topic, err := natsjspubsub.OpenTopic(js, "example.mysubject", nil)
	if err != nil {
		log.Fatal(err)
	}
	defer topic.Shutdown(ctx)
}

func ExampleOpenPullSubscription() {
	// PRAGMA: This example is used on gocloud.dev; PRAGMA comments adjust how it is shown and can be ignored.
	// PRAGMA: On gocloud.dev, hide lines until the next blank line.
	ctx := context.Background()

	natsConn, err := nats.Connect("nats://nats.example.com")
	if err != nil {
		log.Fatal(err)
	}
	defer natsConn.Close()

	js, err := natsConn.JetStream()
	if err != nil {
		log.Fatal(err)
	}

	subscription, err := natsjspubsub.OpenPullSubscription(
		js,
		"example.mysubject",
		"durableName",
		nil)
	if err != nil {
		log.Fatal(err)
	}
	defer subscription.Shutdown(ctx)
}

func ExampleOpenPushSubscription() {
	// PRAGMA: This example is used on gocloud.dev; PRAGMA comments adjust how it is shown and can be ignored.
	// PRAGMA: On gocloud.dev, hide lines until the next blank line.
	ctx := context.Background()

	natsConn, err := nats.Connect("nats://nats.example.com")
	if err != nil {
		log.Fatal(err)
	}
	defer natsConn.Close()

	js, err := natsConn.JetStream()
	if err != nil {
		log.Fatal(err)
	}

	subscription, err := natsjspubsub.OpenPushSubscription(
		js,
		"example.mysubject",
		nil)
	if err != nil {
		log.Fatal(err)
	}
	defer subscription.Shutdown(ctx)
}

func Example_openTopicFromURL() {
	// PRAGMA: This example is used on gocloud.dev; PRAGMA comments adjust how it is shown and can be ignored.
	// PRAGMA: On gocloud.dev, add a blank import: _ "gocloud.dev/pubsub/natsjspubsub"
	// PRAGMA: On gocloud.dev, hide lines until the next blank line.
	ctx := context.Background()

	// pubsub.OpenTopic creates a *pubsub.Topic from a URL.
	// This URL will Dial the NATS server at the URL in the environment variable
	// NATS_SERVER_URL and send messages with subject "example.mysubject".
	topic, err := pubsub.OpenTopic(ctx, "nats://example.mysubject")
	if err != nil {
		log.Fatal(err)
	}
	defer topic.Shutdown(ctx)
}

func Example_openPullSubscriptionFromURL() {
	// PRAGMA: This example is used on gocloud.dev; PRAGMA comments adjust how it is shown and can be ignored.
	// PRAGMA: On gocloud.dev, add a blank import: _ "gocloud.dev/pubsub/natsjspubsub"
	// PRAGMA: On gocloud.dev, hide lines until the next blank line.
	ctx := context.Background()

	// pubsub.OpenPullSubscription creates a *pubsub.Subscription from a URL.
	// This URL will Dial the NATS server at the URL in the environment variable
	// NATS_SERVER_URL and receive messages with subject "example.mysubject".
	subscription, err := pubsub.OpenSubscription(ctx, "nats://example.mysubject?mode=pull&durablename=foo")
	if err != nil {
		log.Fatal(err)
	}
	defer subscription.Shutdown(ctx)
}

func Example_openBindPullSubscriptionFromURL() {
	// PRAGMA: This example is used on gocloud.dev; PRAGMA comments adjust how it is shown and can be ignored.
	// PRAGMA: On gocloud.dev, add a blank import: _ "gocloud.dev/pubsub/natsjspubsub"
	// PRAGMA: On gocloud.dev, hide lines until the next blank line.
	ctx := context.Background()

	// pubsub.OpenPullSubscription creates a *pubsub.Subscription from a URL.
	// This URL will Dial the NATS server at the URL in the environment variable
	// NATS_SERVER_URL and receive messages with subject "example.mysubject".
	subscription, err := pubsub.OpenSubscription(ctx, "nats://example.mysubject?mode=pull&durablename=foo&bind=stream&bind=foo")
	if err != nil {
		log.Fatal(err)
	}
	defer subscription.Shutdown(ctx)
}

func Example_openMaxRequestBatchPullSubscriptionFromURL() {
	// PRAGMA: This example is used on gocloud.dev; PRAGMA comments adjust how it is shown and can be ignored.
	// PRAGMA: On gocloud.dev, add a blank import: _ "gocloud.dev/pubsub/natsjspubsub"
	// PRAGMA: On gocloud.dev, hide lines until the next blank line.
	ctx := context.Background()

	// pubsub.OpenPullSubscription creates a *pubsub.Subscription from a URL.
	// This URL will Dial the NATS server at the URL in the environment variable
	// NATS_SERVER_URL and receive messages with subject "example.mysubject".
	subscription, err := pubsub.OpenSubscription(ctx, "nats://example.mysubject?mode=pull&durablename=foo&bind=maxrequestbatch=10")
	if err != nil {
		log.Fatal(err)
	}
	defer subscription.Shutdown(ctx)
}

func Example_openBindStreamPullSubscriptionFromURL() {
	// PRAGMA: This example is used on gocloud.dev; PRAGMA comments adjust how it is shown and can be ignored.
	// PRAGMA: On gocloud.dev, add a blank import: _ "gocloud.dev/pubsub/natsjspubsub"
	// PRAGMA: On gocloud.dev, hide lines until the next blank line.
	ctx := context.Background()

	// pubsub.OpenPullSubscription creates a *pubsub.Subscription from a URL.
	// This URL will Dial the NATS server at the URL in the environment variable
	// NATS_SERVER_URL and receive messages with subject "example.mysubject".
	subscription, err := pubsub.OpenSubscription(ctx, "nats://example.mysubject?mode=pull&durablename=foo&bindstream=stream")
	if err != nil {
		log.Fatal(err)
	}
	defer subscription.Shutdown(ctx)
}

func Example_openDeliveryGroupPushSubscriptionFromURL() {
	// PRAGMA: This example is used on gocloud.dev; PRAGMA comments adjust how it is shown and can be ignored.
	// PRAGMA: On gocloud.dev, add a blank import: _ "gocloud.dev/pubsub/natsjspubsub"
	// PRAGMA: On gocloud.dev, hide lines until the next blank line.
	ctx := context.Background()

	// pubsub.OpenPullSubscription creates a *pubsub.Subscription from a URL.
	// This URL will Dial the NATS server at the URL in the environment variable
	// NATS_SERVER_URL and receive messages with subject "example.mysubject".
	subscription, err := pubsub.OpenSubscription(ctx, "nats://example.mysubject?mode=push&deliverygroup=foo")
	if err != nil {
		log.Fatal(err)
	}
	defer subscription.Shutdown(ctx)
}

func Example_openPushSubscriptionFromURL() {
	// PRAGMA: This example is used on gocloud.dev; PRAGMA comments adjust how it is shown and can be ignored.
	// PRAGMA: On gocloud.dev, add a blank import: _ "gocloud.dev/pubsub/natsjspubsub"
	// PRAGMA: On gocloud.dev, hide lines until the next blank line.
	ctx := context.Background()

	// pubsub.OpenPullSubscription creates a *pubsub.Subscription from a URL.
	// This URL will Dial the NATS server at the URL in the environment variable
	// NATS_SERVER_URL and receive messages with subject "example.mysubject"
	// This URL will be parsed and the queue attribute will be used as the Queue parameter when creating the NATS Subscription.
	subscription, err := pubsub.OpenSubscription(ctx, "nats://example.mysubject?mode=push")
	if err != nil {
		log.Fatal(err)
	}
	defer subscription.Shutdown(ctx)
}
