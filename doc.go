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

// Package natsjspubsub provides a pubsub implementation for NATS JetStream. Use OpenTopic to
// construct a *pubsub.Topic, OpenPullSubscription to construct a Pull based *pubsub.Subscription,
// or OpenPushSubscription to construct a Push based *pubsub.Subscription.
//
// URLs
//
// For pubsub.OpenTopic and pubsub.OpenSubscription, natsjspubsub registers
// for the scheme "natsjs".
// The default URL opener will connect to a default server based on the
// environment variable "NATS_SERVER_URL".
// To customize the URL opener, or for more details on the URL format,
// see URLOpener.
// See https://gocloud.dev/concepts/urls/ for background information.
//
// Message Delivery Semantics
//
// NATS JetStream supports at-least-semantics; applications need not call Message.Ack,
// and Message.Nack once handled / unacknowledged.
// See https://godoc.org/gocloud.dev/pubsub#hdr-At_most_once_and_At_least_once_Delivery
// for more background.
//
// As
//
// natsjspubsub exposes the following types for As:
//  - Topic: nats.JetStream
//  - Subscription: *nats.Subscription
//  - Message.BeforeSend: *nats.Msg
//  - Message.AfterSend: *nats.PubAck.
//  - Message: *nats.Msg
package natsjspubsub
