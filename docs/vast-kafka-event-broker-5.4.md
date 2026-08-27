# Deploying a Kafka Event Broker in VAST 5.4

A focused setup-and-test walkthrough for standing up a basic Kafka-compatible
Event Broker on a VAST cluster, wiring an S3 event notification to it, and
validating the result with the [`s3_event_consumer.py`](../README.md) demo
consumer in this repository.

This is **not** a substitute for VAST product documentation. It documents one
reproducible lab procedure, in the order it was actually performed, so another
engineer can repeat it.

## Audience and scope

Intended for a VAST SE, administrator, or technically competent user who wants
to:

1. Create a VIP pool for the Event Broker.
2. Create a view that hosts the Kafka broker.
3. Create a Kafka topic.
4. Create a user-data S3 bucket with an S3 event notification pointing at that
   topic.
5. Prove the chain end to end: **S3 PUT → VAST Event Notification → Kafka topic
   → consumer**.

Out of scope: capacity planning, multi-tenant design, production security
hardening, Kafka client tuning, and anything specific to VAST releases other
than the one below.

## Version context

The procedure was captured against:

```
release-5.4.6-2628322
```

> **Note**
> Treat this as version context, not a guarantee. UI labels, menu placement, and
> available options change between VAST builds. Where the exact wording in your
> build differs from what is written here, the navigation context should still
> be enough to locate the function. Confirm anything that looks different
> against the VAST documentation for your release.

## Prerequisites

Before starting, you need:

| Requirement | Notes |
| --- | --- |
| A VAST cluster running 5.4 | This guide was written against 5.4.6. |
| VAST UI access with rights to create VIP pools, views, topics, and event notifications | Typically an administrative role. |
| Free IP addresses on a network your client can reach | See [Step 1](#step-1-allocate-ip-addresses-and-create-the-event-broker-vip-pool). Obtain these from whoever owns IPAM for that subnet. |
| A tenant, view policy, and bucket owner (an existing VAST S3 user) | The lab used the `default` tenant and the built-in `s3_default_policy`. |
| A Linux x86_64 or macOS host with network access to the Event Broker VIPs | Runs the consumer. Python is only needed if you run from source rather than the standalone executable — see [Step 5](#step-5-configure-and-run-the-consumer). |
| An S3 client (for example the AWS CLI) with credentials for the bucket owner | Used to generate the test PUT. See [Optional: S3 access keys](#optional-s3-access-keys). |

## Example values used in this guide

Every value below is an **example**. None of them will be correct for your
environment.

> **Warning**
> Replace every value in this table with values valid for your cluster and
> network. In particular, the IP addresses are documentation-only addresses
> from [RFC 5737](https://www.rfc-editor.org/rfc/rfc5737) (`192.0.2.0/24`,
> TEST-NET-1). They are reserved for examples and **must not** be configured on
> a real network.

| Item | Example value |
| --- | --- |
| Event Broker VIP pool name | `kafka-broker-vippool` |
| Event Broker VIP range | `192.0.2.55` – `192.0.2.59` |
| Broker view path | `/demo/kafka-broker` |
| Broker S3 bucket name | `kafka-broker` |
| Bucket owner | `user@example.com` |
| Kafka topic name | `s3-events` |
| Data view path | `/demo/data` |
| Data S3 bucket name | `demo-data` |
| Event notification name | `object-created-event` |
| Tenant | `default` |
| View policy | `s3_default_policy` |

---

## Step 1: Allocate IP addresses and create the Event Broker VIP pool

The Kafka-compatible Event Broker is reached through a VIP pool, so the pool has
to exist before the broker view can be created.

### 1.1 Size and reserve the address range

> **Note**
> Field observation, not a quoted product requirement: the lab pool was sized
> with **at least one IP address per CNode**, which allows every CNode to
> present a broker endpoint. Confirm the supported and recommended sizing for
> your release in the VAST documentation before treating this as a hard rule.

Reserve the addresses properly:

- Request the range from whoever administers IPAM/DHCP for that subnet, and have
  them mark it as statically assigned.
- Confirm the range is unallocated in the authoritative source of record.
  Probing an address (ping, ARP, port scan) can show that something *is*
  responding, but silence does not prove an address is free — hosts may be
  offline or filtering. Never rely on a probe alone.
- The addresses must be routable from any client that will speak Kafka to the
  broker, including the host that runs the consumer.

### 1.2 Create the pool

In the VAST UI:

```
Network Access > Virtual IP Pools > Create Virtual IP Pool
```

| Field | Example | Notes |
| --- | --- | --- |
| Name | `kafka-broker-vippool` | Use a name that identifies the purpose. |
| IP range | `192.0.2.55` – `192.0.2.59` | **Replace.** Your reserved range. |
| Subnet CIDR / prefix length | *(matches your subnet)* | Must match the real prefix length of the subnet the VIPs live in. The lab subnet happened to be a `/16`; yours will very likely differ. |
| Include all CNodes | Enabled | Lets every CNode serve the pool. |

Create the pool, then confirm it appears with the expected address range before
continuing.

---

## Step 2: Create the view that hosts the Kafka broker

In the VAST UI:

```
Element Store > Views > Create View
```

| Field | Example | Notes |
| --- | --- | --- |
| Tenant | `default` | **Replace** if you use a non-default tenant. |
| Path | `/demo/kafka-broker` | **Replace.** Path of the broker view. |
| Create Directory | Enabled | Creates the path if it does not exist. |
| Policy | `s3_default_policy` | **Replace** with the view policy appropriate for your cluster. |
| Protocols | `Kafka` | Selecting Kafka is what makes this view an Event Broker. |
| S3 bucket name | `kafka-broker` | **Replace.** |
| S3 bucket owner | `user@example.com` | **Replace** with an existing VAST S3 user. |
| Kafka VIP pool | `kafka-broker-vippool` | The pool created in Step 1. |

> **Note**
> In the 5.4.6 build used for this walkthrough, enabling the Kafka protocol also
> surfaced S3 bucket and Database options in the same create-view workflow, so
> the bucket name and owner are set here alongside the Kafka settings. The exact
> set of fields exposed may differ in other builds.

### Authentication settings

The create-view workflow presents the authentication methods the broker will
accept.

> **Warning**
> The original lab demonstration ran on a disposable, isolated cluster, and
> simplified the setup by **de-selecting the encrypted authentication methods**.
> That is a lab shortcut for an isolated environment. It is **not** a
> recommendation, and it should not be carried into a shared, production, or
> otherwise security-sensitive deployment: it affects how Kafka clients
> authenticate and whether that traffic is protected in transit.
>
> This guide deliberately does not document a specific "secure" combination of
> authentication and encryption settings, because the correct choice depends on
> your organization's requirements and on the exact options your VAST release
> offers. Follow your organization's security policy and the VAST documentation
> for your release when selecting authentication methods.
>
> Whatever you choose here must match the client-side settings in
> [`s3_consumer_config.json`](../s3_consumer_config.example.json). The example
> configuration in this repository contains no security settings at all, which
> only works against a broker configured to accept unauthenticated,
> unencrypted connections. If your broker requires SASL and/or TLS, add the
> corresponding `security.protocol`, `sasl.*`, and `ssl.*` keys to
> `kafka_config` — they are passed straight through to librdkafka.

Create the view, then confirm it is listed with the Kafka protocol enabled.

---

## Step 3: Create a Kafka topic

Topics are managed from the Database area of the UI.

```
Database > VAST Database
```

Select the broker created in Step 2, then:

```
Kafka-Compatible Broker Topics > Add Topic
```

> **Note**
> This navigation path was recorded in a single VAST 5.4.6 environment and is
> not guaranteed across every 5.4 build. The capitalization of the menu entry
> ("Database" / "DataBase" / "VAST DataBase") also varies in the field notes and
> could not be verified. If your UI differs, look for the Database area of the
> navigation, select your Event Broker, and find the Kafka-compatible broker
> topic list beneath it.

| Field | Example | Notes |
| --- | --- | --- |
| Name | `s3-events` | **Replace.** This exact string goes into the consumer's `topic` setting. |
| Partitions | `1` | See below. |
| Retention | `7 days` | See below. |

**What these settings mean for this demo**

- **Partitions** — a topic is split into partitions, and Kafka only guarantees
  message ordering *within* a partition. Partitions are also the unit of
  parallelism: one partition can be read by only one consumer in a consumer
  group at a time. `1` partition keeps ordering simple and is sufficient for a
  single-consumer demonstration; production sizing is a different exercise.
- **Retention** — how long the broker keeps messages before discarding them.
  With `7 days`, an event published today is still readable a week later, which
  is convenient for a demo you may re-run. Combined with
  `"auto.offset.reset": "earliest"` in the consumer configuration, a **new**
  consumer group will replay every retained event on its first run — expect a
  burst of old events the first time you start with a fresh `group.id`.

Create the topic, then confirm it appears in the broker's topic list.

---

## Step 4: Create the user-data S3 bucket and its event notification

This is the bucket you will write objects into. It is deliberately separate from
the broker view.

### 4.1 Create the data view

```
Element Store > Views > Create View
```

| Field | Example | Notes |
| --- | --- | --- |
| Tenant | `default` | **Replace** if needed. |
| Path | `/demo/data` | **Replace.** |
| Create Directory | Enabled | |
| Policy | `s3_default_policy` | **Replace** as appropriate. |
| Protocols | `S3 Bucket` | S3 only — this view does not host a broker. |
| S3 bucket name | `demo-data` | **Replace.** |
| S3 bucket owner | `user@example.com` | **Replace.** |

### 4.2 Create the event notification

With the data view selected:

```
S3 Event Notifications > Create New Notification
```

| Field | Example | Notes |
| --- | --- | --- |
| Event name | `object-created-event` | **Replace.** A label for the rule. |
| Broker | `kafka-broker` | The Event Broker from Step 2. It is listed under the name of the view/bucket you created there, so pick the one matching your Step 2 values. |
| Topic | `s3-events` | The topic from Step 3. |
| Event type | Object Creation → Select Manually → **Put** (`ObjectCreated:Put`) | Narrow trigger, easy to demonstrate. |

**This is the link that makes the demo work.** Once this notification exists, an
S3 `PUT` of an object into `demo-data` causes VAST to publish an
`ObjectCreated:Put` event describing that object to the `s3-events` topic on the
`kafka-broker` Event Broker. Anything subscribed to that topic — including
`s3_event_consumer.py` — receives it.

```
PUT s3://demo-data/hello.txt
        │
        ▼
  S3 event notification "object-created-event"  (event type ObjectCreated:Put)
        │
        ▼
  Event Broker "kafka-broker"  →  topic "s3-events"
        │
        ▼
  s3_event_consumer.py prints the event payload
```

> **Note**
> Because the event type is restricted to `ObjectCreated:Put`, other operations
> (deletes, and creation paths that are not a plain PUT, such as multipart
> completion or a server-side copy, depending on how your release classifies
> them) will **not** produce an event with this rule. If you want to demonstrate
> those, add the relevant event types to the notification.

---

## Step 5: Configure and run the consumer

The consumer lives in this repository:

<https://github.com/kmacvast/s3_event_consumer>

### 5.1 Get the consumer

There are two ways to run it. Python is **not** required for the first.

**Standalone executable — recommended for simply running the demo.**
Prebuilt archives are checked into the repository under
[`bin/`](../bin/), so no Python installation and no GitHub Release are needed:

| Platform | Download |
| --- | --- |
| Linux x86_64 | <https://github.com/kmacvast/s3_event_consumer/raw/main/bin/s3_event_consumer-linux-x86_64.tar.gz> |
| macOS Apple Silicon | <https://github.com/kmacvast/s3_event_consumer/raw/main/bin/s3_event_consumer-macos-arm64.tar.gz> |

On the consumer host:

```bash
curl -LO https://github.com/kmacvast/s3_event_consumer/raw/main/bin/s3_event_consumer-linux-x86_64.tar.gz
tar -xzf s3_event_consumer-linux-x86_64.tar.gz
```

The archive contains the executable and `s3_consumer_config.example.json`.
Checksums and build provenance are in [`bin/README.md`](../bin/README.md).

The Linux build needs glibc 2.28 or newer. The macOS build is unsigned; see the
[README](../README.md#option-a-standalone-executable-no-python-needed) if
macOS blocks it. Commands below are shown as `./s3_event_consumer`.

**Python source — useful for development or inspection.**

```bash
git clone https://github.com/kmacvast/s3_event_consumer.git
cd s3_event_consumer
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Requires Python 3.9 or newer. Substitute `python3 s3_event_consumer.py` for
`./s3_event_consumer` in the commands below.

### 5.2 Configure

```bash
cp s3_consumer_config.example.json s3_consumer_config.json
```

Edit `s3_consumer_config.json`:

```json
{
  "kafka_config": {
    "bootstrap.servers": "192.0.2.55:<KAFKA_PORT>,192.0.2.56:<KAFKA_PORT>",
    "group.id": "s3-event-demo",
    "auto.offset.reset": "earliest"
  },
  "topic": "s3-events"
}
```

| Key | What to put here |
| --- | --- |
| `bootstrap.servers` | Comma-separated `host:port` list of Event Broker endpoints. Use IPs from the VIP pool created in Step 1 (or DNS names that resolve to them). **Obtain the port from your VAST Event Broker configuration or the VAST documentation for your release** — this guide does not assume a port value. Listing two or more endpoints gives the client somewhere else to go if one is unavailable. |
| `group.id` | Any consumer group name. Kafka tracks read position per group, so reusing a group resumes where it left off; a new group starts according to `auto.offset.reset`. |
| `auto.offset.reset` | `earliest` replays all retained events for a brand-new group; `latest` shows only events published after the consumer starts. |
| `topic` | The topic name from Step 3, exactly. |

Anything else you add under `kafka_config` is passed straight through to
librdkafka, which is how SASL/TLS settings would be supplied if your broker
requires them.

> **Warning**
> `s3_consumer_config.json` is listed in `.gitignore` because it names cluster
> VIPs and may hold credentials. Do not commit it, and do not put access keys or
> SASL passwords into any file you intend to push.

### 5.3 Run

```bash
./s3_event_consumer --config s3_consumer_config.json
```

Successful startup looks like this (operational messages go to stderr):

```
2026-01-01 09:00:00 INFO    Configuration:      s3_consumer_config.json
2026-01-01 09:00:00 INFO    Bootstrap servers:  192.0.2.55:<KAFKA_PORT>,192.0.2.56:<KAFKA_PORT>
2026-01-01 09:00:00 INFO    Consumer group:     s3-event-demo
2026-01-01 09:00:00 INFO    Topic:              s3-events
2026-01-01 09:00:00 INFO    Subscribed to 's3-events'. Waiting for events (Ctrl-C to stop).
```

After that the consumer sits idle and prints nothing until an event arrives.
Silence at this point is the expected, healthy state — it is not a sign that
something is wrong. If the broker is unreachable you will instead see a
`No Kafka broker reachable` error, reported once rather than repeating.

Press **Ctrl-C** to stop. The consumer finishes its current poll, closes the
Kafka consumer cleanly — which lets the group commit its position and release its
partitions rather than waiting for a session timeout — and exits with status 0:

```
2026-01-01 09:05:12 INFO    Interrupted, shutting down.
2026-01-01 09:05:12 INFO    Closing Kafka consumer.
```

---

## Step 6: Validate end to end

Leave the consumer running in one terminal and use a second terminal for the
upload.

### 6.1 Generate an S3 PUT

This repository does not ship an upload utility, so use any S3 client. The AWS
CLI example below assumes:

- The AWS CLI v2 is installed.
- You have an access key and secret for the bucket owner (see
  [Optional: S3 access keys](#optional-s3-access-keys)).
- `S3_ENDPOINT` is the **S3 endpoint of your VAST cluster** — the S3 VIP pool or
  its DNS name. This is *not* the Kafka Event Broker VIP pool from Step 1, and
  not the broker's own bucket.
- Your cluster's S3 endpoint and TLS configuration are reachable from the client.

```bash
export AWS_ACCESS_KEY_ID='<your-access-key-id>'
export AWS_SECRET_ACCESS_KEY='<your-secret-access-key>'
export S3_ENDPOINT='https://s3.vast.example.com'   # REPLACE: your VAST S3 endpoint

echo "hello vast" > hello.txt

aws --endpoint-url "$S3_ENDPOINT" s3api put-object \
    --bucket demo-data \
    --key hello.txt \
    --body hello.txt
```

> **Note**
> `aws s3api put-object` is used rather than `aws s3 cp` because it always
> issues a single `PutObject` call. `aws s3 cp` may switch to a multipart upload
> for larger files, which is a different event type and would not match an
> `ObjectCreated:Put`-only notification.
>
> If bucket addressing fails against an endpoint specified by IP address, your
> client may need path-style addressing
> (`aws configure set default.s3.addressing_style path`).

### 6.2 Confirm the event

Within a second or two, the consumer terminal should log the arrival and print
the event payload to stdout:

```
2026-01-01 09:03:41 INFO    Event received (s3-events[0]@0, 742 bytes)
{
  "Records": [
    {
      "eventName": "ObjectCreated:Put",
      "s3": {
        "bucket": {
          "name": "demo-data"
        },
        "object": {
          "key": "hello.txt"
        }
      }
    }
  ]
}
```

> **Note**
> The exact payload structure and field set are produced by VAST, not by this
> consumer, and may vary by release. The consumer pretty-prints whatever JSON
> the topic delivers. The fields to look for when demonstrating are the event
> name, the bucket name, and the object key — they should match the object you
> just uploaded.

### 6.3 Checklist

The demonstration has succeeded when all four are true:

1. The consumer started and reported it was subscribed to the topic.
2. The S3 PUT completed without error.
3. The consumer logged `Event received` for the topic.
4. The printed payload names the bucket and key you uploaded.

---

## Optional: S3 access keys

If the user performing the test PUT does not already have S3 credentials, an
access key can be created in the VAST UI:

```
User Management > Query User or Group
```

Select the user, then:

```
Access Keys > Add Another Key
```

The secret is shown once at creation. Capture it then, or you will need to
create another key.

> **Warning**
> An access key ID and secret are credentials. Do not paste them into this
> repository, into `s3_consumer_config.json`, into shared documents, or into
> chat. Do not commit them to Git — a secret pushed to a remote must be treated
> as compromised and rotated, even if the commit is later removed. Prefer
> environment variables or your S3 client's own credential store, and delete
> demo keys when the demonstration is finished.

---

## Troubleshooting

| Symptom | Likely cause | What to check |
| --- | --- | --- |
| `No Kafka broker reachable` on startup | The consumer cannot open a TCP connection to any endpoint in `bootstrap.servers`. | Confirm the VIPs from Step 1 are reachable from the client (`ping`, then `nc -vz <vip> <port>`). Check the port is the one the broker actually listens on, and check firewalls/routing between the client and the VIP network. |
| Startup fails with `still holds the sample value` | `s3_consumer_config.json` was never edited, or was copied from the example without changes. | Replace `bootstrap.servers` with your real broker endpoints. |
| Startup fails with a configuration error naming a key | Missing or empty `topic`, `bootstrap.servers`, or `group.id`. | Compare against `s3_consumer_config.example.json`. The error message names the offending key and the file. |
| Consumer starts and stays silent, S3 PUT produces nothing | Most often the topic name does not match, or the notification is not firing. | Confirm the `topic` value exactly matches the topic name from Step 3 (names are case-sensitive). Confirm the event notification in Step 4 targets the right broker **and** topic, and that its event type includes the operation you performed. |
| Consumer is silent but the upload used `aws s3 cp` of a large file | The upload became a multipart upload, which is not `ObjectCreated:Put`. | Retry with `aws s3api put-object` and a small file, or add the relevant event types to the notification. |
| Consumer connects but replays a flood of old events | A new `group.id` combined with `"auto.offset.reset": "earliest"` reads everything still inside the topic's retention window. | Expected behaviour. Keep the same `group.id` between runs, or set `auto.offset.reset` to `latest`. |
| `Message payload is not valid JSON` warning | Something published a non-JSON message to the topic. | The consumer logs the offending offset and a truncated payload, then continues. Check whether another producer is writing to the same topic. |
| `ModuleNotFoundError: No module named 'confluent_kafka'` (source install only) | Dependencies were not installed, or the wrong interpreter/virtualenv is active. | Re-run `pip install -r requirements.txt` inside the active virtual environment, or use the standalone executable instead. |
| Authentication or TLS handshake errors | The broker requires authentication settings the client is not sending (or vice versa). | Align `kafka_config` with the authentication methods selected on the broker view in Step 2. |
| S3 PUT itself fails | Endpoint, credentials, addressing style, or bucket policy. | Verify with a read-only call first, e.g. `aws --endpoint-url "$S3_ENDPOINT" s3api list-buckets`. |

---

## Things to verify for your release

Items that could not be confirmed from the lab notes alone and are worth
checking against the VAST documentation for your build:

- The exact VIP count requirement or recommendation for an Event Broker VIP pool
  (this guide records "one per CNode" as an observation).
- The port the Kafka-compatible Event Broker listens on.
- The exact UI label and navigation path for the Database / topic-management
  area.
- The full list of supported authentication and encryption options for the Kafka
  protocol, and the corresponding client settings.
- The precise schema of the S3 event notification payload.

---

## See also

- [Project README](../README.md) — what the consumer does and how to run it.
- [`s3_consumer_config.example.json`](../s3_consumer_config.example.json) —
  configuration template.
