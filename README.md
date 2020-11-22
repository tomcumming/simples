# Simpl*es*

A tiny event sourcing database for prototype and small projects.

### Beware: This is pre-release software!

## It's simple!

- Designed to run on low resource servers like the Raspberry Pi and tiny virtual servers. Minimal RAM usage, only limited by disk space.
- Easy to deploy, one binary, one centralised service. Runs in docker or any other Unix like environment.
- Easy for developers; only HTTP calls are needed.

## It's sturdy

- Designed to preserve data even when crashing mid-write. Ready to relaunch immediately.
- Written in Rust, using Tokio and Hyper.
- Allows for huge messages.
- Uses little more disk space than the contents of the topics.

## When might I use this?

- You are designing an event driven service and need an immutable log.
- You don't need to scale to a distributed database yet.
- You want a simple API, usable from your language of choice.
- You do not want the bother of setting up and configuring one of the bigger servers like [Kafka](http://kafka.apache.org/) or [Pulsar](https://pulsar.apache.org/).

# API

## Creating a topic

Creating a topic called `topic_name`:

```bash
curl -X PUT my-server.local/topic/topic_name
```

This returns a JSON value `true` if the topic was created or `false` if the topic already existed.

## Appending an item

Writing a message to a topic called `topic_name`:

```bash
curl -X POST -d "This is the message contents" my-server.local/topic/topic_name/items
```

This returns a JSON number indicating the ID of this item in the log.

The post body can be any binary data, feel free to use a UTF-8 encoded string, JSON or Protobuf.

## Reading from the log

The following options can be passed in the query string:
- `from` : Start reading from the specified item ID.
- `end_before` : Stop reading before the specified item ID.
- `end_after` : Stop reading after the specified item ID.
- `max_items` : Stop reading after the specified number of items.
- `wait_for_more` : If true the connection will be kept alive, waiting for more items.

This returns a stream of binary data, with the following format:

```
| Item | Item | Item ...
```

Aka a concatenation of `Item`s where an `Item` has the following format:

```
| Item ID | Item data length | Item data |
```

- `Item ID` is a `u64` in big endian format.
- `Item data length` is a `u32` in big endian format.
- `Item data` is some binary data with the length specified above

For example; to print the first item of a topic called `topic_name`:

```bash
curl my-server.local/topic/topic_name/items?max_items=1 > my_data
tail -c +13 my_data # Strip the item ID and the length (12 bytes)
```
