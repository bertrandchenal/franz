# Franz

Franz is an experimental implementation of a stream (or log) server,
a concept that was popularized by Apacha Kafka.


## Motivation

Provide a Kafka-like solution that is easy to deploy and easy to use:
- It's a go app, so can be distributed as a single-file binary
- It use the websocket protocol, so it can be interfaced with any
  language that has a websocket library. It also means that it can be
  used across web proxies.


## What is missing (aka the difficult stuff)

- Replication of partitions
- Support for consumer groups
