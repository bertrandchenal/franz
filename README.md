# Franz


Franz is an golang implementation of a stream server.


## Motivation

Provide a Kafka-like solution that is easy to deploy and easy to use:

  - It's a go app, so can be distributed as a single-file binary
  - It use the websocket protocol, so it can be interfaced with any
    language that has a websocket library. It also means that it can be
    used across web proxies.


## What is missing (aka the difficult stuff)

  - Replication of partitions (in progress)
  - Support for consumer groups

# Replication Ideas

- At least 3 peers, default redundancy: 3 copies
- When a tube is created the "gateway" peer pick a candidate peer
  based on current load and wait a quorum of ack to green light the
  tube creation. By default that peer stays the first replica (the
  master) for the full life of the tube, it will contain the full copy
  of it and will support all the writes. Other nodes will replicate
  this tube in a lazy fashion.
- Each peer has to have a meta tube that tells which segments are
  replicated on the current tube, this tube also have to be
  replicated.
- If a machine dies, new masters will have to be elected for the
  orphan tubes (each peer will pick a random one and ask for a
  quorum). This also means that the historical segment are fragmented
  across the cluster there is no one peer with a continuous history,
  but as this part is read-only anyway, it is not an issue.
- For the duration of an election (that can be infinite if a quorum is
  not possible, because to many nodes are down), each gateway node
  receiving writes will buffer those in a tube. It can be configured
  with a short turnover (with short segment that are deleted as soon
  as possible)
