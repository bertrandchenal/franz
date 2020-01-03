# Franz


Franz is an golang implementation of a stream server.


## Motivation

Provide a Kafka-like solution that is easy to deploy and easy to use:

  - It's a go app, so can be distributed as a single-file binary
  - It use the websocket protocol, so it can be interfaced with any
    language that has a websocket library. It also means that it can be
    used across web proxies.


# Replication & concurrency

Peers binds are put in a consitent hash, and this hash is used to
associate them with labels. So for example when a label is created
every peer knows which one is reponsible for it (aka primary peer).

When one or several peer is lost this association change, and may
result in a split-brain situation. In this case each sub-group (or
partition) will redefine this label-to-peer association and forward
writes accordingly.


## Branches

When a parition appears we have a branching situation, because just
like with a DVCS, concurrent writes will happen in different sub-group
of the network split.

Each primary peer keep track of branches in a special branch tube. A
row is added in this branch tube each time a (normal) tube is created,
stopped or branched from another peer.

A row encodes a relation between two buckets. It's a tuple containing:

- old peer id
- new peer id
- old end offset
- new start offset
- flags


In this tuple old peer is always different than new peer (if not there
would be no branching needed), similarly new offset is always greater
than old offset. The only exception is when a new label is created, in
this case old peer id, old offest and new offset are all equal to
zero, only new peer id contains information (the actuall peer where
the first message will be written).

The flags tells if the current branch is the master copy of the data
or if it's a replica.

Each peer continually query all the other peers to fetch updates from
their branch tube, and update its local copies. This ensure that every
peer has a knowledge of all existing labels, and incidentaly this will
detect any peer failure.

The primary peer for a given label is also repsonsible to detect that
the main copy is under-replicated and decide to replicate it.


## Reads

When a label is read (at offset 0 or any later offset), the branch
tubes for this label are read and corresponding tubes are queried.


## Writes

When a new label is created of when a node is out of capacity, the
responsible peer will select 3 random peers and choose the one with
the biggest amount of free space. The decision will be added to the
branch tube and payload forwarded to the selected peer.

In a normal situation, the latest info for the corresponding branch
tube tells on which peer to append the message. This peer wont change
as long as the set of peers does not change and the peer still has
disk capacity.


# What is missing (aka the difficult stuff)

  - Replication of partitions (in progress)
  - Support for consumer groups

