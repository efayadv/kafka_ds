# This is my own version of a Apache Kafka like Distributed System

The project explores the core ideas behind distributed log systems: binary client–broker communication, topic partitioning, append-only storage, broker discovery, controller election, leader/follower replication, and message consumption by offset.

This is a small version made to educate myself on distributed systems.

## Features

* Custom binary wire protocol built with Java ByteBuffer

Topic and partition-based message organization

Append-only partition logs stored in segments

Offset-based message reads

Index entries that map offsets to positions in log files

ZooKeeper-based broker registration and discovery

Ephemeral ZooKeeper nodes for broker liveness

Controller election through ZooKeeper

Partition leader and follower assignments

Message replication between brokers

Java producer and consumer clients
