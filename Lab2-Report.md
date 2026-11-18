# Lab 2 — Hands-On Kafka (CLI)

## Setup

Kafka ran in Docker using the course setup (`kafka-installation/compose.yaml`): one ZooKeeper node and one Kafka broker, Confluent 7.6.15. All Kafka commands ran inside the broker container, from the `kafka-installation` folder:

```
docker compose exec kafka <kafka-command> --bootstrap-server kafka:29092 ...
```

Below, Kafka commands are shown without the `docker compose exec kafka` prefix and the `--bootstrap-server kafka:29092` flag.

**Topic name.** A `transactions` topic already existed from the previous lab (4 partitions, 600 messages, used by five consumer groups). Creating it again fails with `Topic 'transactions' already exists`, and `--if-not-exists` silently keeps the old topic. To start clean without deleting that data, I created a separate topic, `lab2-transactions`. A broker can hold many topics side by side.

## Task 1 — Start Kafka

```
docker compose up -d
docker compose ps
kafka-broker-api-versions | head -1
docker compose exec zookeeper zookeeper-shell localhost:2181 ls /brokers/ids
```

**Observed:** both containers were `Up`. The broker answered as `kafka:29092 (id: 1)`, and ZooKeeper listed one registered broker: `[1]`.

## Task 2 — Topics

```
kafka-topics --list
kafka-topics --create --topic lab2-transactions --partitions 3 --replication-factor 1
kafka-topics --describe --topic lab2-transactions
kafka-configs --entity-type topics --entity-name lab2-transactions --describe --all
```

**Observed:** the topic has **3 partitions** and replication factor 1. Broker 1 is the leader of every partition, since it is the only broker. Messages are kept for 7 days (`retention.ms=604800000`) and then deleted (`cleanup.policy=delete`). Without `--partitions`, the topic would get the broker default of 1.

## Task 3 — Producers

```
kafka-console-producer --topic lab2-transactions
kafka-get-offsets --topic lab2-transactions
tail -n +2 ../Hands-on-Kafka/transactions.csv | awk -F, '{print $2 "|" $0}' | docker compose exec -T kafka kafka-console-producer --bootstrap-server kafka:29092 --topic lab2-transactions --property parse.key=true --property 'key.separator=|'
```

The first command sends messages typed by hand. The last one sends every CSV row with `user_id` as the message key.

**Observed:**

- The 3 typed messages had no key and all landed in partition 2. Without a key, the producer fills one partition at a time instead of spreading every message.
- The 200 CSV rows landed 108 / 12 / 80 in partitions 0 / 1 / 2. The partition is chosen by hashing the key, and there are only 11 user IDs, so the split is uneven. Partition 1 holds only user 110.
- End offsets afterwards were 108 / 12 / 83, 203 messages in total. Messages are only appended: each new message gets the next offset in its partition.

## Task 4 — Consumers

```
kafka-console-consumer --topic lab2-transactions --from-beginning --property print.partition=true --property print.offset=true --property print.key=true
kafka-console-consumer --topic lab2-transactions
```

**Observed:**

- With `--from-beginning`, all 203 messages were printed one partition at a time, not in CSV order. Order is only guaranteed inside a partition.
- The same user ID always appeared in the same partition.
- A message typed in the producer appeared in the running consumer immediately.
- Restarted **without** `--from-beginning`, the consumer printed no old messages, only new ones. Restarted **with** it, everything was printed again. Without `--group`, each run is a new anonymous group, so nothing is remembered between runs.

## Task 5 — Offsets

```
kafka-console-consumer --topic lab2-transactions --group txn-readers --from-beginning
kafka-consumer-groups --describe --group txn-readers
kafka-consumer-groups --group txn-readers --topic lab2-transactions --reset-offsets --to-earliest --dry-run
kafka-consumer-groups --group txn-readers --topic lab2-transactions --reset-offsets --to-earliest --execute
```

**Observed:**

- After reading everything, the group's committed offsets were 108 / 12 / 83. They equal the end of each partition, so the lag was 0.
- After producing a few new messages, the lag went up by the number of new messages in each partition.
- Restarting in the same group with `--from-beginning` returned **only the new messages**. Once a group has committed offsets, that flag is ignored.
- Resetting the group's offsets to earliest replayed all messages. This only works while no consumer in the group is running.
- The broker stores committed offsets in the internal `__consumer_offsets` topic, one entry per group, topic and partition.

## Task 6 — Consumer groups

```
kafka-console-consumer --topic lab2-transactions --group txn-workers --property print.partition=true --property print.key=true
kafka-consumer-groups --describe --group txn-workers --members --verbose
kafka-console-producer --topic lab2-transactions --property parse.key=true --property 'key.separator=|'
kafka-console-consumer --topic lab2-transactions --group txn-audit --from-beginning
```

The first command ran in 2, then 3, then 4 terminals at once.

**Observed:**

| Consumers in `txn-workers` | Partitions per consumer |
| --- | --- |
| 2 | 2 and 1 |
| 3 | 1 each |
| 4 | 1, 1, 1 and 0 (one idle) |

- New keyed messages were split by partition. Each consumer printed only messages from its own partition; for example, all user 110 messages went to the consumer holding partition 1.
- Stopping an active consumer triggered a **rebalance**: its partition was handed to another member, including the idle one.
- A consumer in a different group (`txn-audit`) received every message. Consumers in the same group share the work; each separate group gets its own full copy.

## Reflection questions

**Why are topics considered logical streams?**
A topic is a named category of events, such as transactions, not a single physical file. Underneath, it is split into partitions, and each partition is an append-only log stored on a broker. Producers and consumers only use the topic name, and Kafka handles where the data physically lives.

**Why does Kafka use partitions?**
For parallelism and scale. Partitions can sit on different brokers and be written and read at the same time, so throughput grows with the number of partitions. Order is kept inside each partition, and a key always maps to the same partition, so one user's events stay in order.

**What is the role of offsets?**
An offset is a message's position in its partition. Each consumer group commits the offset it has reached, so after a restart or crash it resumes where it stopped. Reading does not delete messages, so moving the offset back replays the data.

**Why does each partition only allow one consumer per group?**
To keep order and avoid duplicate work. Two consumers in the same group reading one partition would process the same messages twice or out of order. One owner per partition also means a single committed offset per partition for the group.

**What happens if there are more consumers than partitions?**
The extra consumers get no partition and sit idle, like the 4th consumer with 3 partitions. They act as standbys: if an active consumer stops, a rebalance gives its partition to an idle one. The number of partitions sets the maximum parallelism of a group.
