# dist-sys

My solutions to [Gossip Glomers](https://fly.io/dist-sys/) a series of distributed challenges.

## Challenges

## 1. Echo

[here](./ch-echo-1/main.go)

## 2. Unique ID

Used combinations of unique string + node id.
[here](./ch-unique-id-2/main.go)

## 3. Broadcast

### 3a. Single node implementation, no broadcasting
[3a](./ch-broadcast-3a/main.go)

### 3b. Multiple nodes, broadcasting

Used gossip-style broadcasting: each node forwards a message to its neighbors. To prevent infinitive loops, we keep track of the messages we've seen.
[3b](./ch-broadcast-3b/main.go)
