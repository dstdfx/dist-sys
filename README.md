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

### 3c. Fault tolerance broadcasting

In this implementation, I kept the same approach as in 3b, but additionally, I added a periodic syncronization between nodes (every second is enough to pass tests). This way, if a node goes down, it can eventually recover its state from its neighbors.
[3c](./ch-broadcast-3c/main.go)

### 3d. Efficient Broadcast Part 1

In this implementation, I decided to ignore the default topology offered by maelstrom, instead, a node that recieves
initial "broadcast" message becomes "primary", meaning it will be the one responsible for broadcasting messages to the rest of the nodes. In this case, the "primary" node becomes a single point of failure, if it goes down before the rest of the nodes have received the message, the message will be lost. To mitigate this, I added a random "backup" node, that is choosen every time a new "broadcast" message is received.
For the sake of simplicity, the "backup" node just forwards the same message to the rest of the nodes, but it could be further improved, for example, the backup node can wait for some period of time before broadcasting and/or then check N random nodes with "read" call, in order to confirm that the message was already received.

Other minor improvements:
- Changed the interval of the periodic state syncronization between nodes to 3 seconds to reduce the number of messages sent
- Use `SyncRPC` instead of `Send` to make syncronization more reliable

[3d](./ch-broadcast-3d/main.go)

This solution also satisfies the requirements of the next challange `3e`, here's the final metrics:
```
:servers {:send-count 24672,
          :recv-count 24672,
          :msg-count 24672,
          :msgs-per-op 14.729552},

:stable-latencies {0 0, 0.5 78, 0.95 97, 0.99 99, 1 102},
```

Possible further improvements:
- Change the number of nodes we sync state with to reduce the network load (e.g. sync with only log(N)/sqrt(N) random nodes)
- Implement a more sophisticated backup node that can confirm that the message was received by the rest of the nodes

