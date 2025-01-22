package main

import (
	"context"
	"encoding/json"
	"log"
	"maps"
	"os"
	"os/signal"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	rootCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	storage := newMessagesStorage()
	n := maelstrom.NewNode()
	ns := newNodeServer(n, storage)

	n.Handle("broadcast", ns.handleBroadcast)
	n.Handle("read", ns.handleRead)
	n.Handle("topology", ns.handleTopology)
	n.Handle("sync_state", ns.handleSyncState)

	stateSync := newStateSyncronizer(n, storage, time.Second)
	go stateSync.run(rootCtx)

	go func() {
		if err := n.Run(); err != nil {
			log.Fatal(err)
		}
	}()

	<-rootCtx.Done()
}

type nodeServer struct {
	node    *maelstrom.Node
	storage *messagesStorage

	neighborsMu sync.RWMutex
	neighbors   []string
}

func newNodeServer(n *maelstrom.Node, storage *messagesStorage) *nodeServer {
	return &nodeServer{
		node:    n,
		storage: storage,
	}
}

func (ns *nodeServer) handleRead(msg maelstrom.Message) error {
	return ns.node.Reply(msg, map[string]any{
		"type":     "read_ok",
		"messages": ns.storage.getMessages(),
	})
}

func (ns *nodeServer) handleBroadcast(msg maelstrom.Message) error {
	body := struct {
		Message int `json:"message"`
		MsgID   int `json:"msg_id"`
	}{}

	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	// Save the message to the node's state
	isDuplicateMsg := ns.storage.addMessage(body.Message)

	if isDuplicateMsg {
		if body.MsgID == 0 {
			// Do not reply to fire-and-forget messages

			return nil
		}

		// Skip broadcasting the message if it has been seen before
		return ns.node.Reply(msg, map[string]any{
			"type": "broadcast_ok",
		})
	}

	ns.neighborsMu.RLock()
	neighbors := ns.neighbors
	ns.neighborsMu.RUnlock()

	wg := sync.WaitGroup{}
	wg.Add(len(neighbors))

	// Broadcast message to all node's neighbors
	for _, neighbor := range neighbors {
		if neighbor == ns.node.ID() || neighbor == msg.Src {
			wg.Done()

			// Skip sending the message to the node itself and the sender
			continue
		}

		go func() {
			defer wg.Done()

			// Retry sending the message until it is successful
			for {
				if err := ns.node.Send(neighbor, map[string]any{
					"type":    "broadcast",
					"message": body.Message,
				}); err == nil {
					break
				}
			}
		}()
	}

	wg.Wait()

	if body.MsgID == 0 {
		// Do not reply to fire-and-forget messages

		return nil
	}

	return ns.node.Reply(msg, map[string]any{
		"type": "broadcast_ok",
	})
}

func (ns *nodeServer) handleTopology(msg maelstrom.Message) error {
	body := struct {
		Topology map[string][]string `json:"topology"`
	}{}

	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	ns.neighborsMu.Lock()
	ns.neighbors = body.Topology[ns.node.ID()]
	ns.neighborsMu.Unlock()

	return ns.node.Reply(msg, map[string]any{
		"type": "topology_ok",
	})
}

func (ns *nodeServer) handleSyncState(msg maelstrom.Message) error {
	body := struct {
		Messages []int `json:"messages"`
	}{}

	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	ns.storage.mergeMessages(body.Messages)

	return nil
}

/* State syncronizer */

type stateSyncronizer struct {
	node        *maelstrom.Node
	interval    time.Duration
	storage     *messagesStorage
	lastSyncMap map[int]struct{}
}

func newStateSyncronizer(n *maelstrom.Node, storage *messagesStorage, interval time.Duration) *stateSyncronizer {
	return &stateSyncronizer{
		node:        n,
		interval:    interval,
		storage:     storage,
		lastSyncMap: make(map[int]struct{}, 0),
	}
}

func (ss *stateSyncronizer) run(ctx context.Context) {
	ticker := time.NewTicker(ss.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			ss.syncState()

			ticker.Reset(ss.interval)
		}
	}
}

func (ss *stateSyncronizer) syncState() {
	messages := ss.storage.getMessages()

	// Skip syncing if the state has not changed
	currentMessages := make(map[int]struct{}, len(messages))
	for _, msg := range messages {
		currentMessages[msg] = struct{}{}
	}

	if maps.Equal(currentMessages, ss.lastSyncMap) {
		return
	}

	// TODO: randomize the order of neighbors to gossip and/or send only
	// 	     to a subset of neighbors log(n) or sqrt(n) neighbors, for example

	// Send current node state to all neighbors
	for _, node := range ss.node.NodeIDs() {
		if node == ss.node.ID() {
			// Skip sending the message to the node itself
			continue
		}

		if err := ss.node.Send(node, map[string]any{
			"type":     "sync_state",
			"messages": messages,
		}); err != nil {
			// For now we just skip error handling and retries
			continue
		}
	}

	// Update the last synced state
	ss.lastSyncMap = currentMessages
}

/* Messages storage */

type messagesStorage struct {
	messagesMu sync.RWMutex
	messages   map[int]struct{}
}

func newMessagesStorage() *messagesStorage {
	return &messagesStorage{
		messages: make(map[int]struct{}, 0),
	}
}

func (ms *messagesStorage) addMessage(msg int) (duplicate bool) {
	ms.messagesMu.Lock()
	defer ms.messagesMu.Unlock()

	_, duplicate = ms.messages[msg]
	if !duplicate {
		ms.messages[msg] = struct{}{}
	}

	return duplicate
}

func (ms *messagesStorage) getMessages() []int {
	ms.messagesMu.RLock()
	defer ms.messagesMu.RUnlock()

	msgs := make([]int, 0, len(ms.messages))
	for k := range ms.messages {
		msgs = append(msgs, k)
	}

	return msgs
}

func (ms *messagesStorage) mergeMessages(newMessages []int) {
	ms.messagesMu.Lock()
	defer ms.messagesMu.Unlock()

	for _, msg := range newMessages {
		ms.messages[msg] = struct{}{}
	}

}
