package main

import (
	"encoding/json"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()
	nodeMetadata := newNodeMetadata(n)

	n.Handle("broadcast", nodeMetadata.handleBroadcast)
	n.Handle("read", nodeMetadata.handleRead)
	n.Handle("topology", nodeMetadata.handleTopology)

	if err := n.Run(); err != nil {
		panic(err)
	}
}

type nodeMetadata struct {
	node       *maelstrom.Node
	messagesMu sync.RWMutex
	messages   []int
}

func newNodeMetadata(n *maelstrom.Node) *nodeMetadata {
	return &nodeMetadata{node: n}
}

func (nm *nodeMetadata) handleRead(msg maelstrom.Message) error {
	nm.messagesMu.RLock()
	messages := nm.messages
	nm.messagesMu.RUnlock()

	return nm.node.Reply(msg, map[string]any{
		"type":     "read_ok",
		"messages": messages,
	})
}

func (nm *nodeMetadata) handleBroadcast(msg maelstrom.Message) error {
	body := struct {
		Message int `json:"message"`
	}{}

	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	nm.messagesMu.Lock()
	nm.messages = append(nm.messages, body.Message)
	nm.messagesMu.Unlock()

	return nm.node.Reply(msg, map[string]any{
		"type": "broadcast_ok",
	})
}

func (m *nodeMetadata) handleTopology(msg maelstrom.Message) error {
	return m.node.Reply(msg, map[string]any{
		"type": "topology_ok",
	})
}
