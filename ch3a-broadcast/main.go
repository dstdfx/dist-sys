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

type nodeServer struct {
	node       *maelstrom.Node
	messagesMu sync.RWMutex
	messages   []int
}

func newNodeMetadata(n *maelstrom.Node) *nodeServer {
	return &nodeServer{node: n}
}

func (ns *nodeServer) handleRead(msg maelstrom.Message) error {
	ns.messagesMu.RLock()
	messages := ns.messages
	ns.messagesMu.RUnlock()

	return ns.node.Reply(msg, map[string]any{
		"type":     "read_ok",
		"messages": messages,
	})
}

func (ns *nodeServer) handleBroadcast(msg maelstrom.Message) error {
	body := struct {
		Message int `json:"message"`
	}{}

	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	ns.messagesMu.Lock()
	ns.messages = append(ns.messages, body.Message)
	ns.messagesMu.Unlock()

	return ns.node.Reply(msg, map[string]any{
		"type": "broadcast_ok",
	})
}

func (ns *nodeServer) handleTopology(msg maelstrom.Message) error {
	return ns.node.Reply(msg, map[string]any{
		"type": "topology_ok",
	})
}
