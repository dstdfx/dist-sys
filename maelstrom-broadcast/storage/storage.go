package storage

import "sync"

// Node is a simple in-memory storage to keep track of the topology and received messages.
type Node struct {
	topologyMu sync.RWMutex
	topology   []string // list of connected nodes we need to broadcast to

	messagesMu sync.RWMutex
	messages   map[float64]struct{} // unique set of messages, each message is unique in maelstrom
}

func New() *Node {
	return &Node{
		topology: make([]string, 0),
		messages: make(map[float64]struct{}, 0),
	}
}

func (ns *Node) UpdateTopology(topology []string) {
	ns.topologyMu.Lock()
	defer ns.topologyMu.Unlock()

	ns.topology = topology
}

func (ns *Node) GetCurrentTopology() []string {
	ns.topologyMu.RLock()
	defer ns.topologyMu.RUnlock()

	return ns.topology
}

func (ns *Node) StoreMessage(message float64) (isAdded bool) {
	ns.messagesMu.Lock()
	defer ns.messagesMu.Unlock()

	if _, ok := ns.messages[message]; ok {
		return
	}

	ns.messages[message] = struct{}{}
	isAdded = true

	return
}

func (ns *Node) GetCurrentMessages() []float64 {
	ns.messagesMu.RLock()
	defer ns.messagesMu.RUnlock()

	messages := make([]float64, 0, len(ns.messages))
	for message := range ns.messages {
		messages = append(messages, message)
	}

	return messages
}
