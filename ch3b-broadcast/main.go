package main

import (
	"encoding/json"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()
	ns := newNodeServer(n)

	n.Handle("broadcast", ns.handleBroadcast)
	n.Handle("read", ns.handleRead)
	n.Handle("topology", ns.handleTopology)

	if err := n.Run(); err != nil {
		panic(err)
	}
}

type nodeServer struct {
	node       *maelstrom.Node
	messagesMu sync.RWMutex
	messages   map[int]struct{}

	neighborsMu sync.RWMutex
	neighbors   []string
}

func newNodeServer(n *maelstrom.Node) *nodeServer {
	return &nodeServer{
		node:     n,
		messages: make(map[int]struct{}, 0),
	}
}

func (ns *nodeServer) handleRead(msg maelstrom.Message) error {
	ns.messagesMu.RLock()
	m := ns.messages
	ns.messagesMu.RUnlock()

	messages := make([]int, 0, len(m))
	for k := range m {
		messages = append(messages, k)
	}

	return ns.node.Reply(msg, map[string]any{
		"type":     "read_ok",
		"messages": messages,
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

	isDuplicateMsg := false

	// Save the message to the node's state
	ns.messagesMu.Lock()
	_, isDuplicateMsg = ns.messages[body.Message]
	if !isDuplicateMsg {
		ns.messages[body.Message] = struct{}{}
	}
	ns.messagesMu.Unlock()

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

	// Broadcast the message to all node's neighbors
	ns.neighborsMu.RLock()
	neighbors := ns.neighbors
	ns.neighborsMu.RUnlock()

	wg := sync.WaitGroup{}
	wg.Add(len(neighbors))

	for _, neighbor := range neighbors {
		if msg.Src == neighbor {
			wg.Done()

			// Skip the neighbor that sent the message initially
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
