package handle

import (
	"encoding/json"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type nodeStorage interface {
	UpdateTopology(topology []string)
	GetCurrentTopology() []string
	StoreMessage(message float64) (isAdded bool)
	GetCurrentMessages() []float64
}

type handler struct {
	node    *maelstrom.Node
	storage nodeStorage
}

func New(node *maelstrom.Node, storage nodeStorage) *handler {
	return &handler{
		node:    node,
		storage: storage,
	}
}

func (h *handler) HandleBroadcast(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	m := body["message"].(float64)

	// Remember the received message
	// if message is already in the list - we don't broadcast it again.
	if h.storage.StoreMessage(m) {
		// Broadcase new message to all connected nodes
		for _, node := range h.storage.GetCurrentTopology() {
			if msg.Src == node {
				// Skip the node that sent the message
				continue
			}

			if err := h.node.Send(node, body); err != nil {
				return err
			}
		}
	}

	delete(body, "message") // remove the message from the message
	body["type"] = "broadcast_ok"

	if body["msg_id"] == nil {
		// Skip the reply if the message is not a request
		return nil
	}

	return h.node.Reply(msg, body)
}

func (h *handler) HandleRead(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	body["messages"] = h.storage.GetCurrentMessages()
	body["type"] = "read_ok"

	return h.node.Reply(msg, body)
}

func (h *handler) HandleTopology(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	// Parse specific topology message
	type topologyMessage struct {
		MessageType string              `json:"type"`
		Topology    map[string][]string `json:"topology"`
	}

	topologyMsg := topologyMessage{}
	if err := json.Unmarshal(msg.Body, &topologyMsg); err != nil {
		return err
	}

	// Get the topology from the message and update the local topology
	h.storage.UpdateTopology(topologyMsg.Topology[h.node.ID()])

	delete(body, "topology") // remove the topology from the message

	body["type"] = "topology_ok"

	return h.node.Reply(msg, body)
}
