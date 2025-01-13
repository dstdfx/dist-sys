package main

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()
	n.Handle("generate", func(msg maelstrom.Message) error {
		// Unmarshal the message body as an loosely-typed map.
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		nodeID := n.ID()
		if nodeID == "" {
			return errors.New("node ID not set")
		}

		uniqueID, err := uuid.NewRandom()
		if err != nil {
			return fmt.Errorf("failed to generate unique ID: %w", err)
		}

		// Build a unique ID by combining the generated UUID with the node ID.
		responseBody := make(map[string]any)
		responseBody["id"] = uniqueID.String() + "_" + nodeID
		responseBody["type"] = "generate_ok"

		return n.Reply(msg, responseBody)
	})

	if err := n.Run(); err != nil {
		panic(err)
	}
}
