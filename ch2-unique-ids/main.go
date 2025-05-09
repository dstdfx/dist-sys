package main

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"

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

		responseBody := make(map[string]any)

		// TODO: implement sortable unique ids based on timestamp (snowflake like)

		// Generate a random string and combine it with node id to create a unique id.
		responseBody["id"] = fmt.Sprintf("%s-%s", generateRandomString(16), n.ID())
		responseBody["type"] = "generate_ok"

		return n.Reply(msg, responseBody)
	})

	if err := n.Run(); err != nil {
		panic(err)
	}
}

func generateRandomString(length int) string {
	bytes := make([]byte, length)
	_, err := rand.Read(bytes)
	if err != nil {
		panic(err)
	}

	return base64.URLEncoding.EncodeToString(bytes)
}
