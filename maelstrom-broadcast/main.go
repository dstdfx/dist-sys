package main

import (
	"maelstrom-broadcast/handle"
	"maelstrom-broadcast/storage"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()
	nodeStorage := storage.New()
	handler := handle.New(n, nodeStorage)

	n.Handle("broadcast", handler.HandleBroadcast)
	n.Handle("read", handler.HandleRead)
	n.Handle("topology", handler.HandleTopology)

	if err := n.Run(); err != nil {
		panic(err)
	}
}
