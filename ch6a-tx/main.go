package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	rootCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	n := maelstrom.NewNode()
	ns := newNodeServer(n)

	n.Handle("txn", ns.handleTXN)

	go func() {
		if err := n.Run(); err != nil {
			log.Fatal(err)
		}
	}()

	<-rootCtx.Done()
}

type nodeServer struct {
	node    *maelstrom.Node
	mu      sync.RWMutex
	storage map[float64]float64
}

func newNodeServer(n *maelstrom.Node) *nodeServer {
	return &nodeServer{
		node:    n,
		storage: make(map[float64]float64),
	}
}

type txOp string

const (
	readOp  txOp = "r"
	writeOp txOp = "w"
)

func (ns *nodeServer) handleTXN(msg maelstrom.Message) error {
	body := struct {
		TXN [][]any `json:"txn"`
	}{}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	txResult := make([][]any, 0, len(body.TXN))

	var (
		operation string
		key       float64
		val       float64
	)

	// parse and perform the transaction
	for _, op := range body.TXN {
		// assume that values are always valid
		operation = op[0].(string)
		key = op[1].(float64)

		if txOp(operation) == readOp {
			txResult = append(txResult, []any{operation, key, ns.read(key)})
		} else {
			val = op[2].(float64)
			ns.write(key, val)
			txResult = append(txResult, []any{operation, key, val})
		}
	}

	return ns.node.Reply(msg, map[string]any{
		"type": "txn_ok",
		"txn":  txResult,
	})
}

func (ns *nodeServer) read(key float64) any {
	ns.mu.RLock()
	defer ns.mu.RUnlock()

	val, ok := ns.storage[key]
	if !ok {
		return "null"
	}

	return val
}

func (ns *nodeServer) write(key, val float64) {
	ns.mu.Lock()
	ns.storage[key] = val
	ns.mu.Unlock()
}
