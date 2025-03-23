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
	n.Handle("sync", ns.handleSync)

	go func() {
		if err := n.Run(); err != nil {
			log.Fatal(err)
		}
	}()

	go ns.writeReplicator(rootCtx)

	<-rootCtx.Done()
}

type nodeServer struct {
	node        *maelstrom.Node
	storage     *kvStorage
	writeReplCh chan replEvent

	lamportMu      sync.Mutex
	lamportVersion int
}

func newNodeServer(n *maelstrom.Node) *nodeServer {
	return &nodeServer{
		node:        n,
		storage:     &kvStorage{storage: make(map[float64]valueEntity)},
		writeReplCh: make(chan replEvent),
	}
}

type txOp string

const (
	readOp  txOp = "r"
	writeOp txOp = "w"
)

type replEvent struct {
	Key      float64 `json:"key"`
	Value    float64 `json:"value"`
	Version  int     `json:"version"`
	TxNodeID string  `json:"txnode_id"`
}

func (ns *nodeServer) writeReplicator(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case txn := <-ns.writeReplCh:
			for _, n := range ns.node.NodeIDs() {
				if n == ns.node.ID() {
					continue
				}

				if err := ns.node.Send(n, map[string]any{
					"type": "sync",
					"repl": txn,
				}); err != nil {
					log.Println(err)
				}
			}
		}
	}
}

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
			txResult = append(txResult, []any{operation, key, ns.storage.read(key)})
		} else {
			// increment the lamport version
			lamportClock := ns.getNextLamportClock()

			// write the value
			val = op[2].(float64)
			ns.storage.write(key, val, lamportClock, ns.node.ID())

			// append the result
			txResult = append(txResult, []any{operation, key, val})

			// replicate the write
			ns.writeReplCh <- replEvent{
				Key:      key,
				Value:    val,
				Version:  lamportClock,
				TxNodeID: ns.node.ID(),
			}
		}
	}

	return ns.node.Reply(msg, map[string]any{
		"type": "txn_ok",
		"txn":  txResult,
	})
}

func (ns *nodeServer) getNextLamportClock() int {
	ns.lamportMu.Lock()
	defer ns.lamportMu.Unlock()

	ns.lamportVersion++

	return ns.lamportVersion
}

func (ns *nodeServer) handleSync(msg maelstrom.Message) error {
	body := struct {
		Repl replEvent `json:"repl"`
	}{}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	// try to apply the write
	ok := ns.storage.writeWithVersionCheck(body.Repl.Key, body.Repl.Value, body.Repl.Version, body.Repl.TxNodeID)
	if !ok {
		return nil
	}

	// update the lamport version
	ns.lamportMu.Lock()
	ns.lamportVersion = max(ns.lamportVersion, body.Repl.Version)
	ns.lamportMu.Unlock()

	return nil
}

type kvStorage struct {
	mu      sync.RWMutex
	storage map[float64]valueEntity
}

type valueEntity struct {
	val     float64
	version int

	// txNodeID is a node identifier of a node that wrote last state.
	// Used to determine the order of writes and to prevent write conflicts,
	// e.x when two nodes have the same lamport clock value and they handle the same key update at the same time.
	// In this case, the node with the lexicografically higher node ID will win.
	txNodeID string
}

func (kvs *kvStorage) read(key float64) any {
	kvs.mu.RLock()
	defer kvs.mu.RUnlock()

	val, ok := kvs.storage[key]
	if !ok {
		return "null"
	}

	return val
}

func (kvs *kvStorage) write(key, val float64, version int, txNodeID string) {
	kvs.mu.Lock()
	kvs.storage[key] = valueEntity{val: val, version: version, txNodeID: txNodeID}
	kvs.mu.Unlock()
}

func (kvs *kvStorage) writeWithVersionCheck(key, val float64, version int, txNodeID string) bool {
	kvs.mu.Lock()
	defer kvs.mu.Unlock()

	// Apply the write if either:
	// - there is no value for the key
	// - the new version is greater than the current version
	// - the new version is equal to the current version but the new txNodeID is lexicographically higher
	ve, ok := kvs.storage[key]
	if !ok || ve.version < version || (ve.version == version && ve.txNodeID < txNodeID) {
		kvs.storage[key] = valueEntity{val: val, version: version}

		return true
	}

	return false
}
