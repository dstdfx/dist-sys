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
	writeReplCh chan map[float64]valueEntity
}

func newNodeServer(n *maelstrom.Node) *nodeServer {
	return &nodeServer{
		node:        n,
		storage:     &kvStorage{storage: make(map[float64]valueEntity)},
		writeReplCh: make(chan map[float64]valueEntity),
	}
}

type txOp string

const (
	readOp  txOp = "r"
	writeOp txOp = "w"
)

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

	// Transaction handling:
	// 1. Get the latest snapshot of the storage
	// 2. Apply the transaction operations to the snapshot
	// 3. Replicate the changes to other nodes

	// get latest snapshot of the storage
	snapshot := ns.storage.getSnapshot()

	// map to store the values to be replicated on other nodes
	toRepl := make(map[float64]valueEntity, len(snapshot))

	// parse and perform the transaction
	for _, op := range body.TXN {
		// assume that values are always valid
		operation = op[0].(string)
		key = op[1].(float64)

		if txOp(operation) == readOp {
			// read the value from the snapshot
			var readVal any
			snapshotVal, ok := snapshot[key]
			if !ok {
				readVal = "null"
			} else {
				readVal = snapshotVal.val
			}

			txResult = append(txResult, []any{operation, key, readVal})
		} else {
			// increment the lamport version
			lamportClock := ns.storage.getNextLamportClock()

			// write the value
			val = op[2].(float64)

			newEntry := valueEntity{val: val, version: lamportClock, txNodeID: ns.node.ID()}

			// write the value to the snapshot and replicate storage
			snapshot[key] = newEntry
			toRepl[key] = newEntry

			// append the result
			txResult = append(txResult, []any{operation, key, val})
		}
	}

	// apply the changes to the storage
	ns.storage.mergeSnapshot(toRepl)

	// replicate the snapshot to other nodes if there are any changes
	if len(toRepl) != 0 {
		ns.writeReplCh <- toRepl
	}

	return ns.node.Reply(msg, map[string]any{
		"type": "txn_ok",
		"txn":  txResult,
	})
}

func (ns *nodeServer) handleSync(msg maelstrom.Message) error {
	body := struct {
		Repl map[float64]valueEntity `json:"repl"`
	}{}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	ns.storage.mergeSnapshot(body.Repl)

	return nil
}

type kvStorage struct {
	mu      sync.RWMutex
	storage map[float64]valueEntity

	lamportMu      sync.Mutex
	lamportVersion int
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

func (kvs *kvStorage) getSnapshot() map[float64]valueEntity {
	kvs.mu.RLock()
	defer kvs.mu.RUnlock()

	snapshot := make(map[float64]valueEntity, len(kvs.storage))
	for k, v := range kvs.storage {
		snapshot[k] = v
	}

	return snapshot
}

func (kvs *kvStorage) mergeSnapshot(snapshot map[float64]valueEntity) {
	kvs.mu.Lock()
	defer kvs.mu.Unlock()

	for k, v := range snapshot {
		// Apply the write if either:
		// - there is no value for the key
		// - the new version is greater than the current version
		// - the new version is equal to the current version but the new txNodeID is lexicographically higher
		var updated bool

		ve, ok := kvs.storage[k]
		if !ok || ve.version < v.version || (ve.version == v.version && ve.txNodeID < v.txNodeID) {
			kvs.storage[k] = valueEntity{val: v.val, version: v.version}

			updated = true
		}

		if updated {
			// update the lamport version
			kvs.lamportMu.Lock()
			kvs.lamportVersion = max(kvs.lamportVersion, v.version)
			kvs.lamportMu.Unlock()
		}
	}
}

func (kvs *kvStorage) getNextLamportClock() int {
	kvs.lamportMu.Lock()
	defer kvs.lamportMu.Unlock()

	kvs.lamportVersion++

	return kvs.lamportVersion
}
