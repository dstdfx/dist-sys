package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	rootCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	n := maelstrom.NewNode()
	kv := maelstrom.NewSeqKV(n)
	ns := newNodeServer(n, kv)

	n.Handle("add", ns.handleAdd)
	n.Handle("read", ns.handleRead)
	n.Handle("sync", ns.handleSync)

	go ns.runStateBroadcaster(rootCtx, 500*time.Millisecond)

	go func() {
		if err := n.Run(); err != nil {
			log.Fatal(err)
		}
	}()

	<-rootCtx.Done()
}

type nodeServer struct {
	node            *maelstrom.Node
	kv              *maelstrom.KV
	countersMu      sync.RWMutex
	currentCounters map[string]int
}

func newNodeServer(n *maelstrom.Node, kv *maelstrom.KV) *nodeServer {
	return &nodeServer{
		node:            n,
		kv:              kv,
		currentCounters: make(map[string]int, len(n.NodeIDs())),
	}
}

func (ns *nodeServer) handleRead(msg maelstrom.Message) error {
	var total int64

	ns.countersMu.RLock()
	for _, counter := range ns.currentCounters {
		total += int64(counter)
	}
	ns.countersMu.RUnlock()

	return ns.node.Reply(msg, map[string]interface{}{
		"type":  "read_ok",
		"value": total,
	})
}

func (ns *nodeServer) handleAdd(msg maelstrom.Message) error {
	body := struct {
		Delta int `json:"delta"`
	}{}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	key := fmt.Sprintf(keyFormat, ns.node.ID())

	var currentValue int

	for {
		// Read current value and check if the key exists
		value, notExists := ns.readCouterWithRetries(ns.node.ID())

		currentValue = value + body.Delta

		// Try to update the value, otherwise try again from the beginning
		err := ns.kv.CompareAndSwap(context.Background(), key, value, currentValue, notExists)
		if err == nil {
			break
		}

		if maelstrom.ErrorCode(err) != maelstrom.PreconditionFailed {
			return err
		}
	}

	// Update the local counter
	ns.countersMu.Lock()
	ns.currentCounters[ns.node.ID()] = max(currentValue, ns.currentCounters[ns.node.ID()])
	ns.countersMu.Unlock()

	return ns.node.Reply(msg, map[string]interface{}{
		"type": "add_ok",
	})
}

func (ns *nodeServer) handleSync(msg maelstrom.Message) error {
	body := struct {
		Counter int `json:"counter"`
	}{}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	ns.countersMu.Lock()
	ns.currentCounters[msg.Src] = max(body.Counter, ns.currentCounters[msg.Src])
	ns.countersMu.Unlock()

	return nil
}

const keyFormat = "counter-%s"

func (ns *nodeServer) readCouterWithRetries(nodeID string) (int, bool) {
	key := fmt.Sprintf(keyFormat, nodeID)

	for {
		value, err := ns.kv.ReadInt(context.Background(), key)
		if err == nil {
			return value, false
		}

		if maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist {
			return 0, true
		}

		time.Sleep(100 * time.Millisecond)
	}
}

func (ns *nodeServer) runStateBroadcaster(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:

			ns.countersMu.RLock()
			value := ns.currentCounters[ns.node.ID()]
			ns.countersMu.RUnlock()

			// TODO: pick N random nodes to broadcast to?
			for _, nodeID := range ns.node.NodeIDs() {
				if ctx.Err() != nil {
					return
				}

				if nodeID == ns.node.ID() {
					// Skip broadcasting to self
					continue
				}

				go func(nodeID string) {
					for {
						if err := ns.node.Send(nodeID, map[string]interface{}{
							"type":    "sync",
							"counter": value,
						}); err == nil {
							break
						}
					}
				}(nodeID)
			}
		}
	}
}
