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

	n.Handle("send", ns.handleSend)
	n.Handle("poll", ns.handlePoll)
	n.Handle("commit_offsets", ns.handleCommitOffsets)
	n.Handle("list_committed_offsets", ns.handleListCommitedOffsets)

	go func() {
		if err := n.Run(); err != nil {
			log.Fatal(err)
		}
	}()

	<-rootCtx.Done()
}

type nodeServer struct {
	node *maelstrom.Node

	logsMu sync.RWMutex
	logs   map[string][]int // map of logs, each log is a list of msgs

	offsetsMu sync.RWMutex
	offsets   map[string]int // key <-> last commited offset
}

func newNodeServer(n *maelstrom.Node) *nodeServer {
	return &nodeServer{
		node:    n,
		logs:    make(map[string][]int, 0),
		offsets: make(map[string]int, 0),
	}
}

func (ns *nodeServer) handleSend(msg maelstrom.Message) error {
	body := struct {
		Key string `json:"key"`
		Msg int    `json:"msg"`
	}{}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	ns.logsMu.Lock()

	if _, ok := ns.logs[body.Key]; !ok {
		// Init new log
		ns.logs[body.Key] = make([]int, 0)
	}

	// Append a message to the log and get its offset, in our case
	// it's the length of the log
	ns.logs[body.Key] = append(ns.logs[body.Key], body.Msg)
	offset := len(ns.logs[body.Key]) - 1

	ns.logsMu.Unlock()

	return ns.node.Reply(msg, map[string]any{
		"type":   "send_ok",
		"offset": offset,
	})
}

func (ns *nodeServer) handlePoll(msg maelstrom.Message) error {
	body := struct {
		Offsets map[string]int `json:"offsets"`
	}{}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	respMsgs := make(map[string][][]int)

	ns.logsMu.RLock()

	for key, offset := range body.Offsets {
		if _, ok := ns.logs[key]; !ok {
			continue
		}

		respMsgs[key] = make([][]int, 0, len(ns.logs[key])-offset)

		for i := offset; i < len(ns.logs[key]); i++ {
			respMsgs[key] = append(respMsgs[key], []int{i, ns.logs[key][i]}) // [offset, msg]
		}
	}

	ns.logsMu.RUnlock()

	return ns.node.Reply(msg, map[string]any{
		"type": "poll_ok",
		"msgs": respMsgs,
	})
}

func (ns *nodeServer) handleCommitOffsets(msg maelstrom.Message) error {
	body := struct {
		Offsets map[string]int `json:"offsets"`
	}{}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	ns.offsetsMu.Lock()

	for key, offset := range body.Offsets {
		ns.offsets[key] = offset
	}

	ns.offsetsMu.Unlock()

	return ns.node.Reply(msg, map[string]any{
		"type": "commit_offsets_ok",
	})
}

func (ns *nodeServer) handleListCommitedOffsets(msg maelstrom.Message) error {
	body := struct {
		Keys []string `json:"keys"`
	}{}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	commited := make(map[string]int, len(body.Keys))

	ns.offsetsMu.RLock()

	for _, key := range body.Keys {
		if offset, ok := ns.offsets[key]; ok {
			commited[key] = offset
		}
	}

	ns.offsetsMu.RUnlock()

	return ns.node.Reply(msg, map[string]any{
		"type":    "list_committed_offsets_ok",
		"offsets": commited,
	})
}
