package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	rootCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	n := maelstrom.NewNode()
	kv := maelstrom.NewLinKV(n)
	ns := newNodeServer(n, kv)

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
	kv   *maelstrom.KV
}

func newNodeServer(n *maelstrom.Node, kv *maelstrom.KV) *nodeServer {
	return &nodeServer{
		node: n,
		kv:   kv,
	}
}

const (
	logKeyPrefix = "log:"
	logOffsetFmt = logKeyPrefix + "%s:offset"
)

func (ns *nodeServer) handleSend(msg maelstrom.Message) error {
	body := struct {
		Key string `json:"key"`
		Msg int    `json:"msg"`
	}{}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	ctx := context.Background()
	var offset int

	// log:<log id> -> [msg1, msg2, ...]

	for {
		lastLog := make([]int, 0)

		err := ns.kv.ReadInto(ctx, logKeyPrefix+body.Key, &lastLog)
		if err != nil && maelstrom.ErrorCode(err) != maelstrom.KeyDoesNotExist {
			continue
		}

		var log []int
		if len(lastLog) != 0 {
			log = make([]int, len(lastLog))
			copy(log, lastLog)
		} else {
			log = make([]int, 0, 1)
		}

		// Append a message to the log and get its offset
		log = append(log, body.Msg)
		offset = len(log) - 1

		// Attempt to CAS the log, otherwise retry
		err = ns.kv.CompareAndSwap(ctx, logKeyPrefix+body.Key, lastLog, log, true)
		if err == nil {
			break
		}
	}

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

	// Read the logs from kv and return requested offsets
	for key, offset := range body.Offsets {
		log, ok := ns.readLogWithRetries(key)
		if !ok {
			continue
		}

		respMsgs[key] = make([][]int, 0, len(log)-offset)

		for i := offset; i < len(log); i++ {
			respMsgs[key] = append(respMsgs[key], []int{i, log[i]}) // [offset, msg]
		}
	}

	return ns.node.Reply(msg, map[string]any{
		"type": "poll_ok",
		"msgs": respMsgs,
	})
}

func (ns *nodeServer) readLogWithRetries(logID string) ([]int, bool) {
	var log []int
	for {
		err := ns.kv.ReadInto(context.Background(), logKeyPrefix+logID, &log)
		if err == nil {
			break
		}

		if maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist {
			return nil, false
		}
	}

	return log, true
}

func (ns *nodeServer) handleCommitOffsets(msg maelstrom.Message) error {
	body := struct {
		Offsets map[string]int `json:"offsets"`
	}{}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	ctx := context.Background()

	for key, offset := range body.Offsets {
		offsetKey := fmt.Sprintf(logOffsetFmt, key)

		for {
			lastOffset, err := ns.kv.ReadInt(ctx, offsetKey)
			if err != nil && maelstrom.ErrorCode(err) != maelstrom.KeyDoesNotExist {
				continue
			}

			// Attempt to CAS the log's offset, otherwise retry
			err = ns.kv.CompareAndSwap(ctx, offsetKey, lastOffset, offset, true)
			if err == nil {
				break
			}
		}
	}

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

	for _, key := range body.Keys {
		offset, err := ns.kv.ReadInt(context.Background(), fmt.Sprintf(logOffsetFmt, key))
		if err != nil {
			continue
		}

		commited[key] = offset
	}

	return ns.node.Reply(msg, map[string]any{
		"type":    "list_committed_offsets_ok",
		"offsets": commited,
	})
}
