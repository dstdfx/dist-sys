package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sort"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	rootCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	n := maelstrom.NewNode()
	ns := newNodeServer(n, maelstrom.NewLinKV(n), maelstrom.NewSeqKV(n))

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
	node  *maelstrom.Node
	linKV *maelstrom.KV
	seqKV *maelstrom.KV
}

func newNodeServer(n *maelstrom.Node, linKV, seqKV *maelstrom.KV) *nodeServer {
	return &nodeServer{
		node:  n,
		linKV: linKV,
		seqKV: seqKV,
	}
}

const (
	logKeyFmt                  = "log:%s"                      // log:<log-id> -> [{offset, msg}...]
	logOffsetCommitedOffsetKey = "log:%s:last_commited_offset" // log:<log-id>:last_commited_offset -> last commited offset
)

func (ns *nodeServer) getLogMsgKey(logID string) string {
	return fmt.Sprintf(logKeyFmt, logID)
}

func (ns *nodeServer) getLastCommitedOffsetKey(logID string) string {
	return fmt.Sprintf(logOffsetCommitedOffsetKey, logID)
}

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
	for {
		// Read the log entries to get last offset
		entries := make([][]int, 0)
		err := ns.linKV.ReadInto(context.Background(), ns.getLogMsgKey(body.Key), &entries)
		if err != nil {
			if maelstrom.ErrorCode(err) != maelstrom.KeyDoesNotExist {
				// Retry
				continue
			}

			// Initial offset
			offset = 0
		} else {
			offset = entries[len(entries)-1][0] + 1
		}

		copyEntries := make([][]int, len(entries))
		copy(copyEntries, entries)

		err = ns.linKV.CompareAndSwap(ctx, ns.getLogMsgKey(body.Key), copyEntries, append(entries, []int{offset, body.Msg}), true)
		if err == nil {
			break
		}
	}

	return ns.node.Reply(msg, map[string]any{
		"type":   "send_ok",
		"offset": offset,
	})
}

type keyEntry struct {
	key  string
	logs [][]int
}

func (ns *nodeServer) handlePoll(msg maelstrom.Message) error {
	body := struct {
		Offsets map[string]int `json:"offsets"`
	}{}
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	logsCh := make(chan keyEntry, len(body.Offsets))

	wg := sync.WaitGroup{}
	wg.Add(len(body.Offsets))

	// Read the logs from kv and return requested offsets
	for key, offset := range body.Offsets {
		go func() {
			defer wg.Done()

			// Read the log entries
			entries := make([][]int, 0)
			err := ns.linKV.ReadInto(context.Background(), ns.getLogMsgKey(key), &entries)
			if err != nil {
				return
			}

			logs := make([][]int, 0, entries[len(entries)-1][0]-offset+1)

			// Find the leftmost index of the offset
			leftmostIndex := sort.Search(len(entries), func(i int) bool {
				return entries[i][0] >= offset
			})

			// Append the logs from the offset
			for i := leftmostIndex; i < len(entries); i++ {
				logs = append(logs, entries[i]) // [offset, msg]
			}

			if len(logs) == 0 {
				return
			}

			logsCh <- keyEntry{key, logs}
		}()
	}

	wg.Wait()
	close(logsCh)

	respMsgs := make(map[string][][]int, len(body.Offsets))
	for logs := range logsCh {
		respMsgs[logs.key] = logs.logs
	}

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

	ctx := context.Background()
	for key, offset := range body.Offsets {

		// Update last commited offset in linearizable kv with retries
		for {
			lastOffset, err := ns.seqKV.ReadInt(ctx, ns.getLastCommitedOffsetKey(key))
			if err != nil && maelstrom.ErrorCode(err) != maelstrom.KeyDoesNotExist {
				continue
			}

			if lastOffset >= offset {
				// Already commited
				break
			}

			err = ns.seqKV.CompareAndSwap(ctx, ns.getLastCommitedOffsetKey(key), lastOffset, offset, true)
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
		offset, err := ns.seqKV.ReadInt(context.Background(), ns.getLastCommitedOffsetKey(key))
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
