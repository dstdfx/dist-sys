package main

import (
	"context"
	"encoding/json"
	"fmt"
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
	ns := newNodeServer(n, maelstrom.NewSeqKV(n), maelstrom.NewLinKV(n))

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
	logOffsetKeyFmt            = "log:%s:%d"                   // log:<log-id>:<offset> -> msg
	logOffsetKey               = "log:%s:offset"               // log:<log-id> -> offset counter
	logOffsetCommitedOffsetKey = "log:%s:last_commited_offset" // log:<log-id>:last_commited_offset -> last commited offset
)

func (ns *nodeServer) getLogMsgKey(logID string, offset int) string {
	return fmt.Sprintf(logOffsetKeyFmt, logID, offset)
}

func (ns *nodeServer) getLogOffsetKey(logID string) string {
	return fmt.Sprintf(logOffsetKey, logID)
}

func (ns *nodeServer) getLastCommitedOffsetKey(logID string) string {
	return fmt.Sprintf(logOffsetCommitedOffsetKey, logID)
}

func (ns *nodeServer) getNextOffset(logID string) int {
	// Get last offset from linearizable kv with retries and allocate the next offset
	for {
		offset, err := ns.linKV.ReadInt(context.Background(), ns.getLogOffsetKey(logID))
		if err != nil {
			if maelstrom.ErrorCode(err) != maelstrom.KeyDoesNotExist {
				// Retry
				continue
			}

			// Initial offset
			offset = -1
		}

		nextOffset := offset + 1
		err = ns.linKV.CompareAndSwap(context.Background(), ns.getLogOffsetKey(logID), offset, nextOffset, true)
		if err == nil {
			return nextOffset
		}
	}
}

func (ns *nodeServer) getLastOffset(logID string) int {
	// Get last offset from linearizable kv with retries
	for {
		offset, err := ns.linKV.ReadInt(context.Background(), ns.getLogOffsetKey(logID))
		if err == nil {
			return offset
		}

		if maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist {
			// Initial offset
			return -1
		}
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

	ctx := context.Background()
	var offset int

	// To write a message to the log (log:<log-id>:<offset> -> msg):
	// 1. Get the last offset for the log
	// 2. Write the message to the log
	// 3. Update the latest offset for the log
	// In case of failure, retry the whole process again
	for {
		nextOffset := ns.getNextOffset(body.Key)
		offset = nextOffset

		// Write the message to the log
		err := ns.seqKV.Write(ctx, ns.getLogMsgKey(body.Key, nextOffset), body.Msg)
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

			// Get last offset for the log
			lastOffset := ns.getLastOffset(key)
			if offset > lastOffset {
				// Start offset should be less than or equal to the last offset, abort

				return
			}

			logs := make([][]int, 0, lastOffset-offset+1)

			for i := offset; i <= lastOffset; i++ {
				log, ok := ns.readLogWithRetries(key, i)
				if !ok {
					// Key does not exist -> skip featching further
					break
				}

				logs = append(logs, []int{i, log}) // [offset, msg]
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

func (ns *nodeServer) readLogWithRetries(logID string, offset int) (int, bool) {
	for {
		logMsg, err := ns.seqKV.ReadInt(context.Background(), ns.getLogMsgKey(logID, offset))
		if err == nil {
			return logMsg, true
		}

		if maelstrom.ErrorCode(err) == maelstrom.KeyDoesNotExist {
			return -1, false
		}
	}
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
			lastOffset, err := ns.linKV.ReadInt(ctx, ns.getLastCommitedOffsetKey(key))
			if err != nil && maelstrom.ErrorCode(err) != maelstrom.KeyDoesNotExist {
				continue
			}

			if lastOffset >= offset {
				// Already commited
				break
			}

			err = ns.linKV.CompareAndSwap(ctx, ns.getLastCommitedOffsetKey(key), lastOffset, offset, true)
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
		offset, err := ns.linKV.ReadInt(context.Background(), ns.getLastCommitedOffsetKey(key))
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
