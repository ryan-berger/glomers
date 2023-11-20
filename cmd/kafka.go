package main

import (
	"encoding/json"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type logMsg struct {
	log    any
	offset int
}

var logMu = sync.Mutex{}
var logs = make(map[string][]logMsg)
var committedTo = make(map[string]int)
var offset = 0

func main() {
	n := maelstrom.NewNode()

	n.Handle("send", func(msg maelstrom.Message) error {
		type sendMessage struct {
			Key string
			Msg any
		}

		var sendReq sendMessage
		json.Unmarshal(msg.Body, &sendReq)

		logMu.Lock()
		if commit := committedTo[sendReq.Key]; commit > offset+1 {
			offset = commit
		}

		offset++
		logs[sendReq.Key] = append(logs[sendReq.Key], logMsg{log: sendReq.Msg, offset: offset})
		logMu.Unlock()

		return n.Reply(msg, map[string]any{"type": "send_ok", "offset": offset})
	})

	n.Handle("poll", func(msg maelstrom.Message) error {
		type pollMsg struct {
			Offsets map[string]int
		}

		var poll pollMsg
		json.Unmarshal(msg.Body, &poll)

		logLists := make(map[string][][2]any)
		logMu.Lock()
		for k, offset := range poll.Offsets {
			logResp := make([][2]any, 0)
			for _, l := range logs[k] {
				if l.offset < offset {
					continue
				}
				logResp = append(logResp, [2]any{l.offset, l.log})
			}
			logLists[k] = logResp
		}
		logMu.Unlock()

		return n.Reply(msg, map[string]any{"type": "poll_ok", "msgs": logLists})
	})

	n.Handle("commit_offsets", func(msg maelstrom.Message) error {
		type commitOffsetMsg struct {
			Offsets map[string]int
		}

		var commitMsg commitOffsetMsg
		json.Unmarshal(msg.Body, &commitMsg)

		logMu.Lock()
		for k, offset := range commitMsg.Offsets {
			if offset > committedTo[k] {
				committedTo[k] = offset
			}
		}
		logMu.Unlock()

		return n.Reply(msg, map[string]any{"type": "commit_offsets_ok"})
	})

	n.Handle("list_committed_offsets", func(msg maelstrom.Message) error {
		type sendMessage struct {
			Keys string
		}

		logMu.Lock()
		commits := make(map[string]int, len(committedTo))
		for k, v := range committedTo {
			commits[k] = v
		}
		logMu.Unlock()

		return n.Reply(msg, map[string]any{"type": "list_committed_offsets_ok", "offsets": commits})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
