package main

import (
	"encoding/json"
	"log"
	"math/rand"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

var seen = make(map[int]struct{})
var seenMu = &sync.Mutex{}

var topology = make(map[string][]string)
var topMu = &sync.Mutex{}

func runBroadcast(n *maelstrom.Node, message any) {
	time.Sleep(time.Duration(rand.Intn(300)) * time.Millisecond)

	topMu.Lock()
	toSend := topology[n.ID()]
	topMu.Unlock()

	for _, s := range toSend {
		wait := make(chan struct{}, 1)
		n.RPC(s, map[string]any{
			"type":    "broadcast",
			"message": message,
		}, func(msg maelstrom.Message) error {
			close(wait)
			return nil
		})
		<-wait
		time.Sleep(time.Millisecond * 100)
	}
}

func main() {
	n := maelstrom.NewNode()

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		type broadcast struct {
			Type    string `json:"type"`
			Message int    `json:"message"`
		}

		var bMessage broadcast
		if err := json.Unmarshal(msg.Body, &bMessage); err != nil {
			return err
		}

		seenMu.Lock()
		_, ok := seen[bMessage.Message]
		if !ok {
			seen[bMessage.Message] = struct{}{}
			go runBroadcast(n, bMessage.Message)
		}
		seenMu.Unlock()

		return n.Reply(msg, map[string]string{"type": "broadcast_ok"})
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		seenMu.Lock()
		i := 0
		seenCopy := make([]int, len(seen))
		for k := range seen {
			seenCopy[i] = k
			i++
		}
		seenMu.Unlock()

		return n.Reply(msg, map[string]any{"type": "read_ok", "messages": seenCopy})
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		type topologyMessage struct {
			Topology map[string][]string
		}

		var tMsg topologyMessage
		if err := json.Unmarshal(msg.Body, &tMsg); err != nil {
			return err
		}

		topMu.Lock()
		topology = tMsg.Topology
		topMu.Unlock()

		return n.Reply(msg, map[string]any{"type": "topology_ok"})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
