package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"math/rand"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()
	kv := maelstrom.NewSeqKV(n)

	n.Handle("read", func(msg maelstrom.Message) error {
		val, err := kv.ReadInt(context.Background(), "counter")

		var rpcErr *maelstrom.RPCError
		if !errors.As(err, &rpcErr) {
			return err
		}

		return n.Reply(msg, map[string]any{
			"type":  "read_ok",
			"value": val,
		})
	})

	type addReq struct {
		Delta int `json:"delta"`
	}

	n.Handle("add", func(msg maelstrom.Message) error {
		var a addReq
		if err := json.Unmarshal(msg.Body, &a); err != nil {
			return err
		}

		for {
			val, err := kv.ReadInt(context.Background(), "counter")
			if err != nil && maelstrom.ErrorCode(err) != maelstrom.KeyDoesNotExist {
				return err
			}

			newVal := val + a.Delta

			err = kv.CompareAndSwap(context.Background(), "counter", val, newVal, true)
			if err != nil {
				switch maelstrom.ErrorCode(err) {
				case maelstrom.PreconditionFailed:
					time.Sleep(time.Duration(rand.Intn(10)) * time.Millisecond)
					continue
				default:
					return err
				}
			}

			return n.Reply(msg, map[string]any{
				"type": "add_ok",
			})
		}
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}

}
