package main

import (
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
    n := maelstrom.NewNode()

    var values []int

    n.Handle("broadcast", func(msg maelstrom.Message) error {
        var body map[string]any
        if err := json.Unmarshal(msg.Body, &body); err != nil {
            return err
        }

        body["type"] = "broadcast_ok"
        value, ok := body["message"].(float64)
        if ok {
            values = append(values, int(value))
        } 
        
        delete(body, "message")

        return n.Reply(msg, body)
    })

    n.Handle("read", func(msg maelstrom.Message) error {
        var body map[string]any
        if err := json.Unmarshal(msg.Body, &body); err != nil {
            return err
        }

        body["type"] = "read_ok"
        body["messages"] = values

        return n.Reply(msg, body)
    })

    n.Handle("topology", func(msg maelstrom.Message) error {
        var body map[string]any
        if err := json.Unmarshal(msg.Body, &body); err != nil {
            return err
        }

        body["type"] = "topology_ok"

        delete(body, "topology")

        return n.Reply(msg, body)
    })

    if err := n.Run(); err != nil {
        log.Fatal(err)
    }
}
