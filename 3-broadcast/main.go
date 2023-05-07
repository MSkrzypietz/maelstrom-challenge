package main

import (
	"encoding/json"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"log"
)

type Topology = map[string][]string

var n *maelstrom.Node
var messages map[int]struct{}
var topology Topology

func main() {
	n = maelstrom.NewNode()
	messages = map[int]struct{}{}

	n.Handle("broadcast", broadcastHandler)
	n.Handle("read", readHandler)
	n.Handle("topology", topologyHandler)

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

func broadcastHandler(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	body["type"] = "broadcast_ok"
	message, ok := body["message"].(float64)
	if ok {
		messages[int(message)] = struct{}{}
	}

	delete(body, "message")

	return n.Reply(msg, body)
}

func readHandler(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	body["type"] = "read_ok"
	body["messages"] = getMessages()

	return n.Reply(msg, body)
}

func topologyHandler(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	body["type"] = "topology_ok"
	value, ok := body["topology"].(Topology)
	if ok {
		topology = value
	}

	delete(body, "topology")

	return n.Reply(msg, body)
}

func getMessages() []int {
	var result []int
	for message, _ := range messages {
		result = append(result, message)
	}
	return result
}
