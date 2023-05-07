package main

import (
	"encoding/json"
	"fmt"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	"log"
	"os"
	"sync"
	"time"
)

type Topology = map[string][]string

var n *maelstrom.Node
var messages map[int]struct{}
var messagesMutex sync.Mutex
var topology Topology

func main() {
	n = maelstrom.NewNode()
	messages = map[int]struct{}{}
	topology = map[string][]string{}

	n.Handle("broadcast", broadcastHandler)
	n.Handle("read", readHandler)
	n.Handle("topology", topologyHandler)
	n.Handle("sync", syncHandler)
	n.Handle("sync_ok", func(msg maelstrom.Message) error { return nil })

	go syncJob(500 * time.Millisecond)

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

type TopologyMessage struct {
	Type     string   `json:"type"`
	Topology Topology `json:"topology,omitempty"`
}

func topologyHandler(msg maelstrom.Message) error {
	var body TopologyMessage
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	body.Type = "topology_ok"
	topology = body.Topology
	body.Topology = nil

	return n.Reply(msg, body)
}

func getMessages() []int {
	messagesMutex.Lock()
	defer messagesMutex.Unlock()

	var result []int
	for message, _ := range messages {
		result = append(result, message)
	}
	return result
}

type SyncMessage struct {
	Type     string `json:"type"`
	Messages []int  `json:"messages,omitempty"`
}

func syncHandler(msg maelstrom.Message) error {
	var body SyncMessage
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	body.Type = "sync_ok"
	addMessages(body.Messages)
	body.Messages = nil

	return n.Reply(msg, body)
}

func addMessages(newMessages []int) {
	messagesMutex.Lock()
	defer messagesMutex.Unlock()

	for message := range newMessages {
		messages[message] = struct{}{}
	}
}

func syncJob(duration time.Duration) {
	for range time.Tick(duration) {
		go syncWithNeighbors()
	}
}

func syncWithNeighbors() {
	for _, neighborID := range topology[n.ID()] {
		body := SyncMessage{
			Type:     "sync",
			Messages: getMessages(),
		}
		err := n.Send(neighborID, body)
		if err != nil {
			fmt.Fprintf(os.Stderr, "[syncWithNeighbors]: neighborID: %v - err: %v ", neighborID, err)
		}
	}
}
