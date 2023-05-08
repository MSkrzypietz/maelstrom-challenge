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
type Messages map[int]struct{}

var n *maelstrom.Node
var messages Messages
var messagesMutex sync.Mutex
var knownSyncMessages map[string]Messages
var knownSyncMessagesMutex sync.Mutex
var topology Topology

func main() {
	n = maelstrom.NewNode()
	messages = map[int]struct{}{}
	knownSyncMessages = map[string]Messages{}
	topology = map[string][]string{}

	n.Handle("broadcast", broadcastHandler)
	n.Handle("read", readHandler)
	n.Handle("topology", topologyHandler)
	n.Handle("sync", syncHandler)

	go syncScheduler(500 * time.Millisecond)

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
		messagesMutex.Lock()
		messages[int(message)] = struct{}{}
		messagesMutex.Unlock()
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
	body["messages"] = getAllMessages()

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

func getAllMessages() []int {
	messagesMutex.Lock()
	defer messagesMutex.Unlock()

	var result []int
	for message := range messages {
		result = append(result, message)
	}
	return result
}

func getUnknownMessages(nodeID string) []int {
	allMessages := getAllMessages()

	knownSyncMessagesMutex.Lock()
	defer knownSyncMessagesMutex.Unlock()

	var result []int
	for _, message := range allMessages {
		if _, ok := knownSyncMessages[nodeID][message]; !ok {
			result = append(result, message)
		}
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
	addKnownSyncMessages(msg.Src, body.Messages)
	body.Messages = getUnknownMessages(msg.Src)

	return n.Reply(msg, body)
}

func addMessages(newMessages []int) {
	if len(newMessages) == 0 {
		return
	}

	messagesMutex.Lock()
	defer messagesMutex.Unlock()

	for _, message := range newMessages {
		messages[message] = struct{}{}
	}
}

func addKnownSyncMessages(nodeID string, newMessages []int) {
	if len(newMessages) == 0 {
		return
	}

	knownSyncMessagesMutex.Lock()
	defer knownSyncMessagesMutex.Unlock()

	for _, message := range newMessages {
		if _, ok := knownSyncMessages[nodeID]; !ok {
			knownSyncMessages[nodeID] = map[int]struct{}{}
		}
		knownSyncMessages[nodeID][message] = struct{}{}
	}
}

func syncScheduler(duration time.Duration) {
	for range time.Tick(duration) {
		go syncWithNeighbors()
	}
}

func syncWithNeighbors() {
	for _, neighborID := range topology[n.ID()] {
		go func(nodeID string) {
			unknownMessages := getUnknownMessages(nodeID)
			if len(unknownMessages) == 0 {
				return
			}

			body := SyncMessage{
				Type:     "sync",
				Messages: unknownMessages,
			}

			err := n.RPC(nodeID, body, func(msg maelstrom.Message) error {
				return syncResponseHandler(nodeID, unknownMessages, msg)
			})

			if err != nil {
				fmt.Fprintf(os.Stderr, "[syncWithNeighbors]: nodeID: %v - err: %v\n", nodeID, err)
			}
		}(neighborID)
	}
}

func syncResponseHandler(neighborID string, unknownMessages []int, msg maelstrom.Message) error {
	var body SyncMessage
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	addMessages(body.Messages)
	addKnownSyncMessages(neighborID, unknownMessages)

	return nil
}
