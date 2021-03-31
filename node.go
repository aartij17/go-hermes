package go_hermes

import (
	"net/http"
	"paxi"
)

type LiveNodes struct {
	Nodes []string `json:"nodes"`
}

type Metadata struct {
	Lease     bool         `json:"lease"`
	Epoch_id  int          `json:"node_epoch_id"`
	LiveNodes []*LiveNodes `json:"live_nodes"`
}

type Node interface {
	Socket
	Database
	ID() ID
	Run()
}

type node struct {
	id ID
	Socket
	Database
	MessageChan chan interface{}
	server      *http.Server
	Metadata    Metadata
}

func NewNode(id ID) Node {
	return &node{
		id:          id,
		Socket:      NewSocket(id, config.Addrs),
		Database:    NewDatabase(),
		MessageChan: make(chan interface{}),
	}
}

func (n *node) ID() ID {
	return n.id
}

func (n *node) Run() {
	log.Infof("node %v start running", n.id)
	go n.handle()
	go n.recv()
	n.http()
}
