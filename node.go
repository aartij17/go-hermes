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
	paxi.Database
	MessageChan chan interface{}
	server      *http.Server
	Metadata    Metadata
}
