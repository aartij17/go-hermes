package go_hermes

import (
	//"go-hermes/hermes"
	"go-hermes/log"
	"net/http"
	"reflect"
	"sync"
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
	Register(m interface{}, f interface{})
}

type node struct {
	id ID
	Socket
	Database
	MessageChan chan interface{}
	server      *http.Server
	Metadata    Metadata

	sync.RWMutex
	handles map[string]reflect.Value
	//HMan    *hermes.HMan
}

func NewNode(id ID) Node {
	return &node{
		id:          id,
		Socket:      NewSocket(id, config.Addrs),
		Database:    NewDatabase(),
		MessageChan: make(chan interface{}, config.ChanBufferSize),
		Metadata: Metadata{
			Lease:     true,
			Epoch_id:  0,
			LiveNodes: nil,
		},
		handles: make(map[string]reflect.Value),
		//HMan:    hermes.NewHMan(id, config.Addrs),
	}
}

//func (n *node) startHMan() {
//	n.HMan.HManFly()
//}

func (n *node) ID() ID {
	return n.id
}

func (n *node) Run() {
	log.Infof("node %v start running", n.id)
	go n.handle()
	go n.recv()
	n.http()
}

func (n *node) Register(m interface{}, f interface{}) {
	t := reflect.TypeOf(m)
	fn := reflect.ValueOf(f)
	if fn.Kind() != reflect.Func || fn.Type().NumIn() != 1 || fn.Type().In(0) != t {
		panic("register handle function error")
	}
	n.handles[t.String()] = fn
}

func (n *node) recv() {
	for {
		m := n.Recv()
		switch m := m.(type) {
		case Request:
			m.c = make(chan Reply, 1)
			go func(r Request) {
				n.Send(n.id, r.NodeID, <-r.c)
			}(m)
			n.MessageChan <- m
			continue

		case Reply:
			n.RLock()
			//r := n.forwards[m.Command.String()]
			log.Debugf("node %v received reply %v", n.id, m)
			n.RUnlock()
			//r.Reply(m)
			continue
		}
		n.MessageChan <- m
	}
}

func (n *node) handle() {
	for {
		msg := <-n.MessageChan
		if msg == nil {
			log.Debugf("empty message received")
			continue
		}
		//log.Debug(msg)
		v := reflect.ValueOf(msg)
		//log.Debug(v)
		name := v.Type().String()
		f, exists := n.handles[name]
		if !exists {
			log.Fatalf("no registered handle function for message type %v", name)
		}
		// increment the epoch ID for each request received by the node.
		n.Metadata.Epoch_id += 1
		f.Call([]reflect.Value{v})
	}
}
