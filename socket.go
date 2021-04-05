package go_hermes

import (
	"go-hermes/log"
	"sync"
	"time"
)

type Socket interface {
	Send(to ID, m interface{})
	Broadcast(m interface{})
	Recv() interface{}
	Close()
}

type socket struct {
	id        ID
	addresses map[ID]string
	nodes     map[ID]Transport

	lock sync.RWMutex
}

func NewSocket(id ID, addrs map[ID]string) Socket {
	socket := &socket{
		id:        id,
		addresses: addrs,
		nodes:     make(map[ID]Transport),
	}
	socket.nodes[id] = NewTransport(addrs[id])
	socket.nodes[id].Listen()

	return socket
}

func (s *socket) Send(to ID, m interface{}) {
	log.Debugf("node %s send message to %+v to %+v", s.id, m, to)
	s.lock.RLock()
	t, exists := s.nodes[to]
	s.lock.RUnlock()
	if !exists {
		s.lock.RLock()
		address, OK := s.addresses[to]
		s.lock.RUnlock()
		if !OK {
			log.Errorf("socket does not have address of node %s", to)
			//return errors.New("socket does not have address of node")
		}
		t = NewTransport(address)
		err := Retry(t.Dial, 100, time.Duration(50)*time.Millisecond)
		if err != nil {
			return
		}
		s.lock.Lock()
		s.nodes[to] = t
		s.lock.Unlock()
	}
	t.Send(m)
}

func (s *socket) Recv() interface{} {
	s.lock.RLock()
	t := s.nodes[s.id]
	s.lock.RUnlock()
	for {
		m := t.Recv()
		return m
	}
}

func (s *socket) Broadcast(m interface{}) {
	for id := range s.addresses {
		if id == s.id {
			continue
		}
		s.Send(id, m)
	}
}

func (s *socket) Close() {
	for _, t := range s.nodes {
		t.Close()
	}
}
