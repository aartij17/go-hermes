package go_hermes

import (
	"go-hermes/log"
	"sync"
	"time"
)

type Socket interface {
	Crash(t int)
	Send(to ID, m interface{})
	IsCrashed() bool
	Broadcast(m interface{})
	BroadcastToLiveNodes(m interface{}, ids []ID)
	Recv() interface{}
	Close()
}

type socket struct {
	id        ID
	addresses map[ID]string
	nodes     map[ID]Transport

	lock  sync.RWMutex
	crash bool
}

func NewSocket(id ID, addrs map[ID]string) Socket {
	socket := &socket{
		id:        id,
		addresses: addrs,
		nodes:     make(map[ID]Transport),
		crash:     false,
	}
	socket.nodes[id] = NewTransport(addrs[id])
	socket.nodes[id].Listen()

	return socket
}

func (s *socket) Send(to ID, m interface{}) {
	log.Debugf("node %s send message to %+v to %+v", s.id, m, to)
	if s.crash {
		log.Infof("node %s crashed, cannot send message %+v to %v", s.id, m, to)
		return
	}

	s.lock.RLock()
	log.Debugf("Took RLock")
	t, exists := s.nodes[to]
	s.lock.RUnlock()
	log.Debugf("Release RLock")
	if !exists {
		log.Debug("socket doesn't exist!")
		s.lock.RLock()
		address, OK := s.addresses[to]
		s.lock.RUnlock()
		if !OK {
			log.Errorf("socket does not have address of node %s", to)
			//return errors.New("socket does not have address of node")
		}
		t = NewTransport(address)
		//t.Dial()
		err := Retry(t.Dial, 5, time.Duration(50)*time.Millisecond)
		if err != nil {
			log.Error(err)
			return
		}
		s.lock.Lock()
		log.Debugf("Took SLock")
		s.nodes[to] = t
		s.lock.Unlock()
		log.Debugf("Released SLock")
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

func (s *socket) BroadcastToLiveNodes(m interface{}, ids []ID) {
	for _, id := range ids {
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

func (s *socket) IsCrashed() bool {
	return s.crash
}

func (s *socket) Crash(t int) {
	log.Infof("CRASHING")
	s.crash = true
	if t > 0 {
		timer := time.NewTimer(time.Duration(t) * time.Second)
		go func() {
			<-timer.C
			s.crash = false
			log.Infof("Back up and running!")
		}()
	}
}
