package hermes

import (
	go_hermes "go-hermes"
	"go-hermes/log"
	"time"
)

type Replica struct {
	go_hermes.Node
	*Hermes
}

var replica Replica

func NewReplica(id go_hermes.ID) *Replica {
	r := new(Replica)
	r.Node = go_hermes.NewNode(id)
	r.Hermes = NewHermes(r)
	r.Register(go_hermes.Request{}, r.handleRequest)
	r.Register(ACK{}, r.HandleACK)
	r.Register(INV{}, r.HandleINV)
	r.Register(VAL{}, r.HandleVAL)
	return r
}

func (r *Replica) handleRequest(m go_hermes.Request) {
	log.Debugf("Replica %s received %v\n", r.ID(), m)

	if m.Command.IsRead() {
		v, inProgress := r.readInProgress(m)
		reply := go_hermes.Reply{
			Command:    m.Command,
			Value:      v,
			Properties: make(map[string]string),
			Timestamp:  time.Now().Unix(),
		}
		m.Reply(reply)
	}
	go r.Hermes.HandleRequest(m)
}

func (r *Replica) readInProgress(m go_hermes.Request) (go_hermes.Value, bool) {
	return r.Node.Execute(m.Command), false
}
