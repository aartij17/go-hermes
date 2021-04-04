package hermes

import (
	go_hermes "go-hermes"
	"go-hermes/hman"
	"go-hermes/log"
	"time"
)

type Replica struct {
	go_hermes.Node
	*Hermes
	EpochId int

	hman.HMan
}

var replica Replica

func NewReplica(id go_hermes.ID) *Replica {
	r := new(Replica)
	r.EpochId = 0
	r.Node = go_hermes.NewNode(id)
	r.Hermes = NewHermes(r)

	r.Register(go_hermes.Request{}, r.handleRequest)
	r.Register(ACK{}, r.HandleACK)
	r.Register(INV{}, r.HandleINV)
	r.Register(VAL{}, r.HandleVAL)

	r.Register(hman.Beat{}, r.HandleBeat)
	r.Register(hman.BeatACK{}, r.HandleBeatACK)
	r.Register(hman.BeatDecide{}, r.HandleBeatDecide)

	return r
}

func (r *Replica) handleRequest(m go_hermes.Request) {
	log.Debugf("Replica %s received %v\n", r.ID(), m)
	r.EpochId += 1
	r.Hermes.EpochId = r.EpochId

	if m.Command.IsRead() {
		// since this is a local read, read from the node that the client sent a request to,
		// and return
		v, _ := r.readInProgress(m)
		reply := go_hermes.Reply{
			Command:    m.Command,
			Value:      string(v),
			Properties: make(map[string]string),
			Timestamp:  time.Now().Unix(),
		}
		m.Reply(reply)
		return
	}
	m.Epoch_ID = r.EpochId
	go r.Hermes.HandleRequest(m)
}

func (r *Replica) readInProgress(m go_hermes.Request) (go_hermes.Value, bool) {
	state, exists := r.Hermes.CheckKeyState(m)
	if exists && state == go_hermes.VALID_STATE {
		return []byte(r.HermesKeys[int(m.Command.Key)].Value), false
	} else if exists {
		log.Infof("key %v exists but not in VALID state", m.Command.Key)
		time.Sleep(5 * time.Second)
		r.handleRequest(m)
	}
	return go_hermes.Value{}, false
}
