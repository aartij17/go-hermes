package hermes

import (
	go_hermes "go-hermes"
	"go-hermes/log"
)

type Hermes struct {
	go_hermes.Node
	config []go_hermes.ID

	quorum *go_hermes.Quorum

	Q               func(*go_hermes.Quorum) bool
	HermesKeys      map[int]go_hermes.KeyStruct
	ReplyWhenCommit bool // use for optimisation
}

func NewHermes(n go_hermes.Node, options ...func(*Hermes)) *Hermes {
	h := &Hermes{
		Node:            n,
		quorum:          go_hermes.NewQuorum(),
		Q:               func(q *go_hermes.Quorum) bool { return q.All() },
		ReplyWhenCommit: false,
	}
	for _, opt := range options {
		opt(h)
	}
	return h
}

func (h *Hermes) HandleRequest(r go_hermes.Request) {
	log.Debug("Replica %s received %v\n", h.ID(), r)
	// 1. check if the key is in a VALID state
	state, exists := h.checkKeyState(r)
	if exists {
		switch state {
		case go_hermes.VALID_STATE:
			return
		case go_hermes.INVALID_STATE:

		}
	}
	h.broadcastRequest(r)
}

func (h *Hermes) checkKeyState(r go_hermes.Request) (string, bool) {
	key, exists := h.HermesKeys[int(r.Command.Key)]
	if exists {
		return key.State, true
	}
	return "", false
}

func (h *Hermes) broadcastRequest(r go_hermes.Request) {
	h.quorum.Reset()
	h.quorum.ACK(h.ID())
	h.Broadcast(ACK{
		Key:      go_hermes.KeyStruct{},
		Epoch_id: 0,
	})
}

func (h *Hermes) HandleACK(m ACK) {

}

func (h *Hermes) HandleINV(m INV) {

}

func (h *Hermes) HandleVAL(m VAL) {

}
