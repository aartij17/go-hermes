package hermes

import go_hermes "go-hermes"

type Hermes struct {
	go_hermes.Node
	config []go_hermes.ID

	quorum *go_hermes.Quorum

	Q               func(*go_hermes.Quorum) bool
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

}

func (h *Hermes) HandleACK(m ACK) {

}

func (h *Hermes) HandleINV(m INV) {

}

func (h *Hermes) HandleVAL(m VAL) {

}
