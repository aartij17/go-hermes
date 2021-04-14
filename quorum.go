package go_hermes

import (
	"go-hermes/log"
)
type Quorum struct {
	size  int
	acks  map[ID]bool
	zones map[int]int
}

func NewQuorum() *Quorum {
	q := &Quorum{
		size:  0,
		acks:  make(map[ID]bool),
		zones: make(map[int]int),
	}
	return q
}

func (q *Quorum) ACK(id ID) {
	if !q.acks[id] {
		q.acks[id] = true
		q.size++
		q.zones[id.Zone()]++
	}
}

func (q *Quorum) ADD() {
	q.size++
}

func (q *Quorum) Size() int {
	return q.size
}

func (q *Quorum) Reset() {
	q.size = 0
	q.acks = make(map[ID]bool)
	q.zones = make(map[int]int)
}

func (q *Quorum) All() bool {
	return q.size == config.n
}

func (q *Quorum) AllFromViewManagement(livenodes []ID) bool {
	log.Debugf("size of quorum:: %v, size of livenodes: %v", q.size, len(livenodes))
	return q.size >= len(livenodes)
}
