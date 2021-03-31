package go_hermes

type Quorum struct {
	size  int
	acks  map[ID]bool
	zones map[int]int
}

func NewQuorum() *Quorum {
	q := &Quorum{
		size: 0,
		acks: make(map[ID]bool),
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

func (q *Quorum) All() bool {
	return q.size == config.n
}
