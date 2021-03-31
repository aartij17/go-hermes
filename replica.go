package go_hermes

type Replica struct {
	Node
}

var replica Replica

func NewReplica(id ID) *Replica {
	r := new(Replica)
	r.Node = NewNode(id)
	return r
}
