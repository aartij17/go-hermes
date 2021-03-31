package go_hermes

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
}
