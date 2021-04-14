package go_hermes

import (
	"fmt"
	"go-hermes/log"
)

type Timestamp struct {
	Version int `json:"version"`
	C_id    ID  `json:"coordinator_id"`
}

type KeyStruct struct {
	Key   Key       `json:"key"`
	Ts    Timestamp `json:"timestamp"`
	State string    `json:"state"`
	Value string    `json:"value"`
}

func (m KeyStruct) String() string {
	return fmt.Sprintf("KeyStruct {Key=%v Ts=(%v) value=%v State=%v}", m.Key, m.Ts, m.Value, m.State)
}

//--------------------------
// Client replica messages
type Request struct {
	Command    Command
	Value      string
	Properties map[string]string
	Timestamp  int64
	NodeID     ID
	c          chan Reply
	KeyStruct  *KeyStruct
	Epoch_ID   int
}

func (r *Request) Reply(reply Reply) {
	log.Debugf("sending reply back to the http client")
	r.c <- reply
	log.Debugf("unlocked from the r.c reply channel passing")
}

func (r Request) String() string {
	return fmt.Sprintf("request {cmd=%v nid=%v}", r.Command, r.NodeID)
}

type Reply struct {
	Command    Command
	Value      string
	Properties map[string]string
	Timestamp  int64
	Err        error
}

func (r Reply) String() string {
	return fmt.Sprintf("Reply {cmd=%v value=%x prop=%v}", r.Command, r.Value, r.Properties)
}
