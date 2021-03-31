package go_hermes

import (
	"fmt"
	"go-hermes/log"
)

type Timestamp struct {
	Version int    `json:"version"`
	C_id    string `json:"coordinator_id"`
}
type KeyStruct struct {
	Key   string    `json:"key"`
	Ts    Timestamp `json:"timestamp"`
	State string    `json:"state"`
}

//--------------------------
// Client replica messages
type Request struct {
	Command    Command
	Properties map[string]string
	Timestamp  int64
	NodeID     ID
	c          chan Reply
}

func (r *Request) Reply(reply Reply) {
	log.Info("sending reply back to the http client")
	r.c <- reply
}

func (r Request) String() string {
	return fmt.Sprintf("request {cmd=%v nid=%v}", r.Command, r.NodeID)
}

type Reply struct {
	Command    Command
	Value      Value
	Properties map[string]string
	Timestamp  int64
	Err        error
}

func (r Reply) String() string {
	return fmt.Sprintf("Reply {cmd=%v value=%x prop=%v}", r.Command, r.Value, r.Properties)
}

// -------------------------
// Message types in Hermes protocol
type Message struct {
	Type string `json:"message_type"`
	INV  *INV   `json:"inv_message,optional"`
	ACK  *ACK   `json:"ack_message,optional"`
	VAL  *VAL   `json:"val_message,optional"`
}

type INV struct {
	Key      KeyStruct `json:"key"`
	Epoch_id int       `json:"epoch_id"`
	Value    string    `json:"value"`
}

type ACK struct {
	Key      KeyStruct `json:"key"`
	Epoch_id int       `json:"epoch_id"`
}

type VAL struct {
	Key      KeyStruct `json:"key"`
	Epoch_id int       `json:"epoch_id"`
}
