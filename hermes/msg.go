package hermes

import (
	"encoding/gob"
	go_hermes "go-hermes"
)

func init() {
	gob.Register(INV{})
	gob.Register(ACK{})
	gob.Register(VAL{})
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
	Key      go_hermes.KeyStruct `json:"key"`
	Epoch_id int                 `json:"epoch_id"`
	Value    string              `json:"value"`
}

type ACK struct {
	Key      go_hermes.KeyStruct `json:"key"`
	Epoch_id int                 `json:"epoch_id"`
}

type VAL struct {
	Key      go_hermes.KeyStruct `json:"key"`
	Epoch_id int                 `json:"epoch_id"`
}
