package go_hermes

type Timestamp struct {
	Version int    `json:"version"`
	C_id    string `json:"coordinator_id"`
}
type Key struct {
	Key   string    `json:"key"`
	Ts    Timestamp `json:"timestamp"`
	State string    `json:"state"`
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
	Key      Key    `json:"key"`
	Epoch_id int    `json:"epoch_id"`
	Value    string `json:"value"`
}

type ACK struct {
	Key      Key `json:"key"`
	Epoch_id int `json:"epoch_id"`
}

type VAL struct {
	Key      Key `json:"key"`
	Epoch_id int `json:"epoch_id"`
}
