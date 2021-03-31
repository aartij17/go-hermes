package go_hermes

type LiveNodes struct {
	Nodes []string `json:"nodes"`
}

type Metadata struct {
	Lease     bool         `json:"lease"`
	Epoch_id  int          `json:"node_epoch_id"`
	LiveNodes []*LiveNodes `json:"live_nodes"`
}

type Node struct {

	Metadata Metadata `json:"metadata"`
}


