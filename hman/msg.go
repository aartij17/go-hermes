package hman

import go_hermes "go-hermes"

type ballotNum struct {
	epochNum int
	nodeId   go_hermes.ID
}

type Beat struct {
	fromNode  go_hermes.ID
	toNode    go_hermes.ID
	ballotNum *ballotNum
}

type BeatACK struct {
	fromNode  go_hermes.ID
	toNode    go_hermes.ID
	ballotNum *ballotNum
}

type BeatDecide struct {
	fromNode  go_hermes.ID
	toNode    go_hermes.ID
	NodeList  []go_hermes.ID
	ballotNum *ballotNum
}
