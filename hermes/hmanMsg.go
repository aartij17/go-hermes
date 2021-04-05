package hermes

import go_hermes "go-hermes"

type ballotNum struct {
	EpochNum int
	NodeId   go_hermes.ID
}

type Beat struct {
	FromNode  go_hermes.ID
	ToNode    go_hermes.ID
	BallotNum *ballotNum
}

type BeatACK struct {
	FromNode  go_hermes.ID
	ToNode    go_hermes.ID
	BallotNum *ballotNum
}

type BeatDecide struct {
	FromNode  go_hermes.ID
	ToNode    go_hermes.ID
	NodeList  []go_hermes.ID
	BallotNum *ballotNum
}
