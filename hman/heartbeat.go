package hman

import (
	"encoding/gob"
	go_hermes "go-hermes"
	"sync"
	"time"
)

type HMan struct {
	go_hermes.Socket
	go_hermes.Node
	config go_hermes.Config
	quorum *go_hermes.Quorum

	sync.RWMutex
	LiveNodes []go_hermes.ID
	ticker    *time.Ticker
	BallotNum *ballotNum
}

var Hman *HMan

func init() {
	gob.Register(Beat{})
	gob.Register(BeatACK{})
	gob.Register(BeatDecide{})
}

/**
Every node will have another process running calling the HMan.
*/
func NewHMan(id go_hermes.ID, addrs map[go_hermes.ID]string) *HMan {
	Hman = &HMan{
		Socket:    go_hermes.NewSocket(id, addrs),
		quorum:    nil,
		LiveNodes: make([]go_hermes.ID, len(addrs)),
		BallotNum: &ballotNum{
			epochNum: 0,
			nodeId:   id,
		},
	}
	return Hman
}

func (hman *HMan) sendbeats() {
	beat := Beat{
		fromNode: hman.Node.ID(),
	}
	hman.Broadcast(beat)
}

func (hman *HMan) HManFly() {
	hman.ticker = time.NewTicker(time.Duration(go_hermes.GetConfig().MLT) * time.Second)
	for {
		select {
		case <-hman.ticker.C:
			go hman.sendbeats()
		}
	}
}

func (hman *HMan) HandleBeat(m Beat) {
	newBallot := &ballotNum{
		epochNum: m.ballotNum.epochNum + 1,
		nodeId:   m.ballotNum.nodeId,
	}
	hman.Send(m.fromNode, BeatACK{
		fromNode:  hman.Node.ID(),
		toNode:    m.fromNode,
		ballotNum: newBallot,
	})
}

func (hman *HMan) HandleBeatACK(m BeatACK) {

}

func (hman *HMan) HandleBeatDecide(m BeatDecide) {

}
