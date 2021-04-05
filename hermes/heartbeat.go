package hermes

import (
	"encoding/gob"
	go_hermes "go-hermes"
	"go-hermes/log"
	"sync"
	"time"
)

type HManEntry struct {
	timer          *time.Timer
	respondedNodes map[go_hermes.ID]bool
	timedOut       bool
	sentResponse   bool
}

type HMan struct {
	//go_hermes.Socket
	id go_hermes.ID
	go_hermes.Node
	config go_hermes.Config
	quorum *go_hermes.Quorum
	ticker *time.Ticker

	LiveNodeLock sync.RWMutex
	LiveNodes    []go_hermes.ID
	BallotNum    *ballotNum
	InProgress   bool
	EntryMap     map[int]*HManEntry
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
func NewHMan(n go_hermes.Node, options ...func(*Hermes)) *HMan {
	Hman = &HMan{
		//Socket:    go_hermes.NewSocket(id, addrs),
		Node:      n,
		id:        n.ID(),
		quorum:    go_hermes.NewQuorum(),
		LiveNodes: make([]go_hermes.ID, 0),
		BallotNum: &ballotNum{
			EpochNum: 0,
			NodeId:   n.ID(),
		},
		InProgress: false,
		EntryMap:   make(map[int]*HManEntry),
	}
	for node_addr := range go_hermes.GetConfig().Addrs {
		if node_addr == n.ID() {
			continue
		}
		Hman.LiveNodes = append(Hman.LiveNodes, node_addr)
	}
	return Hman
}

func (hman *HMan) processFailureTimeout(epochNum int) {
	hman.LiveNodeLock.Lock()
	defer hman.LiveNodeLock.Unlock()

	if hman.EntryMap[epochNum].sentResponse {
		return
	}
	liveNodes := make([]go_hermes.ID, 0)
	for n := range hman.EntryMap[epochNum].respondedNodes {
		liveNodes = append(liveNodes, n)
	}
	for n := range hman.EntryMap[epochNum].respondedNodes {
		hman.Send(n, BeatDecide{
			FromNode: hman.id,
			ToNode:   n,
			NodeList: liveNodes,
			BallotNum: &ballotNum{
				EpochNum: epochNum,
				NodeId:   hman.id,
			},
		})
	}
	// reset the list of live nodes for hman
	hman.LiveNodes = liveNodes
	hman.EntryMap[epochNum].sentResponse = true
	// now its not in progress anymore
	hman.InProgress = false
}

func (hman *HMan) hmanMltHandler(epochNum int) {
	//log.Debugf("[HMAN]: starting hman mlt handler for epoch: %v", epochNum)
	for {
		select {
		case <-hman.EntryMap[epochNum].timer.C:
			//log.Infof("[HMAN]: failure time out for epoch: %d", epochNum)
			hman.EntryMap[epochNum].timedOut = true
			hman.processFailureTimeout(epochNum)
			break
		}
	}
}

func (hman *HMan) sendbeats() {
	hman.InProgress = true
	hman.BallotNum.EpochNum += 1

	hman.LiveNodeLock.Lock()
	hman.EntryMap[hman.BallotNum.EpochNum] = &HManEntry{
		respondedNodes: make(map[go_hermes.ID]bool),
		timer:          time.NewTimer(time.Duration(go_hermes.GetConfig().FailureMLT) * time.Second),
		timedOut:       false,
	}
	hman.LiveNodeLock.Unlock()

	beat := Beat{
		FromNode:  hman.id,
		BallotNum: hman.BallotNum,
	}
	go hman.hmanMltHandler(hman.BallotNum.EpochNum)
	log.Debugf("[HMAN]: Broadcasting heartbeats")
	hman.Broadcast(beat)
}

func (hman *HMan) HManFly() {
	if hman.id.Node() == 1 && hman.id.Zone() == 1 {
		hman.ticker = time.NewTicker(time.Duration(go_hermes.GetConfig().HeartbeatInterval) * time.Second)
		for {
			select {
			case <-hman.ticker.C:
				go hman.sendbeats()
			}
		}
	} else {
		log.Infof("node %v not the HA node, not starting the heartbeat routine", hman.id)
		return
	}
}

func (hman *HMan) HandleBeat(m Beat) {
	log.Debugf("[HMAN] Node %v received beat", hman.id)
	hman.Send(m.FromNode, BeatACK{
		FromNode:  hman.id,
		ToNode:    m.FromNode,
		BallotNum: m.BallotNum,
	})
}

func (hman *HMan) HandleBeatACK(m BeatACK) {
	log.Debugf("[HandleBeatACK]: trying to take a lock")
	hman.LiveNodeLock.Lock()
	defer hman.LiveNodeLock.Unlock()
	log.Debugf("[HMAN] received BeatACK from node %v, epoch: %v", m.FromNode, m.BallotNum.EpochNum)
	// check if the ACK received has a timestamp lower than the current timestamp, if so,
	// don't accept this ACK, its probably from an old one.
	if hman.isReceivedBallotLesser(m) {
		log.Infof("received BeatACK from a smaller ballot, ignoring it")
		return
	}

	if hman.InProgress {
		liveNodes := make([]go_hermes.ID, 0)

		// if the current epoch number already exists, and heartbeat is ongoing for it,
		// add the newly received BeatACK to the same map
		hman.EntryMap[m.BallotNum.EpochNum].respondedNodes[m.FromNode] = true

		// check if the time out has occurred already for this
		if hman.EntryMap[m.BallotNum.EpochNum].timedOut {
			//log.Debugf("[HMAN]: Looks like epoch %v is timeout", m.BallotNum.EpochNum)
			//log.Debugf("[HMAN]: Responded nodes: %v", hman.EntryMap[hman.BallotNum.EpochNum].respondedNodes)
			// add all the nodes which responded with an ACK to the list of live nodes
			for n := range hman.EntryMap[m.BallotNum.EpochNum].respondedNodes {
				liveNodes = append(liveNodes, n)
			}
			// send this newly created list of live nodes in a BeatDecide message
			// only to those nodes who responded with a BeatACK
			for n := range hman.EntryMap[m.BallotNum.EpochNum].respondedNodes {
				hman.Send(n, BeatDecide{
					FromNode:  hman.id,
					ToNode:    m.FromNode,
					NodeList:  liveNodes,
					BallotNum: m.BallotNum,
				})
			}
			// reset the list of live nodes for hman
			hman.LiveNodes = liveNodes
			hman.EntryMap[m.BallotNum.EpochNum].sentResponse = true
			// now its not in progress anymore
			hman.InProgress = false
		} else {
			log.Debugf("[HMAN]: Epoch %v is not timed out yet, current responded nodes: %v",
				m.BallotNum.EpochNum, hman.EntryMap[m.BallotNum.EpochNum].respondedNodes)
		}
	} else {
		log.Debugf("[HMAN]: not in progress")
	}
}

func (hman *HMan) HandleBeatDecide(m BeatDecide) {
	hman.LiveNodeLock.Lock()
	defer hman.LiveNodeLock.Unlock()

	log.Infof("received BeatDecide message: %v", m.NodeList)
	hman.LiveNodes = m.NodeList
}

func (hman *HMan) isReceivedBallotLesser(m BeatACK) bool {
	return m.BallotNum.EpochNum < hman.BallotNum.EpochNum
}
