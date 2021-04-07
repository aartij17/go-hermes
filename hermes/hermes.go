package hermes

import (
	go_hermes "go-hermes"
	"go-hermes/log"
	"sync"
	"time"
)

type Entry struct {
	Quorum    *go_hermes.Quorum
	Request   go_hermes.Request
	MltTicker *time.Ticker
}

type Hermes struct {
	go_hermes.Node
	config go_hermes.Config

	quorum *go_hermes.Quorum

	Q func(*go_hermes.Quorum) bool

	entryLock       sync.RWMutex
	entryLog        map[int]*Entry
	HermesKeys      map[int]*go_hermes.KeyStruct
	ReplyWhenCommit bool // use for optimisation
	EpochId         int
	timestamp       go_hermes.Timestamp
}

func NewHermes(n go_hermes.Node, options ...func(*Hermes)) *Hermes {
	h := &Hermes{
		Node:            n,
		quorum:          go_hermes.NewQuorum(),
		Q:               func(q *go_hermes.Quorum) bool { return q.All() },
		ReplyWhenCommit: false,
		EpochId:         0,
		entryLog:        make(map[int]*Entry),
		HermesKeys:      make(map[int]*go_hermes.KeyStruct),
		config:          go_hermes.GetConfig(),
		timestamp:       go_hermes.Timestamp{},
	}
	for _, opt := range options {
		opt(h)
	}
	return h
}

func (h *Hermes) HandleRequest(r go_hermes.Request) {
	// 1. check if the key is in a VALID state, if not,
	// stall the request and check again after a while
	for {
		key, exists := h.CheckKeyState(r)
		//log.Infof("Key state: %v", key.State)
		if exists {
			switch key.State {
			case go_hermes.VALID_STATE:
				newVersion := h.HermesKeys[int(r.Command.Key)].Ts.Version + 1
				r.KeyStruct = &go_hermes.KeyStruct{
					Key: r.Command.Key,
					Ts: go_hermes.Timestamp{
						Version: newVersion,
						C_id:    h.Node.ID(),
					},
					State: go_hermes.INVALID_STATE,
					Value: string(r.Command.Value),
				}
				h.broadcastRequest(r)
				return
			default:
				log.Info("sleeping, waiting for the key to become valid!")
				time.Sleep(5 * time.Second)
				continue
			}
		} else {
			log.Infof("key %v doesn't exist yet", r.Command.Key)
			r.KeyStruct = &go_hermes.KeyStruct{
				Key: r.Command.Key,
				Ts: go_hermes.Timestamp{
					Version: 1,
					C_id:    h.Node.ID(),
				},
				Value: string(r.Command.Value),
				State: go_hermes.INVALID_STATE,
			}
			break
		}
	}
	h.broadcastRequest(r)
}

func (h *Hermes) CheckKeyState(r go_hermes.Request) (*go_hermes.KeyStruct, bool) {
	//h.entryLock.RLock()
	//defer h.entryLock.RUnlock()

	key, exists := h.HermesKeys[int(r.Command.Key)]
	if exists {
		return key, true
	}
	return nil, false
}

func (h *Hermes) broadcastRequest(r go_hermes.Request) {
	log.Debugf("waiting for lock")
	h.entryLock.Lock()
	log.Debugf("lock taken")
	h.quorum.Reset()
	h.quorum.ACK(h.ID())

	h.entryLog[r.Epoch_ID] = &Entry{
		Quorum:    go_hermes.NewQuorum(),
		Request:   r,
		MltTicker: time.NewTicker(time.Duration(h.config.MLT) * time.Second),
	}
	h.entryLock.Unlock()
	log.Debugf("lock released")

	INV_message := INV{
		Key:        r.KeyStruct,
		Epoch_id:   r.Epoch_ID,
		Value:      r.Value,
		FromNodeID: h.Node.ID(),
	}
	go h.coordinatorTicker(INV_message, r.Epoch_ID, r)
	h.entryLog[r.Epoch_ID].Quorum.ACK(h.ID())

	h.HermesKeys[int(r.Command.Key)] = r.KeyStruct
	//log.Info(h.HermesKeys)
	log.Info("Broadcasting INV")
	h.Broadcast(INV{
		Key:        r.KeyStruct,
		Epoch_id:   r.Epoch_ID,
		Value:      r.Value,
		FromNodeID: h.Node.ID(),
	}) //, Hman.LiveNodes)
}

func (h *Hermes) HandleACK(m ACK) {
	// TODO: Contact membership service for all live replicas
	h.entryLock.Lock()
	_, exists := h.entryLog[m.Epoch_id]
	h.entryLock.Unlock()

	if exists {
		h.entryLog[m.Epoch_id].Quorum.ADD()
		if h.entryLog[m.Epoch_id].Quorum.AllFromViewManagement(Hman.LiveNodes) {
			log.Info("Quorum found, Broadcasting VAL")
			h.BroadcastToLiveNodes(VAL{
				Key:        m.Key,
				Epoch_id:   m.Epoch_id,
				FromNodeID: h.Node.ID(),
			}, Hman.LiveNodes)
			h.entryLock.Lock()
			h.HermesKeys[int(m.Key.Key)].State = go_hermes.VALID_STATE
			h.entryLock.Unlock()
			//h.changeKeyState(int(m.Key.Key), go_hermes.VALID_STATE)
			h.entryLog[m.Epoch_id].Quorum.Reset()
			// stop the timer for this write
			h.entryLog[m.Epoch_id].MltTicker.Stop()
			log.Info(h.entryLog[m.Epoch_id].Request.Command)
			h.entryLog[m.Epoch_id].Request.Reply(go_hermes.Reply{
				Command:   h.entryLog[m.Epoch_id].Request.Command,
				Value:     h.entryLog[m.Epoch_id].Request.Value,
				Timestamp: h.entryLog[m.Epoch_id].Request.Timestamp,
				Err:       nil,
			})
			h.entryLog[m.Epoch_id].Request = go_hermes.Request{}
		} else {
			log.Infof("Haven't received all ACKs back")
		}
	} else {
		log.Errorf("entry not found in log for epochID: %d", m.Epoch_id)
	}
}

//// follower handlers
func (h *Hermes) HandleINV(m INV) {
	_, exists := h.HermesKeys[int(m.Key.Key)]
	if exists {
		if h.isRecvdTimestampGreater(m.Key) {
			h.entryLock.Lock()
			h.HermesKeys[int(m.Key.Key)].State = go_hermes.INVALID_STATE
			//h.changeKeyState(int(m.Key.Key), go_hermes.INVALID_STATE)
			h.entryLock.Unlock()
			// update key's timestamp and value
			h.HermesKeys[int(m.Key.Key)].Ts = m.Key.Ts
			h.HermesKeys[int(m.Key.Key)].Value = m.Value
		}
	} else {
		h.HermesKeys[int(m.Key.Key)] = m.Key
	}
	h.entryLog[m.Epoch_id] = &Entry{
		Quorum:    go_hermes.NewQuorum(),
		Request:   go_hermes.Request{},
		MltTicker: time.NewTicker(time.Duration(h.config.MLT) * time.Second),
	}
	// start the timer for this epoch
	go h.followerTicker(m)

	log.Info("Sending ACK")
	h.Send(m.FromNodeID, ACK{
		Key:        m.Key,
		Epoch_id:   m.Epoch_id,
		FromNodeID: h.Node.ID(),
	})
}

func (h *Hermes) HandleVAL(m VAL) {
	if h.checkEqualTimestamps(m.Key) {
		log.Debugf("received val, transitioning the key back to valid state")
		h.entryLock.Lock()
		h.HermesKeys[int(m.Key.Key)].State = go_hermes.VALID_STATE
		//h.changeKeyState(int(m.Key.Key), go_hermes.VALID_STATE)
		h.entryLock.Unlock()
	}
	// follower received the VAL, stop the timer
	h.entryLog[m.Epoch_id].MltTicker.Stop()
}

/////////// Time stamp checks
func (h *Hermes) checkEqualTimestamps(m *go_hermes.KeyStruct) bool {
	return (h.HermesKeys[int(m.Key)].Ts.C_id == m.Ts.C_id) &&
		(h.HermesKeys[int(m.Key)].Ts.Version == m.Ts.Version)
}

func (h *Hermes) isRecvdTimestampGreater(m *go_hermes.KeyStruct) bool {
	currTs := h.HermesKeys[int(m.Key)].Ts
	recdTs := m.Ts

	if recdTs.C_id == currTs.C_id {
		if recdTs.Version > currTs.Version {
			return true
		}
		return false
	} else {
		return recdTs.C_id > currTs.C_id
	}
}

//// Ticker functions for message losses
func (h *Hermes) coordinatorTicker(msg INV, epochId int, r go_hermes.Request) {
	for {
		select {
		case <-h.entryLog[epochId].MltTicker.C:
			// ticker went off, which means there is a loss of message somewhere. Retrigger
			// the write once again
			h.entryLog[r.Epoch_ID] = &Entry{
				Quorum:    go_hermes.NewQuorum(),
				Request:   r,
				MltTicker: time.NewTicker(time.Duration(h.config.MLT) * time.Second),
			}
			h.entryLog[r.Epoch_ID].Quorum.ACK(h.ID())
			h.HermesKeys[int(r.Command.Key)] = r.KeyStruct
			h.Broadcast(msg)
		}
	}
}

func (h *Hermes) followerTicker(m INV) {
	for {
		select {
		case <-h.entryLog[m.Epoch_id].MltTicker.C:
			if h.Node.IsCrashed() {
				log.Debugf("its a crashed node, no need to replay")
				h.entryLog[m.Epoch_id].MltTicker.Stop()
				delete(h.entryLog, m.Epoch_id)
				return
			}
			log.Infof("Follower ticker for node %v timed out, changing to replay state",
				h.Node.ID())
			h.entryLock.Lock()
			h.HermesKeys[int(m.Key.Key)].State = go_hermes.REPLAY_STATE
			//h.changeKeyState(int(m.Key.Key), go_hermes.REPLAY_STATE)
			h.entryLock.Unlock()
			h.Broadcast(INV{
				Key:        m.Key,
				Epoch_id:   m.Epoch_id,
				Value:      m.Value,
				FromNodeID: h.Node.ID(),
			}) //, Hman.LiveNodes)
			// don't stop the ticker here, cause we need to keep checking if there are any failures
			// once again
		}
	}
}

//// other utils
//func (h *Hermes) changeKeyState(key int, state string) {
//	//h.entryLock.Lock()
//	//defer h.entryLock.Unlock()
//
//	h.HermesKeys[key].State = state
//}
