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
	Timestamp go_hermes.Timestamp
}

type Hermes struct {
	go_hermes.Node
	config go_hermes.Config

	quorum *go_hermes.Quorum

	Q func(*go_hermes.Quorum) bool

	entryLock sync.RWMutex
	keyLock   sync.RWMutex
	GenLock sync.RWMutex
	entryLog  map[int]*Entry

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
				//time.Sleep(5 * time.Second)
				return
				//continue
			}
		} else {
			log.Debugf("key %v doesn't exist yet", r.Command.Key)
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
	log.Debugf("trying to take lock")
	h.keyLock.RLock()
	log.Debugf("lock taken")
	defer h.keyLock.RUnlock()
	key, exists := h.HermesKeys[int(r.Command.Key)]
	if exists {
		return key, true
	}
	return nil, false
}

func (h *Hermes) broadcastRequest(r go_hermes.Request) {
	log.Debugf("waiting for lock")


	//------------------------
	// TODO: Aarti: huge change
	//h.GenLock.Lock()
	//h.quorum.ACK(h.ID())
	//// h.quorum.Reset()
	//h.GenLock.Unlock()
	// -----------------------

	h.entryLock.Lock()
	log.Debugf("lock taken")
	h.entryLog[r.Epoch_ID] = &Entry{
		Quorum:    go_hermes.NewQuorum(),
		Request:   r,
		MltTicker: time.NewTicker(time.Duration(h.config.MLT) * time.Second),
	}
	h.entryLog[r.Epoch_ID].Quorum.ACK(h.ID())

	INV_message := INV{
		Key:        r.KeyStruct,
		Epoch_id:   r.Epoch_ID,
		Value:      r.Value,
		FromNodeID: h.Node.ID(),
	}
	//go h.coordinatorTicker(INV_message, r.Epoch_ID, r)
	//h.entryLock.Lock()
	//h.entryLog[r.Epoch_ID].Quorum.ACK(h.ID())
	h.entryLock.Unlock()
	log.Debugf("lock released")
	//h.entryLock.Unlock()
	log.Debugf("Key %v added to dictionary", r.KeyStruct.Key)
	h.HermesKeys[int(r.Command.Key)] = r.KeyStruct
	log.Debugf("Broadcasting INV")
	h.Broadcast(h.Node.ID(), INV_message) //, Hman.LiveNodes)
}

func (h *Hermes) HandleACK(m ACK) {
	// TODO: Contact membership service for all live replicas
	log.Debugf("trying to take lock")
	h.entryLock.RLock()
	log.Debugf("got lock")
	_, exists := h.entryLog[m.Epoch_id]
	h.entryLock.RUnlock()

	if exists {
		log.Debugf("trying to take lock")
		h.entryLock.Lock()
		log.Debugf("got lock")
		h.entryLog[m.Epoch_id].Quorum.ADD()
		h.entryLock.Unlock()
		if h.entryLog[m.Epoch_id].Quorum.AllFromViewManagement(Hman.LiveNodes) {
			log.Info("Quorum found, Broadcasting VAL")
			h.BroadcastToLiveNodes(h.Node.ID(), VAL{
				Key:        m.Key,
				Epoch_id:   m.Epoch_id,
				FromNodeID: h.Node.ID(),
			}, Hman.LiveNodes)
			log.Debugf("trying to take lock")
			h.keyLock.Lock()
			log.Debugf("trying to take lock")
			h.HermesKeys[int(m.Key.Key)].State = go_hermes.VALID_STATE
			h.keyLock.Unlock()
			//h.changeKeyState(int(m.Key.Key), go_hermes.VALID_STATE)
			log.Debugf("trying to take LOCK")
			h.entryLock.Lock()
			log.Debugf("got LOCK")
			h.entryLog[m.Epoch_id].Quorum.Reset()
			// stop the timer for this write
			h.entryLog[m.Epoch_id].MltTicker.Stop()
			h.entryLock.Unlock()
			h.entryLog[m.Epoch_id].Request.Reply(go_hermes.Reply{
				Command:   h.entryLog[m.Epoch_id].Request.Command,
				Value:     h.entryLog[m.Epoch_id].Request.Value,
				Timestamp: h.entryLog[m.Epoch_id].Request.Timestamp,
				Err:       nil,
			})
			h.entryLog[m.Epoch_id].Request = go_hermes.Request{}
		} else {
			log.Debugf("Haven't received all ACKs back")
		}
	} else {
		log.Errorf("entry not found in log for epochID: %d", m.Epoch_id)
	}
}

//// follower handlers
func (h *Hermes) HandleINV(m INV) {
	log.Debugf("trying to take lock")
	h.keyLock.Lock()
	log.Debugf("got lock")
	_, exists := h.HermesKeys[int(m.Key.Key)]
	if exists {
		if h.isRecvdTimestampGreater(m.Key) {

			h.HermesKeys[int(m.Key.Key)].State = go_hermes.INVALID_STATE
			//h.changeKeyState(int(m.Key.Key), go_hermes.INVALID_STATE)
			// update key's timestamp and value
			h.HermesKeys[int(m.Key.Key)].Ts = m.Key.Ts
			h.HermesKeys[int(m.Key.Key)].Value = m.Value
		}
	} else {
		log.Debugf("Key %v added to dictionary", m.Key)
		h.HermesKeys[int(m.Key.Key)] = m.Key
	}
	h.keyLock.Unlock()
	log.Debugf("trying to take lock")
	h.entryLock.Lock()
	log.Debugf("got lock")
	h.entryLog[m.Epoch_id] = &Entry{
		Quorum:    go_hermes.NewQuorum(),
		Request:   go_hermes.Request{},
		Timestamp: m.Key.Ts,
		MltTicker: time.NewTicker(time.Duration(h.config.MLT) * time.Second),
	}
	h.entryLock.Unlock()
	// start the timer for this epoch
	//go h.followerTicker(m)

	log.Debugf("Sending ACK")
	h.Send(h.Node.ID(), m.FromNodeID, ACK{
		Key:        m.Key,
		Epoch_id:   m.Epoch_id,
		FromNodeID: h.Node.ID(),
	})
}

func (h *Hermes) HandleVAL(m VAL) {
	log.Debugf("checking equal timestamps for Epoch ID: %v, Key: %v", m.Epoch_id, m.Key.Key)
	if h.checkEqualTimestamps(m) {
		log.Debugf("received val, transitioning the key back to valid state")
		log.Debugf("trying to take lock")
		h.keyLock.Lock()
		log.Debugf("got lock")
		h.HermesKeys[int(m.Key.Key)].State = go_hermes.VALID_STATE
		//h.changeKeyState(int(m.Key.Key), go_hermes.VALID_STATE)
		h.keyLock.Unlock()
		log.Debugf("trying to take lock")
		h.entryLock.Lock()
		log.Debugf("got lock")
		h.entryLog[m.Epoch_id].MltTicker.Stop()
		h.entryLock.Unlock()
	}
	// follower received the VAL, stop the timer
	log.Debugf("something with ticker:: %v, entryLog:: %v", m, h.entryLog[m.Epoch_id])
}

/////////// Time stamp checks
func (h *Hermes) checkEqualTimestamps(m VAL) bool {
	log.Debugf("trying to take lock")
	h.entryLock.RLock()
	log.Debugf("got the lock")
	defer h.entryLock.RUnlock()
	if _, OK := h.entryLog[m.Epoch_id]; !OK {
		log.Info("can't check equal timestamps, nothing to do")
		return false
	}
	log.Debug(h.entryLog[m.Epoch_id])
	log.Debug(h.entryLog[m.Epoch_id].Timestamp.Version)
	log.Debug(m.Key.Ts.C_id)
	log.Debug(m.Key.Ts.Version)
	return h.entryLog[m.Epoch_id].Timestamp.Version == m.Key.Ts.Version &&
		h.entryLog[m.Epoch_id].Timestamp.C_id == m.Key.Ts.C_id
	//return (h.HermesKeys[int(m.Key)].Ts.C_id == m.Ts.C_id) &&
	//	(h.HermesKeys[int(m.Key)].Ts.Version == m.Ts.Version)
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
	h.entryLock.RLock()
	tick := h.entryLog[epochId].MltTicker.C
	h.entryLock.RUnlock()
	for {
		select {
		case <-tick:
			// ticker went off, which means there is a loss of message somewhere. Retrigger
			// the write once again
			h.entryLock.Lock()
			h.entryLog[r.Epoch_ID] = &Entry{
				Quorum:    go_hermes.NewQuorum(),
				Request:   r,
				MltTicker: time.NewTicker(time.Duration(h.config.MLT) * time.Second),
			}
			h.entryLog[r.Epoch_ID].Quorum.ACK(h.ID())
			h.entryLock.Unlock()
			h.keyLock.Lock()
			h.HermesKeys[int(r.Command.Key)] = r.KeyStruct
			h.keyLock.Unlock()
			h.Broadcast(h.Node.ID(), msg)
		default:
			continue
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
				h.entryLock.Lock()
				delete(h.entryLog, m.Epoch_id)
				h.entryLock.Unlock()
				return
			}
			log.Infof("Follower ticker for node %v timed out, changing to replay state: %v",
				h.Node.ID(), m)
			log.Debugf("trying to take lock")
			h.keyLock.Lock()
			log.Debugf("got lock")
			h.HermesKeys[int(m.Key.Key)].State = go_hermes.REPLAY_STATE
			//h.changeKeyState(int(m.Key.Key), go_hermes.REPLAY_STATE)
			h.keyLock.Unlock()
			h.Broadcast(h.Node.ID(), INV{
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
