package hermes

import (
	go_hermes "go-hermes"
	"go-hermes/lib"
	"go-hermes/log"
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

	Q        func(*go_hermes.Quorum) bool
	entryLog *lib.CMap

	HermesKeys      *lib.CMap
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
		entryLog:        lib.NewCMap(), //make(map[int]*Entry),
		HermesKeys:      lib.NewCMap(), //make(map[int]*go_hermes.KeyStruct),
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
				newVersion := h.HermesKeys.Get(int(r.Command.Key)).(*go_hermes.KeyStruct).Ts.
					Version + 1
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
	log.Debugf("lock taken")
	exists := h.HermesKeys.Contains(int(r.Command.Key))
	if exists {
		return h.HermesKeys.Get(int(r.Command.Key)).(*go_hermes.KeyStruct), true
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

	h.entryLog.Put(r.Epoch_ID, &Entry{
		Quorum:    go_hermes.NewQuorum(),
		Request:   r,
		MltTicker: time.NewTicker(time.Duration(h.config.MLT) * time.Second),
	})
	h.entryLog.Get(r.Epoch_ID).(*Entry).Quorum.ACK(h.ID())

	INV_message := INV{
		Key:        r.KeyStruct,
		Epoch_id:   r.Epoch_ID,
		Value:      r.Value,
		FromNodeID: h.Node.ID(),
	}
	//go h.coordinatorTicker(INV_message, r.Epoch_ID, r)
	log.Debugf("Key %v added to dictionary", r.KeyStruct.Key)
	h.HermesKeys.Put(int(r.Command.Key), r.KeyStruct)
	log.Debugf("Broadcasting INV")
	h.Broadcast(h.Node.ID(), INV_message) //, Hman.LiveNodes)
}

func (h *Hermes) HandleACK(m ACK) {
	exists := h.entryLog.Contains(m.Epoch_id)

	if exists {
		h.entryLog.Get(m.Epoch_id).(*Entry).Quorum.ADD()
		if h.entryLog.Get(m.Epoch_id).(*Entry).Quorum.AllFromViewManagement(Hman.LiveNodes) {
			log.Info("Quorum found, Broadcasting VAL")
			h.BroadcastToLiveNodes(h.Node.ID(), VAL{
				Key:        m.Key,
				Epoch_id:   m.Epoch_id,
				FromNodeID: h.Node.ID(),
			}, Hman.LiveNodes)
			key := h.HermesKeys.Get(int(m.Key.Key)).(*go_hermes.KeyStruct)
			key.State = go_hermes.VALID_STATE
			h.HermesKeys.Put(int(m.Key.Key), key)
			h.entryLog.Get(m.Epoch_id).(*Entry).Quorum.Reset()
			// stop the timer for this write
			h.entryLog.Get(m.Epoch_id).(*Entry).MltTicker.Stop()
			eLog := h.entryLog.Get(m.Epoch_id).(*Entry)
			eLog.Request.Reply(go_hermes.Reply{
				Command:   eLog.Request.Command,
				Value:     eLog.Request.Value,
				Timestamp: eLog.Request.Timestamp,
				Err:       nil,
			})
			eLog.Request = go_hermes.Request{}
			h.entryLog.Put(m.Epoch_id, eLog)
		} else {
			log.Debugf("Haven't received all ACKs back")
		}
	} else {
		log.Errorf("entry not found in log for epochID: %d", m.Epoch_id)
	}
}

//// follower handlers
func (h *Hermes) HandleINV(m INV) {
	exists := h.HermesKeys.Contains(int(m.Key.Key))
	hKey := h.HermesKeys.Get(int(m.Key.Key)).(*go_hermes.KeyStruct)
	if exists {
		if h.isRecvdTimestampGreater(m.Key) {
			hKey.State = go_hermes.INVALID_STATE
			hKey.Ts = m.Key.Ts
			hKey.Value = m.Value
		}
	} else {
		log.Debugf("Key %v added to dictionary", m.Key)
		h.HermesKeys.Put(int(m.Key.Key), m.Key)
	}
	h.entryLog.Put(m.Epoch_id, &Entry{
		Quorum:    go_hermes.NewQuorum(),
		Request:   go_hermes.Request{},
		MltTicker: time.NewTicker(time.Duration(h.config.MLT) * time.Second),
		Timestamp: m.Key.Ts,
	})
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
		k := h.HermesKeys.Get(int(m.Key.Key)).(*go_hermes.KeyStruct)
		k.State = go_hermes.VALID_STATE
		h.HermesKeys.Put(int(m.Key.Key), k)
		e := h.entryLog.Get(m.Epoch_id).(*Entry)
		e.MltTicker.Stop()
		h.entryLog.Put(m.Epoch_id, e)
	}
}

/////////// Time stamp checks
func (h *Hermes) checkEqualTimestamps(m VAL) bool {
	exists := h.entryLog.Contains(m.Epoch_id)
	if !exists {
		log.Info("can't check equal timestamps, nothing to do")
		return false
	}
	e := h.entryLog.Get(m.Epoch_id).(*Entry)
	return e.Timestamp.Version == m.Key.Ts.Version &&
		e.Timestamp.C_id == m.Key.Ts.C_id
}

func (h *Hermes) isRecvdTimestampGreater(m *go_hermes.KeyStruct) bool {
	currTs := h.HermesKeys.Get(int(m.Key)).(*go_hermes.KeyStruct).Ts
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
	tick := h.entryLog.Get(epochId).(*Entry).MltTicker.C
	for {
		select {
		case <-tick:
			// ticker went off, which means there is a loss of message somewhere. Retrigger
			// the write once again
			h.entryLog.Put(r.Epoch_ID, &Entry{
				Quorum:    go_hermes.NewQuorum(),
				Request:   r,
				MltTicker: time.NewTicker(time.Duration(h.config.MLT) * time.Second),
			})
			h.entryLog.Get(r.Epoch_ID).(*Entry).Quorum.ACK(h.ID())
			h.HermesKeys.Put(int(r.Command.Key), r.KeyStruct)
			h.Broadcast(h.Node.ID(), msg)
		default:
			continue
		}
	}
}

func (h *Hermes) followerTicker(m INV) {
	e := h.entryLog.Get(m.Epoch_id).(*Entry)
	for {
		select {
		case <-e.MltTicker.C:
			if h.Node.IsCrashed() {
				log.Debugf("its a crashed node, no need to replay")
				e.MltTicker.Stop()
				h.entryLog.Delete(m.Epoch_id)
				return
			}
			log.Infof("Follower ticker for node %v timed out, changing to replay state: %v",
				h.Node.ID(), m)
			k := h.HermesKeys.Get(int(m.Key.Key)).(*go_hermes.KeyStruct)
			k.State = go_hermes.REPLAY_STATE
			h.HermesKeys.Put(int(m.Key.Key), k)
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
