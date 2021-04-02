package hermes

import (
	go_hermes "go-hermes"
	"go-hermes/log"
	"sync"
	"time"
)

type Entry struct {
	Quorum  *go_hermes.Quorum
	Request *go_hermes.Request
}

type Hermes struct {
	go_hermes.Node
	config []go_hermes.ID

	quorum *go_hermes.Quorum

	Q func(*go_hermes.Quorum) bool

	entryLock       sync.RWMutex
	entryLog        map[int]*Entry
	HermesKeys      map[int]*go_hermes.KeyStruct
	ReplyWhenCommit bool // use for optimisation
	Epoch_ID        int
}

func NewHermes(n go_hermes.Node, options ...func(*Hermes)) *Hermes {
	h := &Hermes{
		Node:            n,
		quorum:          go_hermes.NewQuorum(),
		Q:               func(q *go_hermes.Quorum) bool { return q.All() },
		ReplyWhenCommit: false,
		Epoch_ID:        0,
		entryLog:        make(map[int]*Entry),
		HermesKeys:      make(map[int]*go_hermes.KeyStruct),
		//slot:            -1,
	}
	for _, opt := range options {
		opt(h)
	}
	return h
}

func (h *Hermes) HandleRequest(r go_hermes.Request) {
	log.Debugf("Replica %v received %v", h.ID(), r)
	// 1. check if the key is in a VALID state, if not,
	// stall the request and check again after a while
	for {
		state, exists := h.CheckKeyState(r)
		if exists {
			switch state {
			case go_hermes.VALID_STATE:
				newVersion := h.HermesKeys[int(r.Command.Key)].Ts.Version + 1
				r.KeyStruct = &go_hermes.KeyStruct{
					Key: r.Command.Key,
					Ts: go_hermes.Timestamp{
						Version: newVersion,
						C_id:    h.Node.ID(),
					},
					State: go_hermes.INVALID_STATE,
				}
				break
			default:
				log.Info("sleeping, waiting for the key to become valid!")
				time.Sleep(5 * time.Second)
				continue
			}
		} else {
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

func (h *Hermes) CheckKeyState(r go_hermes.Request) (string, bool) {
	key, exists := h.HermesKeys[int(r.Command.Key)]
	if exists {
		return key.State, true
	}
	return "", false
}

func (h *Hermes) broadcastRequest(r go_hermes.Request) {
	h.entryLock.Lock()
	defer h.entryLock.Unlock()

	h.quorum.Reset()
	h.quorum.ACK(h.ID())

	h.entryLog[r.Epoch_ID] = &Entry{
		Quorum:  go_hermes.NewQuorum(),
		Request: &r,
	}
	h.entryLog[r.Epoch_ID].Quorum.ACK(h.ID())

	h.HermesKeys[int(r.Command.Key)] = r.KeyStruct
	log.Info("Broadcasting INV")
	h.Broadcast(INV{
		Key:        r.KeyStruct,
		Epoch_id:   r.Epoch_ID,
		Value:      r.Value,
		FromNodeID: h.Node.ID(),
	})

	h.Node.Execute(r.Command) // --> doesn't matter, can remove it.
}

func (h *Hermes) HandleACK(m ACK) {
	// TODO: Contact membership service for all live replicas
	h.entryLock.RLock()
	defer h.entryLock.RUnlock()

	_, exists := h.entryLog[m.Epoch_id]
	if exists {
		h.entryLog[m.Epoch_id].Quorum.ADD()
		if h.entryLog[m.Epoch_id].Quorum.All() {
			log.Info("Quorum found, Broadcasting VAL")
			h.Broadcast(VAL{
				Key:        m.Key,
				Epoch_id:   m.Epoch_id,
				FromNodeID: h.Node.ID(),
			})
			h.HermesKeys[int(m.Key.Key)].State = go_hermes.VALID_STATE
			h.entryLog[m.Epoch_id].Quorum.Reset()
			h.entryLog[m.Epoch_id].Request.Reply(go_hermes.Reply{
				Command:   h.entryLog[m.Epoch_id].Request.Command,
				Value:     h.entryLog[m.Epoch_id].Request.Value,
				Timestamp: h.entryLog[m.Epoch_id].Request.Timestamp,
				Err:       nil,
			})
			h.entryLog[m.Epoch_id].Request = nil
		} else {
			log.Infof("Haven't received all ACKs back")
		}
	} else {
		log.Errorf("entry not found in log for epochID: %d", m.Epoch_id)
	}
}

func (h *Hermes) HandleINV(m INV) {
	_, exists := h.HermesKeys[int(m.Key.Key)]
	if exists {
		if h.isRecvdTimestampGreater(m.Key) {
			h.HermesKeys[int(m.Key.Key)].State = go_hermes.INVALID_STATE
			// update key's timestamp and value
			h.HermesKeys[int(m.Key.Key)].Ts = m.Key.Ts
			h.HermesKeys[int(m.Key.Key)].Value = m.Value
		}
	} else {
		h.HermesKeys[int(m.Key.Key)] = m.Key
	}
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
		h.HermesKeys[int(m.Key.Key)].State = go_hermes.VALID_STATE
	}
}

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
