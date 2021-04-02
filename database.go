package go_hermes

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sync"
)

// Key type of the key-value database
// TODO key should be general too
type Key int

// Value type of key-value database
type Value []byte

// Command of key-value database
type Command struct {
	Key       Key
	Value     Value
	ClientID  ID
	CommandID int
}

func (c Command) Empty() bool {
	if c.Key == 0 && c.Value == nil && c.ClientID == "" && c.CommandID == 0 {
		return true
	}
	return false
}

func (c Command) IsRead() bool {
	return c.Value == nil
}

func (c Command) IsWrite() bool {
	return c.Value != nil
}

func (c Command) Equal(a Command) bool {
	return c.Key == a.Key && bytes.Equal(c.Value, a.Value) &&
		c.ClientID == a.ClientID && c.CommandID == a.CommandID
}

func (c Command) String() string {
	if c.Value == nil {
		return fmt.Sprintf("Get{key=%v id=%s cid=%d}", c.Key, c.ClientID, c.CommandID)
	}
	return fmt.Sprintf("Put{key=%v value=%x id=%s cid=%d", c.Key, c.Value, c.ClientID, c.CommandID)
}

// Database defines a database interface
// TODO replace with more general StateMachine interface
type Database interface {
	Execute(Command) Value
	History(Key) []Value
	Get(Key) Value
	Put(Key, Value)
}

// Database implements a multi-version key-value datastore as the StateMachine
type database struct {
	sync.RWMutex
	data         map[Key]Value
	version      int
	multiversion bool
	history      map[Key][]Value
}

// NewDatabase returns database that impelements Database interface
func NewDatabase() Database {
	return &database{
		data:         make(map[Key]Value),
		version:      0,
		multiversion: false,
		history:      make(map[Key][]Value),
	}
}

// Execute executes a command agaist database
func (d *database) Execute(c Command) Value {
	d.Lock()
	defer d.Unlock()

	// get previous value
	v := d.data[c.Key]

	// writes new value
	d.put(c.Key, c.Value)

	return v
}

// Get gets the current value and version of given key
func (d *database) Get(k Key) Value {
	d.RLock()
	defer d.RUnlock()
	return d.data[k]
}

func (d *database) put(k Key, v Value) {
	if v != nil {
		d.data[k] = v
		d.version++
		if d.multiversion {
			if d.history[k] == nil {
				d.history[k] = make([]Value, 0)
			}
			d.history[k] = append(d.history[k], v)
		}
	}
}

// Put puts a new value of given key
func (d *database) Put(k Key, v Value) {
	d.Lock()
	defer d.Unlock()
	d.put(k, v)
}

// Version returns current version of given key
func (d *database) Version(k Key) int {
	d.RLock()
	defer d.RUnlock()
	return d.version
}

// History returns entire vlue history in order
func (d *database) History(k Key) []Value {
	d.RLock()
	defer d.RUnlock()
	return d.history[k]
}

func (d *database) String() string {
	d.RLock()
	defer d.RUnlock()
	b, _ := json.Marshal(d.data)
	return string(b)
}
