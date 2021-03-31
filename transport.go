package go_hermes

import (
	"encoding/gob"
	"errors"
	"flag"
	"go-hermes/log"
	"net"
	"net/url"
	"strings"
	"sync"
)

var scheme = flag.String("transport", "tcp", "transport scheme (tcp, udp, chan), default tcp")

type Transport interface {
	Scheme() string
	Send(interface{})
	Recv() interface{}
	Dial() error
	Listen()
	Close()
}

type transport struct {
	uri   *url.URL
	send  chan interface{}
	recv  chan interface{}
	close chan struct{}
}

func NewTransport(addr string) Transport {
	if !strings.Contains(addr, "://") {
		addr = *scheme + "://" + addr
	}
	uri, err := url.Parse(addr)
	if err != nil {
		log.Fatalf("error parsing address %s: %s\n", addr, err)
	}
	transport := &transport{
		uri:   uri,
		send:  make(chan interface{}),
		recv:  make(chan interface{}),
		close: make(chan struct{}),
	}
	switch uri.Scheme {
	case "chan":
		t := new(channel)
		t.transport = transport
		return t
	case "tcp":
		t := new(tcp)
		t.transport = transport
		return t
	default:
		log.Fatalf("unknown scheme %s", uri.Scheme)
	}
	return nil
}

func (t *transport) Send(m interface{}) {
	t.send <- m
}

func (t *transport) Recv() interface{} {
	return <-t.recv
}

func (t *transport) Close() {
	close(t.send)
	close(t.close)
}

func (t *transport) Scheme() string {
	return t.uri.Scheme
}

func (t *transport) Dial() error {
	conn, err := net.Dial(t.Scheme(), t.uri.Host)
	if err != nil {
		return err
	}
	go func(conn net.Conn) {
		encoder := gob.NewEncoder(conn)
		defer conn.Close()
		for m := range t.send {
			err := encoder.Encode(&m)
			if err != nil {
				log.Error(err)
			}
		}
	}(conn)
	return nil
}

type tcp struct {
	*transport
}

func (t *tcp) Listen() {
	log.Debug("start listening ", t.uri.Port())
	listener, err := net.Listen("tcp", ":"+t.uri.Port())
	if err != nil {
		log.Fatal("TCP listener error: ", err)
	}

	go func(listener net.Listener) {
		defer listener.Close()
		for {
			conn, err := listener.Accept()
			if err != nil {
				log.Error("TCP Accept error: ", err)
				continue
			}
			go func(conn net.Conn) {
				decoder := gob.NewDecoder(conn)
				defer conn.Close()
				for {
					select {
					case <-t.close:
						return
					default:
						var m interface{}
						err := decoder.Decode(&m)
						if err != nil {
							log.Error(err)
							log.Error("this is where I changed stuff!")
							return
						}
						t.recv <- m
					}
				}
			}(conn)
		}
	}(listener)
}

var chans = make(map[string]chan interface{})
var chansLock sync.RWMutex

type channel struct {
	*transport
}

func (c *channel) Listen() {
	chansLock.Lock()
	defer chansLock.Unlock()
	chans[c.uri.Host] = make(chan interface{})
	go func(conn <-chan interface{}) {
		for {
			select {
			case <-c.close:
				return
			case m := <-conn:
				c.recv <- m
			}
		}
	}(chans[c.uri.Host])
}

func (c *channel) Scheme() string {
	return "chan"
}

func (c *channel) Dial() error {
	chansLock.RLock()
	defer chansLock.RUnlock()

	conn, OK := chans[c.uri.Host]
	if !OK {
		return errors.New("server not ready")
	}
	go func(conn chan<- interface{}) {
		for m := range c.send {
			conn <- m
		}
	}(conn)
	return nil
}
