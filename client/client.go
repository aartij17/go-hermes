package main

import (
	"encoding/binary"
	"flag"
	go_hermes "go-hermes"
	"go-hermes/hermes"
	"strconv"
)

var id = flag.String("id", "", "node id this client connects to")
var zone = flag.String("zone", "", "zone id this client connects to")
var algorithm = flag.String("algorithm", "", "Client API type [paxos, chain]")
var load = flag.Bool("load", false, "Load K keys into DB")
var master = flag.String("master", "", "Master address.")

// db implements Paxi.DB interface for benchmarking
type db struct {
	go_hermes.Client
}

func (d *db) Init() error {
	return nil
}

func (d *db) Stop() error {
	return nil
}

func (d *db) Read(k int) (int, error) {
	key := go_hermes.Key(k)
	v, err := d.Get(key)
	if len(v) == 0 {
		return 0, nil
	}
	x, _ := binary.Uvarint(v)
	return int(x), err
}

func (d *db) Write(k, v int) error {
	key := go_hermes.Key(k)
	value := make([]byte, binary.MaxVarintLen64)
	binary.PutUvarint(value, uint64(v))
	err := d.Put(key, value)
	return err
}

func main() {
	go_hermes.Init()

	//if *master != "" {
	//	paxi.ConnectToMaster(*master, true, paxi.ID(*id))
	//}

	d := new(db)
	zone_int, _ := strconv.Atoi(*zone)
	d.Client = hermes.NewClient(go_hermes.ID(*id), zone_int)
	//switch *algorithm {
	//case "paxos":
	//
	//case "chain":
	//	d.Client = chain.NewClient()
	//default:
	//	d.Client = paxi.NewHTTPClient(paxi.ID(*id))
	//}

	b := go_hermes.NewBenchmark(d)
	if *load {
		b.Load()
	} else {
		b.Run()
	}
}
