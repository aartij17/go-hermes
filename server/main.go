package main

import (
	"flag"
	"go-hermes"
)

var id = flag.String("id", "", "ID in format of Zone.Node.")

func replica(id go_hermes.ID) {
	go_hermes.NewReplica(id).Run()
}
func main() {
	go_hermes.Init()
	replica(go_hermes.ID(*id))
}
