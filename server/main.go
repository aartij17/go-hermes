package main

import (
	"flag"
	"go-hermes"
	"go-hermes/hermes"
)

var id = flag.String("id", "", "ID in format of Zone.Node.")

func replica(id go_hermes.ID) {
	hermes.NewReplica(id).Run()
}
func main() {
	go_hermes.Init()
	replica(go_hermes.ID(*id))
}
