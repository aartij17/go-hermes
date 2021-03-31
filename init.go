package go_hermes

import (
	"flag"
	"go-hermes/log"
)

func Init() {
	flag.Parse()
	log.Setup()
	config.Load()
}
