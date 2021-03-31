package go_hermes

import (
	"encoding/json"
	"flag"
	"go-hermes/log"
	"os"
)

var configFile = flag.String("config", "config.json",
	"Configuration file for hermes, defaults to config.json")

type Config struct {
	Addrs     map[ID]string
	HTTPAddrs map[ID]string
	MLT       int

	n   int         // total number of nodes
	npz map[int]int // nodes per zone
	z   int         // number of zones
}

var config Config

func GetConfig() Config {
	return config
}

func (c Config) N() int {
	return c.z
}

func (c Config) Z() int {
	return c.z
}

func (c *Config) Load() {
	file, err := os.Open(*configFile)
	if err != nil {
		log.Fatal(err)
	}
	decoder := json.NewDecoder(file)
	err = decoder.Decode(c)
	if err != nil {
		log.Fatal(err)
	}
	c.npz = make(map[int]int)
	for id := range c.Addrs {
		c.n++
		c.npz[id.Zone()]++
	}
	c.z = len(c.npz)
}
