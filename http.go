package go_hermes

import (
	"go-hermes/log"
	"io"
	"net/http"
)

func (n *node) http() {
	mux := http.NewServeMux()
	mux.HandleFunc("/", n.handleRoot)
}

// TODO: Complete this method based on the algorithm
func (n *node) handleRoot(w http.ResponseWriter, r *http.Request) {
	var req Request
	// var cmd Command
	var err error

	n.MessageChan <- req
	reply := <-req.c
	_, err = io.WriteString(w, string(reply.Value))
	if err != nil {
		log.Error(err)
	}
}
