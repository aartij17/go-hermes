package go_hermes

import (
	"encoding/json"
	"go-hermes/log"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

func (n *node) http() {
	mux := http.NewServeMux()
	mux.HandleFunc("/", n.handleRoot)

	url, err := url.Parse(config.HTTPAddrs[n.id])
	if err != nil {
		log.Fatal("http url parse error: ", err)
	}
	port := ":" + url.Port()
	n.server = &http.Server{
		Addr:    port,
		Handler: mux,
	}
	log.Info("http server starting on ", port)
	log.Fatal(n.server.ListenAndServe())
}

// TODO: Complete this method based on the algorithm
func (n *node) handleRoot(w http.ResponseWriter, r *http.Request) {
	var req Request
	var cmd Command
	var err error

	if len(r.URL.Path) > 1 {
		i, err := strconv.Atoi(r.URL.Path[1:])
		if err != nil {
			http.Error(w, "invalid path", http.StatusBadRequest)
			log.Error(err)
			return
		}
		cmd.Key = Key(i)
		if r.Method == http.MethodPost {
			body, err := ioutil.ReadAll(r.Body)
			if err != nil {
				log.Error("error reading request body", err)
				http.Error(w, "cannot read body", http.StatusBadRequest)
				return
			}
			cmd.Value = Value(body)
		}
	} else {
		body, err := ioutil.ReadAll(r.Body)
		if err != nil {
			log.Error("error reading body: ", err)
			http.Error(w, "cannot read body", http.StatusBadRequest)
			return
		}
		json.Unmarshal(body, &cmd)
	}
	req.Command = cmd
	req.Timestamp = time.Now().UnixNano()
	req.NodeID = n.id
	req.c = make(chan Reply, 1)

	n.MessageChan <- req
	reply := <-req.c
	_, err = io.WriteString(w, string(reply.Value))
	if err != nil {
		log.Error(err)
	}
}
