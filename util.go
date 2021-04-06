package go_hermes

import (
	"fmt"
	"go-hermes/log"
	"time"
)

func Retry(f func() error, attempts int, sleep time.Duration) error {
	log.Debugf("Dial send retry")
	var err error
	for i := 0; ; i++ {
		err = f()
		if err == nil {
			return nil
		}
		if i >= attempts-1 {
			break
		}
		time.Sleep(sleep * time.Duration(i+1))
	}
	return fmt.Errorf("after %d attempts, last error: %s", attempts, err)
}
