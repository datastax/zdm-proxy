package httpzdmproxy

import (
	log "github.com/sirupsen/logrus"
	"net/http"
	"sync"
)

func StartHttpServer(addr string, wg *sync.WaitGroup) *http.Server {
	srv := &http.Server{Addr: addr}

	wg.Add(1)
	go func() {
		defer wg.Done()

		if err := srv.ListenAndServe(); err != http.ErrServerClosed {
			log.Errorf("Failed to listen on the metrics endpoint: %v. "+
				"The proxy will stay up and listen for CQL requests.", err)
		}
	}()

	return srv
}
