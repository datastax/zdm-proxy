package migration

import (
	"cloud-gate/updates"
	"encoding/json"

	log "github.com/sirupsen/logrus"
)

type Comms struct {
	m *Migration
}

func (c *Comms) sendRequest(req *updates.Update) {
	c.m.updateLock.Lock()
	c.m.outstandingUpdates[req.ID] = req
	c.m.updateLock.Unlock()

	err := updates.Send(req, c.m.conn)
	if err != nil {
		log.WithError(err).Errorf("Error sending request %s", req.ID)
	}
}

func (c *Comms) sendStart() {
	bytes, err := json.Marshal(c.m.status)
	if err != nil {
		log.WithError(err).Fatal("Error marshalling status for start signal")
	}

	c.sendRequest(updates.New(updates.Start, bytes))
}

func (c *Comms) sendComplete() {
	bytes, err := json.Marshal(c.m.status)
	if err != nil {
		log.WithError(err).Fatal("Error marshalling status for complete signal")
	}

	c.sendRequest(updates.New(updates.Complete, bytes))
}

func (c *Comms) sendTableUpdate(table *Table) {
	bytes, err := json.Marshal(table)
	if err != nil {
		log.WithError(err).Fatal("Error marshalling table for update")
	}

	c.sendRequest(updates.New(updates.TableUpdate, bytes))
}
