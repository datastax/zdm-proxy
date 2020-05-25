package migration

import (
	"sync"
	"time"

	"github.com/gocql/gocql"
)

// Status represents status of migration
type Status struct {
	Timestamp time.Time
	Tables    map[string]map[string]*Table
	Lock      *sync.Mutex
}

// Status constructor
func newStatus() *Status {
	var status Status
	status.Tables = make(map[string]map[string]*Table)
	status.Lock = new(sync.Mutex)
	return &status
}

// Populates the Status with initial values in accordance w/ the given TableMetadata
func (s *Status) initTableData(tables map[string]map[string]*gocql.TableMetadata) {
	for keyspace, keyspaceTables := range tables {
		t := make(map[string]*Table)
		s.Tables[keyspace] = t
		for table := range keyspaceTables {
			s.Tables[keyspace][table] = &(Table{
				Keyspace: keyspace,
				Name:     table,
				Step:     Waiting,
				Error:    nil,
				Priority: 1,
				Lock:     new(sync.Mutex),
				Redo:     false,
			})
		}
	}
}
