package migration

import (
	"sync"
	"time"

	"github.com/gocql/gocql"
)

// Status represents status of migration
type Status struct {
	Timestamp  time.Time
	Tables     map[string]*map[string]*Table
	Steps      int
	TotalSteps int
	Speed      float64
	Lock       *sync.Mutex
}

// Status constructor
func newStatus() *Status {
	var status Status
	status.Tables = make(map[string]*map[string]*Table)
	status.Lock = new(sync.Mutex)
	return &status
}

// Populates the Status with initial values in accordance w/ the given TableMetadata
func (s *Status) initTableData(tables map[string]*map[string]*gocql.TableMetadata) {
	s.TotalSteps = 0
	for keyspace, keyspaceTables := range tables {
		s.TotalSteps += 3 * len(*keyspaceTables)
		t := make(map[string]*Table)
		s.Tables[keyspace] = &t
		for table := range *keyspaceTables {
			(*s.Tables[keyspace])[table] = &(Table{
				Name:     table,
				Step:     Waiting,
				Error:    nil,
				Priority: 0,
			})
		}
	}
}
