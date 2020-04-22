package migration

import "sync"

// Table represents status of migration of a single table
type Table struct {
	Keyspace string
	Name     string
	Step     Step
	Error    error
	Priority int

	Lock *sync.Mutex
}

// Update changes Table information
func (t *Table) Update(newData *Table) {
	t.Step = newData.Step
	t.Error = newData.Error
	t.Priority = newData.Priority
}
