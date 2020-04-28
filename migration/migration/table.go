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
	t.Lock.Lock()
	defer t.Lock.Unlock()

	t.Step = newData.Step
	t.Error = newData.Error
	t.Priority = newData.Priority
}

func (t *Table) SetStep(step Step) {
	t.Lock.Lock()
	defer t.Lock.Unlock()

	t.Step = step
}

func (t *Table) SetPriority(priority int) {
	t.Lock.Lock()
	defer t.Lock.Unlock()

	t.Priority = priority
}

func (t *Table) SetErr(err error) {
	t.Lock.Lock()
	defer t.Lock.Unlock()

	t.Step = Errored
	t.Error = err
}
