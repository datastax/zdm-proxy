package migration

// Step is an enum of the migration steps for a table
type Step int

// Errored, Waiting, MigratingSchema, ... are the enums of the migration steps
const (
	Errored = iota
	Waiting
	MigratingSchema
	MigratingSchemaComplete
	WaitingToUnload
	UnloadingData
	UnloadingDataComplete
	LoadingData
	LoadingDataComplete
)
