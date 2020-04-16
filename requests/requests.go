package requests

// RequestType is an enum of types of requests between the proxy and migration services
type RequestType int

// Errored, Waiting, MigratingSchema, ... are the enums of the migration steps
const (
	TableUpdate = iota
	Start
	Complete
	Shutdown
)

// Request represents a request between the migration and proxy services
type Request struct {
	Type RequestType
	Data []byte
}
