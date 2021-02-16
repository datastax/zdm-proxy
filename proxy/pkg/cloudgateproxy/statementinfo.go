package cloudgateproxy

import "fmt"

type StatementInfo interface {
	GetForwardDecision() forwardDecision
}

type PreparedStatementInfo struct {
	*baseStatementInfo
}

type GenericStatementInfo struct {
	*baseStatementInfo
}

type baseStatementInfo struct {
	forwardDecision forwardDecision
}

func (recv *baseStatementInfo) GetForwardDecision() forwardDecision {
	return recv.forwardDecision
}

func NewGenericStatementInfo(decision forwardDecision) *GenericStatementInfo {
	return &GenericStatementInfo{baseStatementInfo: newBaseStmtInfo(decision)}
}

func NewPreparedStatementInfo(decision forwardDecision) *PreparedStatementInfo {
	return &PreparedStatementInfo{baseStatementInfo: newBaseStmtInfo(decision)}
}

func newBaseStmtInfo(decision forwardDecision) *baseStatementInfo {
	return &baseStatementInfo{forwardDecision: decision}
}

func (recv *GenericStatementInfo) String() string {
	return fmt.Sprintf("GenericStatementInfo{forwardDecision: %v}", recv.forwardDecision)
}

func (recv *PreparedStatementInfo) String() string {
	return fmt.Sprintf("PreparedStatementInfo{forwardDecision: %v}", recv.forwardDecision)
}