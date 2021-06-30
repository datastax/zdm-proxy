package cloudgateproxy

import "fmt"

type StatementInfo interface {
	GetForwardDecision() forwardDecision
}

type baseStatementInfo struct {
	forwardDecision forwardDecision
}

func newBaseStmtInfo(decision forwardDecision) *baseStatementInfo {
	return &baseStatementInfo{forwardDecision: decision}
}

func (recv *baseStatementInfo) GetForwardDecision() forwardDecision {
	return recv.forwardDecision
}

type GenericStatementInfo struct {
	*baseStatementInfo
}

func NewGenericStatementInfo(decision forwardDecision) *GenericStatementInfo {
	return &GenericStatementInfo{baseStatementInfo: newBaseStmtInfo(decision)}
}

func (recv *GenericStatementInfo) String() string {
	return fmt.Sprintf("GenericStatementInfo{forwardDecision: %v}", recv.forwardDecision)
}

type PreparedStatementInfo struct {
	baseStatementInfo StatementInfo
}

func NewPreparedStatementInfo(baseStmtInfo StatementInfo) *PreparedStatementInfo {
	return &PreparedStatementInfo{baseStatementInfo: baseStmtInfo}
}

func (recv *PreparedStatementInfo) String() string {
	return fmt.Sprintf("PreparedStatementInfo{baseStatementInfo: %v}", recv.baseStatementInfo)
}

func (recv *PreparedStatementInfo) GetForwardDecision() forwardDecision {
	return recv.baseStatementInfo.GetForwardDecision()
}

func (recv *PreparedStatementInfo) GetBaseStatementInfo() StatementInfo {
	return recv.baseStatementInfo
}

type InterceptedStatementInfo struct {
	*baseStatementInfo
	interceptedQueryType interceptedQueryType
}

func NewInterceptedStatementInfo(queryType interceptedQueryType) *InterceptedStatementInfo {
	return &InterceptedStatementInfo{baseStatementInfo: newBaseStmtInfo(forwardToNone), interceptedQueryType: queryType}
}

func (recv *InterceptedStatementInfo) String() string {
	return fmt.Sprintf("InterceptedStatementInfo{interceptedQueryType: %v}", recv.interceptedQueryType)
}

func (recv *InterceptedStatementInfo) GetQueryType() interceptedQueryType {
	return recv.interceptedQueryType
}