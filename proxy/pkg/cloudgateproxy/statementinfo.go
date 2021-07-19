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
	prepareForwardDecision forwardDecision
	baseStatementInfo      StatementInfo
}

func NewPreparedStatementInfo(prepareForwardDecision forwardDecision, baseStmtInfo StatementInfo) *PreparedStatementInfo {
	return &PreparedStatementInfo{prepareForwardDecision: prepareForwardDecision, baseStatementInfo: baseStmtInfo}
}

func (recv *PreparedStatementInfo) String() string {
	return fmt.Sprintf("PreparedStatementInfo{prepareForwardDecision: %v, baseStatementInfo: %v}",
		recv.prepareForwardDecision, recv.baseStatementInfo)
}

func (recv *PreparedStatementInfo) GetForwardDecision() forwardDecision {
	return recv.prepareForwardDecision
}

func (recv *PreparedStatementInfo) GetBaseStatementInfo() StatementInfo {
	return recv.baseStatementInfo
}

type BoundStatementInfo struct {
	preparedData PreparedData
}

func NewBoundStatementInfo(preparedData PreparedData) *BoundStatementInfo {
	return &BoundStatementInfo{preparedData: preparedData}
}

func (recv *BoundStatementInfo) String() string {
	return fmt.Sprintf("BoundStatementInfo{PreparedData: %v}", recv.preparedData)
}

func (recv *BoundStatementInfo) GetForwardDecision() forwardDecision {
	return recv.preparedData.GetPreparedStatementInfo().GetBaseStatementInfo().GetForwardDecision()
}

func (recv *BoundStatementInfo) GetPreparedData() PreparedData {
	return recv.preparedData
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