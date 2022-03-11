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
	baseStatementInfo         StatementInfo
	replacedTerms             []*term
	containsPositionalMarkers bool
}

func NewPreparedStatementInfo(
	baseStmtInfo StatementInfo,
	replacedTerms []*term,
	containsPositionalMarkers bool) *PreparedStatementInfo {
	return &PreparedStatementInfo{
		baseStatementInfo:         baseStmtInfo,
		replacedTerms:             replacedTerms,
		containsPositionalMarkers: containsPositionalMarkers}
}

func (recv *PreparedStatementInfo) String() string {
	return fmt.Sprintf("PreparedStatementInfo{baseStatementInfo: %v}", recv.baseStatementInfo)
}

func (recv *PreparedStatementInfo) GetForwardDecision() forwardDecision {
	return forwardToBoth // always send PREPARE to both, use origin's ID
}

func (recv *PreparedStatementInfo) GetBaseStatementInfo() StatementInfo {
	return recv.baseStatementInfo
}

func (recv *PreparedStatementInfo) GetReplacedTerms() []*term {
	return recv.replacedTerms
}

func (recv *PreparedStatementInfo) ContainsPositionalMarkers() bool {
	return recv.containsPositionalMarkers
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

type BatchStatementInfo struct {
	preparedDataByStmtIdx map[int]PreparedData
}

func NewBatchStatementInfo(preparedDataByStmtIdx map[int]PreparedData) *BatchStatementInfo {
	return &BatchStatementInfo{preparedDataByStmtIdx: preparedDataByStmtIdx}
}

func (recv *BatchStatementInfo) String() string {
	return fmt.Sprintf("BatchStatementInfo{PreparedDataByStmtIdx: %v}", recv.preparedDataByStmtIdx)
}

func (recv *BatchStatementInfo) GetForwardDecision() forwardDecision {
	return forwardToBoth // always send BATCH to both, use origin's prepared IDs
}

func (recv *BatchStatementInfo) GetPreparedDataByStmtIdx() map[int]PreparedData {
	return recv.preparedDataByStmtIdx
}