package cloudgateproxy

import "fmt"

type StatementInfo interface {
	GetForwardDecision() forwardDecision
	ShouldBeSentAsync() bool
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
	shouldBeSentAsync bool
}

func NewGenericStatementInfo(decision forwardDecision, shouldBeSentAsync bool) *GenericStatementInfo {
	return &GenericStatementInfo{baseStatementInfo: newBaseStmtInfo(decision), shouldBeSentAsync: shouldBeSentAsync}
}

func (recv *GenericStatementInfo) String() string {
	return fmt.Sprintf("GenericStatementInfo{forwardDecision: %v, shouldBeSentAsync=%v}", recv.forwardDecision, recv.shouldBeSentAsync)
}

func (recv *GenericStatementInfo) ShouldBeSentAsync() bool {
	return recv.shouldBeSentAsync
}

type PreparedStatementInfo struct {
	baseStatementInfo StatementInfo
	query             string
	keyspace          string
}

func NewPreparedStatementInfo(baseStmtInfo StatementInfo, query string, keyspace string) *PreparedStatementInfo {
	return &PreparedStatementInfo{baseStatementInfo: baseStmtInfo, query: query, keyspace: keyspace}
}

func (recv *PreparedStatementInfo) String() string {
	return fmt.Sprintf("PreparedStatementInfo{baseStatementInfo: %v, query: %v, keyspace: %v}",
		recv.baseStatementInfo, recv.query, recv.keyspace)
}

func (recv *PreparedStatementInfo) GetQuery() string {
	return recv.query
}

func (recv *PreparedStatementInfo) GetKeyspace() string {
	return recv.keyspace
}

func (recv *PreparedStatementInfo) GetForwardDecision() forwardDecision {
	return forwardToBoth // always send PREPARE to both, use origin's ID
}

func (recv *PreparedStatementInfo) ShouldBeSentAsync() bool {
	return recv.GetBaseStatementInfo().ShouldBeSentAsync()
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

func (recv *BoundStatementInfo) ShouldBeSentAsync() bool {
	return recv.preparedData.GetPreparedStatementInfo().GetBaseStatementInfo().ShouldBeSentAsync()
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

func (recv *InterceptedStatementInfo) ShouldBeSentAsync() bool {
	return false
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

func (recv *BatchStatementInfo) ShouldBeSentAsync() bool {
	return false
}

func (recv *BatchStatementInfo) GetPreparedDataByStmtIdx() map[int]PreparedData {
	return recv.preparedDataByStmtIdx
}