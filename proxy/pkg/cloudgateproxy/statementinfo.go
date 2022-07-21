package cloudgateproxy

import "fmt"

type StatementInfo interface {
	GetForwardDecision() forwardDecision
	ShouldAlsoBeSentAsync() bool
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
	shouldAlsoBeSentAsync bool
}

func NewGenericStatementInfo(decision forwardDecision, shouldBeSentAsync bool) *GenericStatementInfo {
	return &GenericStatementInfo{baseStatementInfo: newBaseStmtInfo(decision), shouldAlsoBeSentAsync: shouldBeSentAsync}
}

func (recv *GenericStatementInfo) String() string {
	return fmt.Sprintf("GenericStatementInfo{forwardDecision: %v, shouldAlsoBeSentAsync=%v}", recv.forwardDecision, recv.shouldAlsoBeSentAsync)
}

func (recv *GenericStatementInfo) ShouldAlsoBeSentAsync() bool {
	return recv.shouldAlsoBeSentAsync
}

type PreparedStatementInfo struct {
	baseStatementInfo         StatementInfo
	replacedTerms             []*term
	containsPositionalMarkers bool
	query                     string
	keyspace                  string
}

func NewPreparedStatementInfo(
	baseStmtInfo StatementInfo,
	replacedTerms []*term,
	containsPositionalMarkers bool,
	query string,
	keyspace string) *PreparedStatementInfo {
	return &PreparedStatementInfo{
		baseStatementInfo:         baseStmtInfo,
		replacedTerms:             replacedTerms,
		containsPositionalMarkers: containsPositionalMarkers,
		query:                     query,
		keyspace:                  keyspace}
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

func (recv *PreparedStatementInfo) ShouldAlsoBeSentAsync() bool {
	return recv.GetBaseStatementInfo().ShouldAlsoBeSentAsync()
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

func (recv *BoundStatementInfo) ShouldAlsoBeSentAsync() bool {
	return recv.preparedData.GetPreparedStatementInfo().GetBaseStatementInfo().ShouldAlsoBeSentAsync()
}

type InterceptedStatementInfo struct {
	*baseStatementInfo
	interceptedQueryType interceptedQueryType
	parsedSelectClause   *selectClause
}

func NewInterceptedStatementInfo(queryType interceptedQueryType, parsedSelectClause *selectClause) *InterceptedStatementInfo {
	return &InterceptedStatementInfo{
		baseStatementInfo:    newBaseStmtInfo(forwardToNone),
		interceptedQueryType: queryType,
		parsedSelectClause:   parsedSelectClause}
}

func (recv *InterceptedStatementInfo) String() string {
	return fmt.Sprintf("InterceptedStatementInfo{interceptedQueryType: %v, parsedSelectClause: %v}",
		recv.interceptedQueryType, recv.parsedSelectClause)
}

func (recv *InterceptedStatementInfo) GetQueryType() interceptedQueryType {
	return recv.interceptedQueryType
}

func (recv *InterceptedStatementInfo) ShouldAlsoBeSentAsync() bool {
	return false
}

func (recv *InterceptedStatementInfo) GetParsedSelectClause() *selectClause {
	return recv.parsedSelectClause
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

func (recv *BatchStatementInfo) ShouldAlsoBeSentAsync() bool {
	return false
}

func (recv *BatchStatementInfo) GetPreparedDataByStmtIdx() map[int]PreparedData {
	return recv.preparedDataByStmtIdx
}