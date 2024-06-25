package zdmproxy

import "fmt"

type RequestInfo interface {
	GetForwardDecision() forwardDecision
	ShouldAlsoBeSentAsync() bool
	ShouldBeTrackedInMetrics() bool
}

type baseRequestInfo struct {
	forwardDecision       forwardDecision
	shouldAlsoBeSentAsync bool
	trackMetrics          bool
}

func newBaseRequestInfo(decision forwardDecision, shouldBeSentAsync bool, trackMetrics bool) *baseRequestInfo {
	return &baseRequestInfo{forwardDecision: decision, shouldAlsoBeSentAsync: shouldBeSentAsync, trackMetrics: trackMetrics}
}

func (recv *baseRequestInfo) GetForwardDecision() forwardDecision {
	return recv.forwardDecision
}

func (recv *baseRequestInfo) ShouldAlsoBeSentAsync() bool {
	return recv.shouldAlsoBeSentAsync
}

func (recv *baseRequestInfo) ShouldBeTrackedInMetrics() bool {
	return recv.trackMetrics
}

type GenericRequestInfo struct {
	*baseRequestInfo
}

func NewGenericRequestInfo(decision forwardDecision, shouldBeSentAsync bool, trackMetrics bool) *GenericRequestInfo {
	return &GenericRequestInfo{baseRequestInfo: newBaseRequestInfo(decision, shouldBeSentAsync, trackMetrics)}
}

func (recv *GenericRequestInfo) String() string {
	return fmt.Sprintf("GenericRequestInfo{forwardDecision: %v, shouldAlsoBeSentAsync=%v, trackMetrics=%v}",
		recv.forwardDecision, recv.shouldAlsoBeSentAsync, recv.trackMetrics)
}

type PrepareRequestInfo struct {
	baseRequestInfo           RequestInfo
	fullyQualified            bool
	replacedTerms             []*term
	containsPositionalMarkers bool
	currentKeyspace           string
	query                     string
	keyspace                  string
}

func NewPrepareRequestInfo(
	baseRequestInfo RequestInfo,
	fullyQualified bool,
	replacedTerms []*term,
	containsPositionalMarkers bool,
	currentKeyspace string,
	query string,
	keyspace string) *PrepareRequestInfo {
	return &PrepareRequestInfo{
		baseRequestInfo:           baseRequestInfo,
		fullyQualified:            fullyQualified,
		replacedTerms:             replacedTerms,
		containsPositionalMarkers: containsPositionalMarkers,
		currentKeyspace:           currentKeyspace,
		query:                     query,
		keyspace:                  keyspace}
}

func (recv *PrepareRequestInfo) String() string {
	return fmt.Sprintf("PrepareRequestInfo{baseRequestInfo: %v, query: %v, keyspace: %v}",
		recv.baseRequestInfo, recv.query, recv.keyspace)
}

func (recv *PrepareRequestInfo) ShouldAlsoBeSentAsync() bool {
	return recv.baseRequestInfo.ShouldAlsoBeSentAsync()
}

func (recv *PrepareRequestInfo) ShouldBeTrackedInMetrics() bool {
	return false
}

func (recv *PrepareRequestInfo) IsFullyQualified() bool {
	return recv.fullyQualified
}

func (recv *PrepareRequestInfo) GetQuery() string {
	return recv.query
}

func (recv *PrepareRequestInfo) GetKeyspace() string {
	return recv.keyspace
}

func (recv *PrepareRequestInfo) GetCurrentKeyspace() string {
	return recv.currentKeyspace
}

func (recv *PrepareRequestInfo) GetForwardDecision() forwardDecision {
	return recv.GetBaseRequestInfo().GetForwardDecision()
}

func (recv *PrepareRequestInfo) GetBaseRequestInfo() RequestInfo {
	return recv.baseRequestInfo
}

func (recv *PrepareRequestInfo) GetReplacedTerms() []*term {
	return recv.replacedTerms
}

func (recv *PrepareRequestInfo) ContainsPositionalMarkers() bool {
	return recv.containsPositionalMarkers
}

type ExecuteRequestInfo struct {
	preparedEntry PreparedEntry
}

func NewExecuteRequestInfo(preparedEntry PreparedEntry) *ExecuteRequestInfo {
	return &ExecuteRequestInfo{preparedEntry: preparedEntry}
}

func (recv *ExecuteRequestInfo) String() string {
	return fmt.Sprintf("ExecuteRequestInfo{PreparedData: %v}", recv.preparedEntry)
}

func (recv *ExecuteRequestInfo) GetForwardDecision() forwardDecision {
	return recv.preparedEntry.GetPrepareRequestInfo().GetBaseRequestInfo().GetForwardDecision()
}

func (recv *ExecuteRequestInfo) GetPreparedEntry() PreparedEntry {
	return recv.preparedEntry
}

func (recv *ExecuteRequestInfo) ShouldAlsoBeSentAsync() bool {
	return recv.preparedEntry.GetPrepareRequestInfo().GetBaseRequestInfo().ShouldAlsoBeSentAsync()
}

func (recv *ExecuteRequestInfo) ShouldBeTrackedInMetrics() bool {
	return recv.preparedEntry.GetPrepareRequestInfo().GetBaseRequestInfo().ShouldBeTrackedInMetrics()
}

// InterceptedRequestInfo on its own means that this intercepted request is a QUERY request.
// This can also be the base request field of a PrepareRequestInfo object in which case the intercepted request will be
// a PREPARE (or EXECUTE if it's a ExecuteRequestInfo).
type InterceptedRequestInfo struct {
	*baseRequestInfo
	interceptedQueryType interceptedQueryType
	parsedSelectClause   *selectClause
}

func NewInterceptedRequestInfo(
	queryType interceptedQueryType, parsedSelectClause *selectClause) *InterceptedRequestInfo {
	return &InterceptedRequestInfo{
		baseRequestInfo:      newBaseRequestInfo(forwardToNone, false, false),
		interceptedQueryType: queryType,
		parsedSelectClause:   parsedSelectClause}
}

func (recv *InterceptedRequestInfo) String() string {
	return fmt.Sprintf("InterceptedRequestInfo{interceptedQueryType: %v, parsedSelectClause: %v}",
		recv.interceptedQueryType, recv.parsedSelectClause)
}

func (recv *InterceptedRequestInfo) GetQueryType() interceptedQueryType {
	return recv.interceptedQueryType
}

func (recv *InterceptedRequestInfo) GetParsedSelectClause() *selectClause {
	return recv.parsedSelectClause
}

type BatchRequestInfo struct {
	preparedDataByStmtIdx map[int]PreparedEntry
}

func NewBatchRequestInfo(preparedDataByStmtIdx map[int]PreparedEntry) *BatchRequestInfo {
	return &BatchRequestInfo{preparedDataByStmtIdx: preparedDataByStmtIdx}
}

func (recv *BatchRequestInfo) String() string {
	return fmt.Sprintf("BatchRequestInfo{PreparedDataByStmtIdx: %v}", recv.preparedDataByStmtIdx)
}

func (recv *BatchRequestInfo) GetForwardDecision() forwardDecision {
	return forwardToBoth // always send BATCH to both, use origin's prepared IDs
}

func (recv *BatchRequestInfo) ShouldAlsoBeSentAsync() bool {
	return false
}

func (recv *BatchRequestInfo) ShouldBeTrackedInMetrics() bool {
	return true
}

func (recv *BatchRequestInfo) GetPreparedDataByStmtIdx() map[int]PreparedEntry {
	return recv.preparedDataByStmtIdx
}
