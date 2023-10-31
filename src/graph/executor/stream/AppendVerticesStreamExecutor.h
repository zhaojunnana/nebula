// Copyright (c) 2020 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#ifndef GRAPH_EXECUTOR_QUERY_APPENDVERTICESSTREAMEXECUTOR_H_
#define GRAPH_EXECUTOR_QUERY_APPENDVERTICESSTREAMEXECUTOR_H_

#include "graph/executor/stream/GetPropStreamExecutor.h"
#include "graph/planner/plan/Query.h"
// only used in match scenarios
// due to the architecture design, the attributes of the destination point and the edge are
// not stored together, so the last step in the match statement needs to call this operator
// to obtain the attribute of the destination point
namespace nebula {
namespace graph {

struct AppendVerticesRoundContext {
  // dsts_ and result_ are used for handling the response by multi jobs
  // DstId -> Vertex
  folly::ConcurrentHashMap<Value, Value> dsts_;
  DataSet result_;
};

class AppendVerticesStreamExecutor final : public GetPropStreamExecutor {
 public:
  AppendVerticesStreamExecutor(const PlanNode *node, QueryContext *qctx)
      : GetPropStreamExecutor("AppendVerticesStreamExecutor", node, qctx) {}

  std::shared_ptr<RoundResult> executeOneRound(
    std::shared_ptr<DataSet> input, Offset offset) override;

 private:
  StatusOr<DataSet> buildRequestDataSet(const AppendVertices *gv, Result& input);

  folly::Future<Status> appendVertices(Result& input, AppendVerticesRoundContext& roundCtx);

  Status handleResp(storage::StorageRpcResponse<storage::cpp2::GetPropResponse> &&rpcResp,
    Result& input, AppendVerticesRoundContext& roundCtx);

  Status handleNullProp(const AppendVertices *av, Result& input,
    AppendVerticesRoundContext& roundCtx);

  folly::Future<Status> handleRespMultiJobs(
      storage::StorageRpcResponse<storage::cpp2::GetPropResponse> &&rpcResp,
      Result& input, AppendVerticesRoundContext& roundCtx);

  DataSet handleJob(size_t begin, size_t end, Iterator *iter,
    AppendVerticesRoundContext& roundCtx);

  DataSet buildVerticesResult(size_t begin, size_t end, Iterator *iter);

  void buildMap(size_t begin, size_t end, Iterator *iter, AppendVerticesRoundContext& roundCtx);
};

}  // namespace graph
}  // namespace nebula

#endif  // GRAPH_EXECUTOR_QUERY_APPENDVERTICESSTREAMEXECUTOR_H_
