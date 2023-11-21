// Copyright (c) 2021 vesoft inc. All rights reserved.
//
// This source code is licensed under Apache 2.0 License.

#include "graph/executor/stream/AppendVerticesStreamExecutor.h"

#include <iterator>
#include "common/base/Status.h"

using nebula::storage::StorageClient;
using nebula::storage::StorageRpcResponse;
using nebula::storage::cpp2::GetPropResponse;

DECLARE_bool(optimize_appendvertices);

namespace nebula {
namespace graph {

std::shared_ptr<RoundResult> AppendVerticesStreamExecutor::executeOneRound(
  std::shared_ptr<DataSet> input, Offset offset) {
  auto inputResult = ResultBuilder().value(Value(*input))
    .iter(Iterator::Kind::kProp).build();
  AppendVerticesRoundContext roundCtx;
  auto resultStatus = appendVertices(inputResult, roundCtx).get();
  if (resultStatus.ok()) {
    auto roundResult = std::make_shared<RoundResult>(
        std::make_shared<DataSet>(std::move(roundCtx.result_)),
        false,
        offset);
    return roundResult;
  } else {
    *abnormalStatus_ = std::move(resultStatus);
    return std::make_shared<RoundResult>(nullptr, false, offset);
  }
}

StatusOr<DataSet> AppendVerticesStreamExecutor::buildRequestDataSet(
  const AppendVertices *av, Result& input) {
  if (av == nullptr) {
    return nebula::DataSet({kVid});
  }
  // auto valueIter = ectx_->getResult(av->inputVar()).iter();
  auto valueIter = input.iter();
  return buildRequestDataSetByVidType(valueIter.get(), av->src()->clone(), av->dedup(), true);
}

folly::Future<Status> AppendVerticesStreamExecutor::appendVertices(
    Result& input, AppendVerticesRoundContext& roundCtx) {
  SCOPED_TIMER(&execTime_);
  auto *av = asNode<AppendVertices>(node());
  if (FLAGS_optimize_appendvertices && av != nullptr && av->noNeedFetchProp()) {
    return handleNullProp(av, input, roundCtx);
  }

  StorageClient *storageClient = qctx()->getStorageClient();
  auto res = buildRequestDataSet(av, input);
  NG_RETURN_IF_ERROR(res);
  auto vertices = std::move(res).value();
  if (vertices.rows.empty()) {
    roundCtx.result_ = DataSet(av->colNames());
    return Status::OK();
  }

  StorageClient::CommonRequestParam param(av->space(),
                                          qctx()->rctx()->session()->id(),
                                          qctx()->plan()->id(),
                                          qctx()->plan()->isProfileEnabled());

  time::Duration getPropsTime;
  return DCHECK_NOTNULL(storageClient)
      ->getProps(param,
                 std::move(vertices),
                 av->props(),
                 nullptr,
                 av->exprs(),
                 av->dedup(),
                 av->orderBy(),
                 av->limit(qctx()),
                 av->filter())
      .via(runner())
      .thenValue([this, getPropsTime, &input, &roundCtx](
        StorageRpcResponse<GetPropResponse> &&rpcResp) {
        // MemoryTrackerVerified
        memory::MemoryCheckGuard guard;
        SCOPED_TIMER(&execTime_);
        addState("total_rpc", getPropsTime);
        addStats(rpcResp);
        if (FLAGS_max_job_size <= 1) {
          return folly::makeFuture<Status>(handleResp(std::move(rpcResp), input, roundCtx));
        } else {
          return handleRespMultiJobs(std::move(rpcResp), input, roundCtx);
        }
      });
}

Status AppendVerticesStreamExecutor::handleNullProp(const AppendVertices *av, Result& input,
  AppendVerticesRoundContext& roundCtx) {
  // auto iter = ectx_->getResult(av->inputVar()).iter();
  auto iter = input.iter();
  auto *src = av->src()->clone();

  auto size = iter->size();
  DataSet ds;
  ds.colNames = av->colNames();
  ds.rows.reserve(size);

  QueryExpressionContext ctx(ectx_);
  bool canBeMoved = movable(av->inputVars().front());

  for (; iter->valid(); iter->next()) {
    const auto &vid = src->eval(ctx(iter.get()));
    if (vid.empty()) {
      continue;
    }
    Vertex vertex;
    vertex.vid = vid;
    if (!av->trackPrevPath()) {
      Row row;
      row.values.emplace_back(std::move(vertex));
      ds.rows.emplace_back(std::move(row));
    } else {
      Row row;
      row = canBeMoved ? iter->moveRow() : *iter->row();
      row.values.emplace_back(std::move(vertex));
      ds.rows.emplace_back(std::move(row));
    }
  }
  roundCtx.result_ = std::move(ds);
  return Status::OK();
}

Status AppendVerticesStreamExecutor::handleResp(
    storage::StorageRpcResponse<storage::cpp2::GetPropResponse> &&rpcResp,
    Result& input, AppendVerticesRoundContext& roundCtx) {
  auto result = handleCompleteness(rpcResp, FLAGS_accept_partial_success);
  NG_RETURN_IF_ERROR(result);
  // auto state = std::move(result).value();
  std::unordered_map<Value, Value> map;
  auto *av = asNode<AppendVertices>(node());
  auto *vFilter = av->vFilter() ? av->vFilter()->clone() : nullptr;
  QueryExpressionContext ctx(qctx()->ectx());

  // auto inputIter = qctx()->ectx()->getResult(av->inputVar()).iter();
  auto inputIter = input.iter();
  DataSet ds;
  ds.colNames = av->colNames();
  ds.rows.reserve(inputIter->size());

  for (auto &resp : rpcResp.responses()) {
    if (resp.props_ref().has_value()) {
      auto iter = PropIter(std::make_shared<Value>(std::move(*resp.props_ref())));
      for (; iter.valid(); iter.next()) {
        if (vFilter != nullptr) {
          auto &vFilterVal = vFilter->eval(ctx(&iter));
          if (!vFilterVal.isBool() || !vFilterVal.getBool()) {
            continue;
          }
        }
        if (!av->trackPrevPath()) {  // eg. MATCH (v:Person) RETURN v
          Row row;
          row.values.emplace_back(iter.getVertex());
          ds.rows.emplace_back(std::move(row));
        } else {
          map.emplace(iter.getColumn(kVid), iter.getVertex());
        }
      }
    }
  }

  if (!av->trackPrevPath()) {
    roundCtx.result_ = std::move(ds);
    return Status::OK();
  }

  auto *src = av->src()->clone();
  bool mv = movable(av->inputVars().front());
  for (; inputIter->valid(); inputIter->next()) {
    auto dstFound = map.find(src->eval(ctx(inputIter.get())));
    if (dstFound != map.end()) {
      Row row = mv ? inputIter->moveRow() : *inputIter->row();
      row.values.emplace_back(dstFound->second);
      ds.rows.emplace_back(std::move(row));
    }
  }
  roundCtx.result_ = std::move(ds);
  return Status::OK();
}

folly::Future<Status> AppendVerticesStreamExecutor::handleRespMultiJobs(
    storage::StorageRpcResponse<storage::cpp2::GetPropResponse> &&rpcResp,
    Result& input, AppendVerticesRoundContext& roundCtx) {
  auto result = handleCompleteness(rpcResp, FLAGS_accept_partial_success);
  NG_RETURN_IF_ERROR(result);
  auto *av = asNode<AppendVertices>(node());

  // auto inputIter = qctx()->ectx()->getResult(av->inputVar()).iter();
  auto inputIter = input.iter();
  roundCtx.result_.colNames = av->colNames();
  roundCtx.result_.rows.reserve(inputIter->size());

  nebula::DataSet v;
  for (auto &resp : rpcResp.responses()) {
    if (resp.props_ref().has_value()) {
      auto &&respV = std::move(*resp.props_ref());
      v.colNames = respV.colNames;
      std::move(respV.begin(), respV.end(), std::back_inserter(v.rows));
    }
  }
  auto propIter = PropIter(std::make_shared<Value>(std::move(v)));

  if (!av->trackPrevPath()) {
    auto scatter = [this](
                       size_t begin, size_t end, Iterator *tmpIter) mutable -> StatusOr<DataSet> {
      return buildVerticesResult(begin, end, tmpIter);
    };

    auto gather = [&roundCtx](
      std::vector<folly::Try<StatusOr<DataSet>>> &&results) -> Status {
      memory::MemoryCheckGuard guard;
      for (auto &respVal : results) {
        if (respVal.hasException()) {
          auto ex = respVal.exception().get_exception<std::bad_alloc>();
          if (ex) {
            throw std::bad_alloc();
          } else {
            throw std::runtime_error(respVal.exception().what().c_str());
          }
        }
        auto res = std::move(respVal).value();
        auto &&rows = std::move(res).value();
        std::move(rows.begin(), rows.end(), std::back_inserter(roundCtx.result_.rows));
      }
      return Status::OK();
    };

    return runMultiJobs(std::move(scatter), std::move(gather), &propIter);
  } else {
    auto scatter = [this, &roundCtx](
      size_t begin, size_t end, Iterator *tmpIter) mutable -> folly::Unit {
      buildMap(begin, end, tmpIter, roundCtx);
      return folly::unit;
    };

    auto gather =
        [this, inputIterNew = std::move(inputIter), &roundCtx](
            std::vector<folly::Try<folly::Unit>> &&prepareResult) -> folly::Future<Status> {
      memory::MemoryCheckGuard guard1;
      for (auto &respVal : prepareResult) {
        if (respVal.hasException()) {
          auto ex = respVal.exception().get_exception<std::bad_alloc>();
          if (ex) {
            throw std::bad_alloc();
          } else {
            throw std::runtime_error(respVal.exception().what().c_str());
          }
        }
      }

      auto scatterInput =[this, &roundCtx](
        size_t begin, size_t end, Iterator *tmpIter) mutable -> StatusOr<DataSet> {
        return handleJob(begin, end, tmpIter, roundCtx);
      };

      auto gatherFinal = [&roundCtx](
        std::vector<folly::Try<StatusOr<DataSet>>> &&results) -> Status {
        memory::MemoryCheckGuard guard2;
        for (auto &respVal : results) {
          if (respVal.hasException()) {
            auto ex = respVal.exception().get_exception<std::bad_alloc>();
            if (ex) {
              throw std::bad_alloc();
            } else {
              throw std::runtime_error(respVal.exception().what().c_str());
            }
          }
          auto res = std::move(respVal).value();
          auto &&rows = std::move(res).value();
          std::move(rows.begin(), rows.end(), std::back_inserter(roundCtx.result_.rows));
        }
        return Status::OK();
      };

      return runMultiJobs(std::move(scatterInput), std::move(gatherFinal), inputIterNew.get());
    };

    return runMultiJobs(std::move(scatter), std::move(gather), &propIter);
  }
}

DataSet AppendVerticesStreamExecutor::buildVerticesResult(
  size_t begin, size_t end, Iterator *iter) {
  auto *av = asNode<AppendVertices>(node());
  auto vFilter = av->vFilter() ? av->vFilter()->clone() : nullptr;
  DataSet ds;
  ds.colNames = av->colNames();
  ds.rows.reserve(end - begin);
  QueryExpressionContext ctx(qctx()->ectx());
  for (; iter->valid() && begin++ < end; iter->next()) {
    if (vFilter != nullptr) {
      auto &vFilterVal = vFilter->eval(ctx(iter));
      if (!vFilterVal.isBool() || !vFilterVal.getBool()) {
        continue;
      }
    }
    Row row;
    row.values.emplace_back(iter->getVertex());
    ds.rows.emplace_back(std::move(row));
  }

  return ds;
}

void AppendVerticesStreamExecutor::buildMap(size_t begin, size_t end, Iterator *iter,
  AppendVerticesRoundContext& roundCtx) {
  auto *av = asNode<AppendVertices>(node());
  auto vFilter = av->vFilter() ? av->vFilter()->clone() : nullptr;
  QueryExpressionContext ctx(qctx()->ectx());
  for (; iter->valid() && begin++ < end; iter->next()) {
    if (vFilter != nullptr) {
      auto &vFilterVal = vFilter->eval(ctx(iter));
      if (!vFilterVal.isBool() || !vFilterVal.getBool()) {
        continue;
      }
    }
    roundCtx.dsts_.emplace(iter->getColumn(kVid), iter->getVertex());
  }
}

DataSet AppendVerticesStreamExecutor::handleJob(size_t begin, size_t end, Iterator *iter,
  AppendVerticesRoundContext& roundCtx) {
  auto *av = asNode<AppendVertices>(node());
  DataSet ds;
  ds.colNames = av->colNames();
  ds.rows.reserve(end - begin);
  auto src = av->src()->clone();
  QueryExpressionContext ctx(qctx()->ectx());
  for (; iter->valid() && begin++ < end; iter->next()) {
    auto dstFound = roundCtx.dsts_.find(src->eval(ctx(iter)));
    if (dstFound != roundCtx.dsts_.end()) {
      Row row = *iter->row();
      row.values.emplace_back(dstFound->second);
      ds.rows.emplace_back(std::move(row));
    }
  }

  return ds;
}

}  // namespace graph
}  // namespace nebula
