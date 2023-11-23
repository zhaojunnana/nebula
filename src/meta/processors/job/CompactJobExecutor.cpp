/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "meta/processors/job/CompactJobExecutor.h"

#include "common/utils/Utils.h"

namespace nebula {
namespace meta {

CompactJobExecutor::CompactJobExecutor(GraphSpaceID space,
                                       JobID jobId,
                                       kvstore::KVStore* kvstore,
                                       AdminClient* adminClient,
                                       const std::vector<std::string>& paras)
    : SimpleConcurrentJobExecutor(space, jobId, kvstore, adminClient, paras) {}

folly::Future<Status> CompactJobExecutor::executeInternal(HostAddr&& address,
                                                          std::vector<PartitionID>&& parts) {
  folly::Promise<Status> pro;
  auto f = pro.getFuture();
  adminClient_
      ->addTask(cpp2::JobType::COMPACT,
                jobId_,
                taskId_++,
                space_,
                std::move(address),
                paras_,
                std::move(parts))
      .then([pro = std::move(pro)](auto&& t) mutable {
        CHECK(!t.hasException());
        auto status = std::move(t).value();
        if (status.ok()) {
          pro.setValue(Status::OK());
        } else {
          pro.setValue(status.status());
        }
      });
  return f;
}

nebula::cpp2::ErrorCode CompactJobExecutor::stop() {
  auto errOrTargetHost = getTargetHost(space_);
  if (!nebula::ok(errOrTargetHost)) {
    LOG(ERROR) << "Get target host failed";
    auto retCode = nebula::error(errOrTargetHost);
    if (retCode != nebula::cpp2::ErrorCode::E_LEADER_CHANGED) {
      retCode = nebula::cpp2::ErrorCode::E_NO_HOSTS;
    }
    return retCode;
  }

  auto& hosts = nebula::value(errOrTargetHost);
  std::vector<folly::Future<StatusOr<bool>>> futures;
  for (auto& host : hosts) {
    auto future = adminClient_->stopTask(host.first, jobId_, 0);
    futures.emplace_back(std::move(future));
  }

  auto tries = folly::collectAll(std::move(futures)).get();
  if (std::any_of(tries.begin(), tries.end(), [](auto& t) { return t.hasException(); })) {
    LOG(ERROR) << "CompactJobExecutor::stop() RPC failure.";
    return nebula::cpp2::ErrorCode::E_BALANCER_FAILURE;
  }
  for (const auto& t : tries) {
    if (!t.value().ok()) {
      LOG(ERROR) << "Stop Build Index Failed";
      return nebula::cpp2::ErrorCode::E_BALANCER_FAILURE;
    }
  }
  return nebula::cpp2::ErrorCode::SUCCEEDED;
}

nebula::cpp2::ErrorCode CompactJobExecutor::check() {
  return paras_.empty() || paras_.size() == 1 ? nebula::cpp2::ErrorCode::SUCCEEDED
                                              : nebula::cpp2::ErrorCode::E_INVALID_JOB;
}

}  // namespace meta
}  // namespace nebula
