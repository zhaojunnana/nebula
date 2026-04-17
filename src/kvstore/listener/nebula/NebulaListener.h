/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef KVSTORE_LISTENER_NEBULA_LISTENER_H_
#define KVSTORE_LISTENER_NEBULA_LISTENER_H_

#include "clients/meta/MetaClient.h"
#include "clients/storage/StorageClient.h"
#include "codec/RowReaderWrapper.h"
#include "kvstore/listener/elasticsearch/ESListener.h"

namespace nebula {
namespace kvstore {

class NebulaListener : public ESListener {
 public:
  /**
   * @brief Construct a new NEBULA Listener, it is a derived class of Listener
   *
   * @param spaceId
   * @param partId
   * @param localAddr Listener ip/addr
   * @param walPath Listener's wal path
   * @param ioPool IOThreadPool for listener
   * @param workers Background thread for listener
   * @param handlers Worker thread for listener
   * @param schemaMan Schema manager
   */
  NebulaListener(GraphSpaceID spaceId,
                 PartitionID partId,
                 HostAddr localAddr,
                 const std::string& walPath,
                 std::shared_ptr<folly::IOThreadPoolExecutor> ioPool,
                 std::shared_ptr<thread::GenericThreadPool> workers,
                 std::shared_ptr<folly::Executor> handlers,
                 meta::SchemaManager* schemaMan)
      : ESListener(
            spaceId, partId, std::move(localAddr), walPath, ioPool, workers, handlers, schemaMan) {}

 protected:
  void init() override;

  bool applyBatch(const BatchHolder& batch);

  void processLogs() override;

  void updateWriteSpace();

  void resetListener() override;

  bool pursueLeaderDone() override;

  std::tuple<nebula::cpp2::ErrorCode, int64_t, int64_t> commitSnapshot(
      const std::vector<std::string>& data,
      LogID committedLogId,
      TermID committedLogTerm,
      bool finished) override;

 private:
  std::unique_ptr<meta::MetaClient> metaClient_;
  std::unique_ptr<storage::StorageClient> storage_;
  int writeSpaceId_;
};

}  // namespace kvstore
}  // namespace nebula
#endif
