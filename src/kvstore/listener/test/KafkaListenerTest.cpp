/* Copyright (c) 2024 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include <gtest/gtest.h>
#include <thrift/lib/cpp/concurrency/ThreadManager.h>

#include "codec/RowReaderWrapper.h"
#include "codec/RowWriterV2.h"
#include "common/base/Base.h"
#include "common/fs/TempDir.h"
#include "common/meta/Common.h"
#include "common/network/NetworkUtils.h"
#include "common/utils/NebulaKeyUtils.h"
#include "kvstore/LogEncoder.h"
#include "kvstore/NebulaStore.h"
#include "kvstore/PartManager.h"
#include "kvstore/listener/kafka/KafkaAdapter.h"
#include "kvstore/listener/kafka/KafkaListener.h"
#include "meta/ActiveHostsMan.h"
#include "mock/AdHocSchemaManager.h"

DECLARE_uint32(raft_heartbeat_interval_secs);

using nebula::meta::ListenerHosts;
using nebula::meta::PartHosts;

namespace nebula {
namespace kvstore {

class SharedMockKafkaAdapter : public KafkaAdapter {
 public:
  explicit SharedMockKafkaAdapter(std::shared_ptr<std::vector<KafkaMessage>> store)
      : KafkaAdapter(KafkaClientConfig{}, "mock_topic", 1), store_(std::move(store)) {}

  Status send(const KafkaMessage& message) override {
    store_->emplace_back(message);
    return Status::OK();
  }

  Status sendBatch(const std::vector<KafkaMessage>& messages) override {
    for (const auto& msg : messages) {
      store_->emplace_back(msg);
    }
    return Status::OK();
  }

  ~SharedMockKafkaAdapter() override = default;

 private:
  std::shared_ptr<std::vector<KafkaMessage>> store_;
};

class TestableKafkaListener : public KafkaListener {
 public:
  TestableKafkaListener(GraphSpaceID spaceId,
                        PartitionID partId,
                        HostAddr localAddr,
                        const std::string& walPath,
                        std::shared_ptr<folly::IOThreadPoolExecutor> ioPool,
                        std::shared_ptr<thread::GenericThreadPool> workers,
                        std::shared_ptr<folly::Executor> handlers,
                        meta::SchemaManager* schemaMan)
      : KafkaListener(spaceId, partId, localAddr, walPath, ioPool, workers, handlers, schemaMan),
        messageStore_(std::make_shared<std::vector<KafkaMessage>>()) {}

  const std::vector<KafkaMessage>& getMessages() const {
    return *messageStore_;
  }

  void clearMessages() {
    messageStore_->clear();
  }

  std::tuple<nebula::cpp2::ErrorCode, int64_t, int64_t> commitSnapshotForTest(
      const std::vector<std::string>& data,
      LogID committedLogId,
      TermID committedLogTerm,
      bool finished) {
    std::lock_guard<std::mutex> guard(raftLock_);
    return commitSnapshot(data, committedLogId, committedLogTerm, finished);
  }

 protected:
  StatusOr<KafkaAdapter*> getKafkaAdapter() override {
    if (!kafkaAdapter_) {
      kafkaAdapter_ = std::make_unique<SharedMockKafkaAdapter>(messageStore_);
    }
    return kafkaAdapter_.get();
  }

 private:
  std::shared_ptr<std::vector<KafkaMessage>> messageStore_;
};

class KafkaListenerTest : public ::testing::TestWithParam<std::tuple<int32_t, int32_t, int32_t>> {
 public:
  void SetUp() override {
    auto param = GetParam();
    partCount_ = std::get<0>(param);
    replicas_ = std::get<1>(param);
    listenerCount_ = std::get<2>(param);

    schemaMan_ = std::make_unique<mock::AdHocSchemaManager>();
    setupSchema();

    rootPath_ = std::make_unique<fs::TempDir>("/tmp/kafka_listener_test.XXXXXX");
    getAvailablePort();
    initDataReplica();
    initListenerReplica();
    startKafkaListener();

    LOG(INFO) << "Waiting for all leaders elected!";
    waitLeader();
  }

  void TearDown() override {
    for (const auto& store : stores_) {
      store->stop();
    }
    for (const auto& listener : listeners_) {
      listener->stop();
    }
  }

 protected:
  void setupSchema() {
    auto tagSchema = std::make_shared<meta::NebulaSchemaProvider>(0);
    tagSchema->addField("name", nebula::cpp2::PropertyType::STRING);
    tagSchema->addField("age", nebula::cpp2::PropertyType::INT64);
    schemaMan_->addTagSchema(spaceId_, tagId_, tagSchema);

    auto edgeSchema = std::make_shared<meta::NebulaSchemaProvider>(0);
    edgeSchema->addField("start_year", nebula::cpp2::PropertyType::INT64);
    edgeSchema->addField("end_year", nebula::cpp2::PropertyType::INT64);
    schemaMan_->addEdgeSchema(spaceId_, edgeType_, edgeSchema);
  }

  void getAvailablePort() {
    std::string ip("127.0.0.1");
    for (int32_t i = 0; i < replicas_; i++) {
      peers_.emplace_back(ip, network::NetworkUtils::getAvailablePort());
    }
    for (int32_t i = 0; i < listenerCount_; i++) {
      listenerHosts_.emplace_back(ip, network::NetworkUtils::getAvailablePort());
    }
  }

  void initDataReplica() {
    LOG(INFO) << "Init data replica";
    for (int32_t i = 0; i < replicas_; i++) {
      stores_.emplace_back(initStore(i, listenerHosts_));
      stores_.back()->init();
    }
  }

  void initListenerReplica() {
    LOG(INFO) << "Init listener replica";
    for (int32_t i = 0; i < listenerCount_; i++) {
      listeners_.emplace_back(initListener(i));
      listeners_.back()->init();
      listeners_.back()->spaceListeners_.emplace(spaceId_, std::make_shared<SpaceListenerInfo>());
    }
  }

  std::unique_ptr<NebulaStore> initStore(int32_t index,
                                         const std::vector<HostAddr>& listeners = {}) {
    auto partMan = std::make_unique<MemPartManager>();
    auto ioThreadPool = std::make_shared<folly::IOThreadPoolExecutor>(4);

    for (int32_t partId = 1; partId <= partCount_; partId++) {
      PartHosts ph;
      ph.spaceId_ = spaceId_;
      ph.partId_ = partId;
      ph.hosts_ = peers_;
      partMan->partsMap_[spaceId_][partId] = std::move(ph);
      if (!listeners.empty()) {
        partMan->remoteListeners_[spaceId_][partId].emplace_back(
            listeners[partId % listeners.size()], meta::cpp2::ListenerType::UNKNOWN);
      }
    }

    std::vector<std::string> paths;
    paths.emplace_back(folly::stringPrintf("%s/disk%d", rootPath_->path(), index));

    KVOptions options;
    options.dataPaths_ = std::move(paths);
    options.partMan_ = std::move(partMan);
    HostAddr local = peers_[index];
    return std::make_unique<NebulaStore>(std::move(options), ioThreadPool, local, getWorkers());
  }

  std::unique_ptr<NebulaStore> initListener(int32_t index) {
    auto partMan = std::make_unique<MemPartManager>();
    auto ioThreadPool = std::make_shared<folly::IOThreadPoolExecutor>(4);

    KVOptions options;
    options.listenerPath_ = folly::stringPrintf("%s/listener%d", rootPath_->path(), index);
    options.partMan_ = std::move(partMan);
    HostAddr local = listenerHosts_[index];
    return std::make_unique<NebulaStore>(std::move(options), ioThreadPool, local, getWorkers());
  }

  void startKafkaListener() {
    for (int32_t partId = 1; partId <= partCount_; partId++) {
      auto index = partId % listenerHosts_.size();
      auto walPath = folly::stringPrintf(
          "%s/listener%lu/%d/%d/wal", rootPath_->path(), index, spaceId_, partId);
      auto local = NebulaStore::getRaftAddr(listenerHosts_[index]);
      auto kafka = std::make_shared<TestableKafkaListener>(spaceId_,
                                                           partId,
                                                           local,
                                                           walPath,
                                                           listeners_[index]->ioPool_,
                                                           listeners_[index]->bgWorkers_,
                                                           listeners_[index]->workers_,
                                                           schemaMan_.get());
      listeners_[index]->raftService_->addPartition(kafka);
      std::vector<HostAddr> raftPeers;
      std::transform(
          peers_.begin(), peers_.end(), std::back_inserter(raftPeers), [](const auto& host) {
            return NebulaStore::getRaftAddr(host);
          });
      kafka->start(std::move(raftPeers));
      listeners_[index]->spaceListeners_[spaceId_]->listeners_[partId].emplace(
          meta::cpp2::ListenerType::UNKNOWN, kafka);
      kafkaListeners_.emplace(partId, kafka);
    }
  }

  std::shared_ptr<apache::thrift::concurrency::PriorityThreadManager> getWorkers() {
    auto worker = apache::thrift::concurrency::PriorityThreadManager::newPriorityThreadManager(1);
    worker->setNamePrefix("executor");
    worker->start();
    return worker;
  }

  void waitLeader() {
    while (true) {
      int32_t leaderCount = 0;
      for (int i = 0; i < replicas_; i++) {
        nebula::meta::ActiveHostsMan::AllLeaders leaderIds;
        leaderCount += stores_[i]->allLeader(leaderIds);
      }
      if (leaderCount == partCount_) {
        break;
      }
      usleep(100000);
    }
  }

  HostAddr findLeader(PartitionID partId) {
    while (true) {
      auto leaderRet = stores_[0]->partLeader(spaceId_, partId);
      CHECK(ok(leaderRet));
      auto leader = value(std::move(leaderRet));
      if (leader == HostAddr("", 0)) {
        sleep(1);
        continue;
      }
      return leader;
    }
  }

  size_t findStoreIndex(const HostAddr& addr) {
    for (size_t i = 0; i < peers_.size(); i++) {
      if (peers_[i] == addr) {
        return i;
      }
    }
    LOG(FATAL) << "Should not reach here!";
    return 0;
  }

  std::string encodeTagValue(const std::string& name, int64_t age) {
    auto schema = schemaMan_->getTagSchema(spaceId_, tagId_);
    RowWriterV2 writer(schema.get());
    writer.set("name", name);
    writer.set("age", age);
    writer.finish();
    return writer.moveEncodedStr();
  }

  std::string encodeEdgeValue(int64_t startYear, int64_t endYear) {
    auto schema = schemaMan_->getEdgeSchema(spaceId_, edgeType_);
    RowWriterV2 writer(schema.get());
    writer.set("start_year", startYear);
    writer.set("end_year", endYear);
    writer.finish();
    return writer.moveEncodedStr();
  }

 protected:
  int32_t partCount_;
  int32_t replicas_;
  int32_t listenerCount_;

  static constexpr GraphSpaceID spaceId_ = 1;
  static constexpr TagID tagId_ = 3;
  static constexpr EdgeType edgeType_ = 5;
  static constexpr int32_t vIdLen_ = 32;

  std::unique_ptr<fs::TempDir> rootPath_;
  std::unique_ptr<mock::AdHocSchemaManager> schemaMan_;
  std::vector<HostAddr> peers_;
  std::vector<HostAddr> listenerHosts_;
  std::vector<std::unique_ptr<NebulaStore>> stores_;
  std::vector<std::unique_ptr<NebulaStore>> listeners_;
  std::unordered_map<PartitionID, std::shared_ptr<TestableKafkaListener>> kafkaListeners_;
};

TEST_P(KafkaListenerTest, VertexPutTest) {
  LOG(INFO) << "Insert vertex data";
  for (int32_t partId = 1; partId <= partCount_; partId++) {
    std::vector<KV> data;
    for (int32_t i = 0; i < 10; i++) {
      auto vid = folly::stringPrintf("player_%d_%d", partId, i);
      auto key = NebulaKeyUtils::tagKey(vIdLen_, partId, vid, tagId_);
      auto val = encodeTagValue(folly::stringPrintf("name_%d", i), 20 + i);
      data.emplace_back(std::move(key), std::move(val));
    }
    auto leader = findLeader(partId);
    auto index = findStoreIndex(leader);
    folly::Baton<true, std::atomic> baton;
    stores_[index]->asyncMultiPut(
        spaceId_, partId, std::move(data), [&baton](cpp2::ErrorCode code) {
          EXPECT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
          baton.post();
        });
    baton.wait();
  }

  sleep(FLAGS_raft_heartbeat_interval_secs + 1);

  LOG(INFO) << "Verify Kafka messages for vertex PUT";
  for (int32_t partId = 1; partId <= partCount_; partId++) {
    auto kafka = kafkaListeners_[partId];
    const auto& messages = kafka->getMessages();
    ASSERT_EQ(10, messages.size()) << "partId=" << partId;

    auto parsed = folly::parseJson(messages[0].value);
    EXPECT_EQ("vertex", parsed["type"].getString());
    EXPECT_EQ("PUT", parsed["operation"].getString());
    EXPECT_EQ(tagId_, parsed["tagId"].getInt());
    EXPECT_TRUE(parsed.count("properties"));
    EXPECT_TRUE(parsed["properties"]["name"].isString());
    EXPECT_TRUE(parsed["properties"]["age"].isInt());
    EXPECT_TRUE(parsed.count("logId"));
    EXPECT_TRUE(parsed.count("timestamp"));
    EXPECT_TRUE(parsed.count("seq"));
  }
}

TEST_P(KafkaListenerTest, EdgePutTest) {
  LOG(INFO) << "Insert edge data (forward + reverse)";
  for (int32_t partId = 1; partId <= partCount_; partId++) {
    std::vector<KV> data;
    for (int32_t i = 0; i < 10; i++) {
      auto src = folly::stringPrintf("player_%d_%d", partId, i);
      auto dst = folly::stringPrintf("team_%d_%d", partId, i);
      auto key = NebulaKeyUtils::edgeKey(vIdLen_, partId, src, edgeType_, 2020 + i, dst);
      auto val = encodeEdgeValue(2016, 2020 + i);
      data.emplace_back(std::move(key), std::move(val));
      auto revKey = NebulaKeyUtils::edgeKey(vIdLen_, partId, dst, -edgeType_, 2020 + i, src);
      data.emplace_back(std::move(revKey), encodeEdgeValue(2016, 2020 + i));
    }
    auto leader = findLeader(partId);
    auto index = findStoreIndex(leader);
    folly::Baton<true, std::atomic> baton;
    stores_[index]->asyncMultiPut(
        spaceId_, partId, std::move(data), [&baton](cpp2::ErrorCode code) {
          EXPECT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
          baton.post();
        });
    baton.wait();
  }

  sleep(FLAGS_raft_heartbeat_interval_secs + 1);

  LOG(INFO) << "Verify only forward edges produce Kafka messages";
  for (int32_t partId = 1; partId <= partCount_; partId++) {
    auto kafka = kafkaListeners_[partId];
    const auto& messages = kafka->getMessages();
    ASSERT_EQ(10, messages.size()) << "partId=" << partId;

    auto parsed = folly::parseJson(messages[0].value);
    EXPECT_EQ("edge", parsed["type"].getString());
    EXPECT_EQ("PUT", parsed["operation"].getString());
    EXPECT_EQ(edgeType_, parsed["edgeType"].getInt());
    EXPECT_TRUE(parsed.count("properties"));
    EXPECT_TRUE(parsed.count("logId"));
    EXPECT_TRUE(parsed.count("timestamp"));
  }
}

TEST_P(KafkaListenerTest, VertexAndEdgeMixedTest) {
  LOG(INFO) << "Insert mixed vertex and edge data";
  for (int32_t partId = 1; partId <= partCount_; partId++) {
    std::vector<KV> data;
    for (int32_t i = 0; i < 5; i++) {
      auto vid = folly::stringPrintf("player_%d_%d", partId, i);
      auto tagKey = NebulaKeyUtils::tagKey(vIdLen_, partId, vid, tagId_);
      data.emplace_back(std::move(tagKey), encodeTagValue(folly::stringPrintf("name_%d", i), i));

      auto dst = folly::stringPrintf("team_%d_%d", partId, i);
      auto edgeKey = NebulaKeyUtils::edgeKey(vIdLen_, partId, vid, edgeType_, i, dst);
      data.emplace_back(std::move(edgeKey), encodeEdgeValue(2000 + i, 2010 + i));
    }
    auto leader = findLeader(partId);
    auto index = findStoreIndex(leader);
    folly::Baton<true, std::atomic> baton;
    stores_[index]->asyncMultiPut(
        spaceId_, partId, std::move(data), [&baton](cpp2::ErrorCode code) {
          EXPECT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
          baton.post();
        });
    baton.wait();
  }

  sleep(FLAGS_raft_heartbeat_interval_secs + 1);

  LOG(INFO) << "Verify both vertex and edge messages are produced";
  for (int32_t partId = 1; partId <= partCount_; partId++) {
    auto kafka = kafkaListeners_[partId];
    const auto& messages = kafka->getMessages();
    ASSERT_EQ(10, messages.size()) << "partId=" << partId;

    int vertexCount = 0;
    int edgeCount = 0;
    for (const auto& msg : messages) {
      auto parsed = folly::parseJson(msg.value);
      if (parsed["type"].getString() == "vertex") {
        vertexCount++;
      } else if (parsed["type"].getString() == "edge") {
        edgeCount++;
      }
    }
    EXPECT_EQ(5, vertexCount);
    EXPECT_EQ(5, edgeCount);
  }
}

TEST_P(KafkaListenerTest, CommitSnapshotTest) {
  LOG(INFO) << "Commit snapshot with vertex data";
  for (int32_t partId = 1; partId <= partCount_; partId++) {
    std::vector<std::string> rows;
    int64_t totalSize = 0;
    for (int32_t i = 0; i < 10; i++) {
      auto vid = folly::stringPrintf("player_%d_%d", partId, i);
      auto key = NebulaKeyUtils::tagKey(vIdLen_, partId, vid, tagId_);
      auto val = encodeTagValue(folly::stringPrintf("name_%d", i), 30 + i);
      auto kvStr = encodeKV(key, val);
      totalSize += kvStr.size();
      rows.emplace_back(std::move(kvStr));
    }

    auto kafka = kafkaListeners_[partId];
    auto ret = kafka->commitSnapshotForTest(rows, 100, 1, true);
    EXPECT_EQ(std::get<0>(ret), nebula::cpp2::ErrorCode::SUCCEEDED);
    EXPECT_EQ(std::get<1>(ret), 10);
    EXPECT_EQ(std::get<2>(ret), totalSize);
  }

  LOG(INFO) << "Verify snapshot data produced Kafka messages";
  for (int32_t partId = 1; partId <= partCount_; partId++) {
    auto kafka = kafkaListeners_[partId];
    const auto& messages = kafka->getMessages();
    ASSERT_EQ(10, messages.size()) << "partId=" << partId;

    for (const auto& msg : messages) {
      auto parsed = folly::parseJson(msg.value);
      EXPECT_EQ("vertex", parsed["type"].getString());
      EXPECT_EQ("PUT", parsed["operation"].getString());
      EXPECT_TRUE(parsed.count("logId"));
      EXPECT_TRUE(parsed.count("timestamp"));
      EXPECT_TRUE(parsed.count("seq"));
    }
  }
}

TEST_P(KafkaListenerTest, JsonPayloadFieldsTest) {
  LOG(INFO) << "Insert one vertex and one edge, verify detailed JSON fields";
  for (int32_t partId = 1; partId <= partCount_; partId++) {
    std::vector<KV> data;
    auto vid = "player_json_test";
    auto tagKey = NebulaKeyUtils::tagKey(vIdLen_, partId, vid, tagId_);
    data.emplace_back(std::move(tagKey), encodeTagValue("Tim Duncan", 42));

    std::string dst = "team_spurs";
    auto edgeKey = NebulaKeyUtils::edgeKey(vIdLen_, partId, vid, edgeType_, 2019, dst);
    data.emplace_back(std::move(edgeKey), encodeEdgeValue(1997, 2016));

    auto leader = findLeader(partId);
    auto index = findStoreIndex(leader);
    folly::Baton<true, std::atomic> baton;
    stores_[index]->asyncMultiPut(
        spaceId_, partId, std::move(data), [&baton](cpp2::ErrorCode code) {
          EXPECT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
          baton.post();
        });
    baton.wait();
  }

  sleep(FLAGS_raft_heartbeat_interval_secs + 1);

  for (int32_t partId = 1; partId <= partCount_; partId++) {
    auto kafka = kafkaListeners_[partId];
    const auto& messages = kafka->getMessages();
    ASSERT_EQ(2, messages.size()) << "partId=" << partId;

    auto vertexMsg = folly::parseJson(messages[0].value);
    EXPECT_EQ("vertex", vertexMsg["type"].getString());
    EXPECT_EQ(spaceId_, vertexMsg["spaceId"].getInt());
    EXPECT_EQ(partId, vertexMsg["partId"].getInt());
    EXPECT_EQ("Tim Duncan", vertexMsg["properties"]["name"].getString());
    EXPECT_EQ(42, vertexMsg["properties"]["age"].getInt());
    EXPECT_EQ("UPSERT_VERTEX", vertexMsg["graphOperation"].getString());

    auto edgeMsg = folly::parseJson(messages[1].value);
    EXPECT_EQ("edge", edgeMsg["type"].getString());
    EXPECT_EQ(edgeType_, edgeMsg["edgeType"].getInt());
    EXPECT_EQ(2019, edgeMsg["ranking"].getInt());
    EXPECT_EQ(1997, edgeMsg["properties"]["start_year"].getInt());
    EXPECT_EQ(2016, edgeMsg["properties"]["end_year"].getInt());
    EXPECT_EQ("UPSERT_EDGE", edgeMsg["graphOperation"].getString());
  }
}

TEST_P(KafkaListenerTest, TransLeaderTest) {
  LOG(INFO) << "Insert some data";
  for (int32_t partId = 1; partId <= partCount_; partId++) {
    std::vector<KV> data;
    for (int32_t i = 0; i < 10; i++) {
      auto vid = folly::stringPrintf("player_%d_%d", partId, i);
      auto key = NebulaKeyUtils::tagKey(vIdLen_, partId, vid, tagId_);
      data.emplace_back(std::move(key), encodeTagValue(folly::stringPrintf("name_%d", i), i));
    }
    auto leader = findLeader(partId);
    auto index = findStoreIndex(leader);
    folly::Baton<true, std::atomic> baton;
    stores_[index]->asyncMultiPut(
        spaceId_, partId, std::move(data), [&baton](cpp2::ErrorCode code) {
          EXPECT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
          baton.post();
        });
    baton.wait();
  }

  LOG(INFO) << "Transfer all part leader to first replica";
  auto targetAddr = NebulaStore::getRaftAddr(peers_[0]);
  for (int32_t partId = 1; partId <= partCount_; partId++) {
    folly::Baton<true, std::atomic> baton;
    auto leader = findLeader(partId);
    auto index = findStoreIndex(leader);
    auto partRet = stores_[index]->part(spaceId_, partId);
    CHECK(ok(partRet));
    auto part = value(partRet);
    part->asyncTransferLeader(targetAddr, [&](cpp2::ErrorCode) { baton.post(); });
    baton.wait();
  }
  sleep(FLAGS_raft_heartbeat_interval_secs);
  {
    nebula::meta::ActiveHostsMan::AllLeaders leaderIds;
    ASSERT_EQ(partCount_, stores_[0]->allLeader(leaderIds));
  }

  LOG(INFO) << "Insert more data after leader transfer";
  for (int32_t partId = 1; partId <= partCount_; partId++) {
    std::vector<KV> data;
    for (int32_t i = 10; i < 20; i++) {
      auto vid = folly::stringPrintf("player_%d_%d", partId, i);
      auto key = NebulaKeyUtils::tagKey(vIdLen_, partId, vid, tagId_);
      data.emplace_back(std::move(key), encodeTagValue(folly::stringPrintf("name_%d", i), i));
    }
    auto leader = findLeader(partId);
    auto index = findStoreIndex(leader);
    folly::Baton<true, std::atomic> baton;
    stores_[index]->asyncMultiPut(
        spaceId_, partId, std::move(data), [&baton](cpp2::ErrorCode code) {
          EXPECT_EQ(cpp2::ErrorCode::SUCCEEDED, code);
          baton.post();
        });
    baton.wait();
  }

  sleep(FLAGS_raft_heartbeat_interval_secs);

  LOG(INFO) << "Verify all data received after leader transfer";
  for (int32_t partId = 1; partId <= partCount_; partId++) {
    auto kafka = kafkaListeners_[partId];
    const auto& messages = kafka->getMessages();
    EXPECT_EQ(20, messages.size()) << "partId=" << partId;
  }
}

INSTANTIATE_TEST_SUITE_P(PartCount_Replicas_ListenerCount,
                         KafkaListenerTest,
                         ::testing::Values(std::make_tuple(1, 1, 1)));

}  // namespace kvstore
}  // namespace nebula

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, true);
  google::SetStderrLogging(google::INFO);
  return RUN_ALL_TESTS();
}
