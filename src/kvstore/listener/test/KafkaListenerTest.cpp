/* Copyright (c) 2024 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

 #include <folly/dynamic.h>
 #include <folly/json.h>
 #include <gtest/gtest.h>
 #include <thrift/lib/cpp/concurrency/ThreadManager.h>

 #include "codec/RowReaderWrapper.h"
 #include "codec/RowWriterV2.h"
 #include "common/base/Base.h"
 #include "common/fs/TempDir.h"
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

 namespace nebula {
 namespace kvstore {

namespace {

/**
 * @brief Convert a nebula Value to folly::dynamic with proper typing.
 * Mirrors the valueToDynamic() in KafkaListener.cpp for test consistency.
 */
folly::dynamic valueToDynamic(const Value& val) {
  switch (val.type()) {
    case Value::Type::INT:
      return val.getInt();
    case Value::Type::FLOAT:
      return val.getFloat();
    case Value::Type::BOOL:
      return val.getBool();
    case Value::Type::STRING:
      return val.getStr();
    case Value::Type::NULLVALUE:
    case Value::Type::__EMPTY__:
      return nullptr;
    case Value::Type::DATE:
      return val.getDate().toString();
    case Value::Type::TIME:
      return val.getTime().toString();
    case Value::Type::DATETIME:
      return val.getDateTime().toString();
    default:
      return val.toString();
  }
}

}  // anonymous namespace

 /**
  * @brief Mock KafkaAdapter that captures messages instead of sending to Kafka
  */
 class MockKafkaAdapter : public KafkaAdapter {
  public:
   MockKafkaAdapter() : KafkaAdapter(KafkaClientConfig{}) {}

   Status send(const KafkaMessage& message) override {
     messages_.emplace_back(message);
     return Status::OK();
   }

   Status sendBatch(const std::vector<KafkaMessage>& messages) override {
     for (const auto& msg : messages) {
       messages_.emplace_back(msg);
     }
     return Status::OK();
   }

   ~MockKafkaAdapter() override = default;

   const std::vector<KafkaMessage>& getMessages() const {
     return messages_;
   }

   void clear() {
     messages_.clear();
   }

  private:
   std::vector<KafkaMessage> messages_;
 };

 /**
  * @brief Test fixture for KafkaListener unit tests.
  *
  * Tests key parsing, row encoding/decoding, JSON payload construction,
  * and mock adapter behavior — the core logic exercised by apply().
  * Does not require full Raft infrastructure.
  */
 class KafkaListenerTest : public ::testing::Test {
  protected:
   void SetUp() override {
     mockAdapter_ = std::make_unique<MockKafkaAdapter>();
     schemaMan_ = std::make_unique<mock::AdHocSchemaManager>();
     setupSchema();
   }

   void TearDown() override {
     mockAdapter_.reset();
     schemaMan_.reset();
   }

   void setupSchema() {
     // tag schema: player(name string, age int64)
     auto tagSchema = std::make_shared<meta::NebulaSchemaProvider>(0);
     tagSchema->addField("name", nebula::cpp2::PropertyType::STRING);
     tagSchema->addField("age", nebula::cpp2::PropertyType::INT64);
     schemaMan_->addTagSchema(spaceId_, tagId_, tagSchema);

     // edge schema: serve(start_year int64, end_year int64)
     auto edgeSchema = std::make_shared<meta::NebulaSchemaProvider>(0);
     edgeSchema->addField("start_year", nebula::cpp2::PropertyType::INT64);
     edgeSchema->addField("end_year", nebula::cpp2::PropertyType::INT64);
     schemaMan_->addEdgeSchema(spaceId_, edgeType_, edgeSchema);
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

   // Trim trailing null bytes from vid (same logic as KafkaListener::normalizeVid for string vid)
   std::string normalizeStringVid(const std::string& vid) {
     return folly::rtrim(folly::StringPiece(vid), [](char c) { return c == '\0'; }).toString();
   }

   // AdHocSchemaManager returns vIdLen=32 by default
   static constexpr GraphSpaceID spaceId_ = 1;
   static constexpr PartitionID partId_ = 1;
   static constexpr TagID tagId_ = 3;
   static constexpr EdgeType edgeType_ = 5;
   static constexpr int32_t vIdLen_ = 32;

   std::unique_ptr<MockKafkaAdapter> mockAdapter_;
   std::unique_ptr<mock::AdHocSchemaManager> schemaMan_;
 };

 // ==================== Key Parsing Tests ====================

 TEST_F(KafkaListenerTest, VertexKeyParsing) {
   std::string vid = "player01";
   auto key = NebulaKeyUtils::tagKey(vIdLen_, partId_, vid, tagId_);

   ASSERT_TRUE(NebulaKeyUtils::isTag(vIdLen_, key));
   ASSERT_FALSE(NebulaKeyUtils::isEdge(vIdLen_, key));

   EXPECT_EQ(tagId_, NebulaKeyUtils::getTagId(vIdLen_, key));

   auto parsedVid = NebulaKeyUtils::getVertexId(vIdLen_, key);
   std::string expected = vid;
   expected.resize(vIdLen_, '\0');
   EXPECT_EQ(expected, parsedVid.toString());
 }

 TEST_F(KafkaListenerTest, EdgeKeyParsing) {
   std::string src = "player01";
   std::string dst = "team0001";
   int64_t rank = 2019;
   auto key = NebulaKeyUtils::edgeKey(vIdLen_, partId_, src, edgeType_, rank, dst);

   ASSERT_FALSE(NebulaKeyUtils::isTag(vIdLen_, key));
   ASSERT_TRUE(NebulaKeyUtils::isEdge(vIdLen_, key));

   EXPECT_EQ(edgeType_, NebulaKeyUtils::getEdgeType(vIdLen_, key));
   EXPECT_GT(NebulaKeyUtils::getEdgeType(vIdLen_, key), 0);  // positive = forward edge
   EXPECT_EQ(rank, NebulaKeyUtils::getRank(vIdLen_, key));
 }

 TEST_F(KafkaListenerTest, ReverseEdgeDetection) {
   std::string src = "player01";
   std::string dst = "team0001";
   int64_t rank = 2019;
   // Reverse edge uses negative edge type
   auto key = NebulaKeyUtils::edgeKey(vIdLen_, partId_, src, -edgeType_, rank, dst);

   ASSERT_TRUE(NebulaKeyUtils::isEdge(vIdLen_, key));
   auto parsedEdgeType = NebulaKeyUtils::getEdgeType(vIdLen_, key);
   EXPECT_LT(parsedEdgeType, 0);  // negative = reverse edge, should be skipped by apply()
 }

 TEST_F(KafkaListenerTest, NormalizeStringVid) {
   // String vid padded with null bytes should be trimmed
   std::string vid = "player01";
   vid.resize(vIdLen_, '\0');
   EXPECT_EQ("player01", normalizeStringVid(vid));

   // Empty vid after trimming
   std::string nullVid(vIdLen_, '\0');
   EXPECT_EQ("", normalizeStringVid(nullVid));

   // Vid that fills the entire length (no padding)
   std::string fullVid(vIdLen_, 'x');
   EXPECT_EQ(fullVid, normalizeStringVid(fullVid));
 }

 // ==================== Row Encoding/Decoding Tests ====================

 TEST_F(KafkaListenerTest, TagRowEncodingRoundtrip) {
   auto encoded = encodeTagValue("Tim Duncan", 42);
   auto reader = RowReaderWrapper::getTagPropReader(schemaMan_.get(), spaceId_, tagId_, encoded);
   ASSERT_TRUE(reader);
   EXPECT_EQ(2, reader->numFields());

   auto nameVal = reader->getValueByName("name");
   EXPECT_EQ(Value::Type::STRING, nameVal.type());
   EXPECT_EQ("Tim Duncan", nameVal.getStr());

   auto ageVal = reader->getValueByName("age");
   EXPECT_EQ(Value::Type::INT, ageVal.type());
   EXPECT_EQ(42, ageVal.getInt());
 }

 TEST_F(KafkaListenerTest, EdgeRowEncodingRoundtrip) {
   auto encoded = encodeEdgeValue(2016, 2023);
   auto reader =
       RowReaderWrapper::getEdgePropReader(schemaMan_.get(), spaceId_, edgeType_, encoded);
   ASSERT_TRUE(reader);
   EXPECT_EQ(2, reader->numFields());

   auto startVal = reader->getValueByName("start_year");
   EXPECT_EQ(Value::Type::INT, startVal.type());
   EXPECT_EQ(2016, startVal.getInt());

   auto endVal = reader->getValueByName("end_year");
   EXPECT_EQ(Value::Type::INT, endVal.type());
   EXPECT_EQ(2023, endVal.getInt());
 }

 // ==================== JSON Payload Construction Tests ====================
 // These replicate the core logic of KafkaListener::apply() to verify
 // correct deserialization and JSON construction without Raft infrastructure.

 TEST_F(KafkaListenerTest, VertexPutJsonPayload) {
   std::string vid = "player01";
   auto key = NebulaKeyUtils::tagKey(vIdLen_, partId_, vid, tagId_);
   auto value = encodeTagValue("Tim Duncan", 42);

   // Replicate apply() logic for vertex PUT
   ASSERT_TRUE(NebulaKeyUtils::isTag(vIdLen_, key));
   auto tagId = NebulaKeyUtils::getTagId(vIdLen_, key);
   auto rawVid = NebulaKeyUtils::getVertexId(vIdLen_, key).toString();
   auto vertexId = normalizeStringVid(rawVid);

   folly::dynamic payload = folly::dynamic::object;
   payload["spaceId"] = spaceId_;
   payload["partId"] = partId_;
   payload["logId"] = 100;
   payload["timestamp"] = 1234567890;
   payload["seq"] = 0;
   payload["type"] = "vertex";
   payload["vertexId"] = vertexId;
   payload["tagId"] = tagId;
   payload["operation"] = "PUT";
   payload["graphOperation"] = "UPSERT_VERTEX";

   auto reader = RowReaderWrapper::getTagPropReader(schemaMan_.get(), spaceId_, tagId, value);
   ASSERT_TRUE(reader);
   folly::dynamic props = folly::dynamic::object;
   for (size_t i = 0; i < reader->numFields(); ++i) {
     auto fieldName = reader->getSchema()->getFieldName(i);
     props[fieldName] = valueToDynamic(reader->getValueByIndex(i));
   }
   payload["properties"] = std::move(props);

   auto json = folly::toJson(payload);
   auto parsed = folly::parseJson(json);

   EXPECT_EQ("vertex", parsed["type"].getString());
   EXPECT_EQ("player01", parsed["vertexId"].getString());
   EXPECT_EQ(tagId_, parsed["tagId"].getInt());
   EXPECT_EQ("PUT", parsed["operation"].getString());
   EXPECT_EQ("UPSERT_VERTEX", parsed["graphOperation"].getString());
   EXPECT_TRUE(parsed.count("properties"));
   // String values should be clean (no extra quotes)
   EXPECT_EQ("Tim Duncan", parsed["properties"]["name"].getString());
   // Int values should be proper integers
   EXPECT_EQ(42, parsed["properties"]["age"].getInt());
   // Metadata fields
   EXPECT_EQ(100, parsed["logId"].getInt());
   EXPECT_EQ(1234567890, parsed["timestamp"].getInt());
   EXPECT_EQ(0, parsed["seq"].getInt());
 }

 TEST_F(KafkaListenerTest, EdgePutJsonPayload) {
   std::string src = "player01";
   std::string dst = "team0001";
   int64_t rank = 2019;
   auto key = NebulaKeyUtils::edgeKey(vIdLen_, partId_, src, edgeType_, rank, dst);
   auto value = encodeEdgeValue(2016, 2023);

   ASSERT_TRUE(NebulaKeyUtils::isEdge(vIdLen_, key));
   auto edgeType = NebulaKeyUtils::getEdgeType(vIdLen_, key);
   ASSERT_GT(edgeType, 0);  // forward edge

   auto srcId = normalizeStringVid(NebulaKeyUtils::getSrcId(vIdLen_, key).toString());
   auto dstId = normalizeStringVid(NebulaKeyUtils::getDstId(vIdLen_, key).toString());
   auto parsedRank = NebulaKeyUtils::getRank(vIdLen_, key);

   folly::dynamic payload = folly::dynamic::object;
   payload["spaceId"] = spaceId_;
   payload["partId"] = partId_;
   payload["logId"] = 200;
   payload["timestamp"] = 1234567890;
   payload["seq"] = 0;
   payload["type"] = "edge";
   payload["srcId"] = srcId;
   payload["dstId"] = dstId;
   payload["edgeType"] = edgeType;
   payload["ranking"] = parsedRank;
   payload["operation"] = "PUT";

   auto reader =
       RowReaderWrapper::getEdgePropReader(schemaMan_.get(), spaceId_, edgeType, value);
   ASSERT_TRUE(reader);
   folly::dynamic props = folly::dynamic::object;
   for (size_t i = 0; i < reader->numFields(); ++i) {
     auto fieldName = reader->getSchema()->getFieldName(i);
     props[fieldName] = valueToDynamic(reader->getValueByIndex(i));
   }
   payload["properties"] = std::move(props);

   auto json = folly::toJson(payload);
   auto parsed = folly::parseJson(json);

   EXPECT_EQ("edge", parsed["type"].getString());
   EXPECT_EQ("player01", parsed["srcId"].getString());
   EXPECT_EQ("team0001", parsed["dstId"].getString());
   EXPECT_EQ(edgeType_, parsed["edgeType"].getInt());
   EXPECT_EQ(rank, parsed["ranking"].getInt());
   EXPECT_EQ("PUT", parsed["operation"].getString());
   EXPECT_TRUE(parsed.count("properties"));
   // Int values should be proper integers
   EXPECT_EQ(2016, parsed["properties"]["start_year"].getInt());
   EXPECT_EQ(2023, parsed["properties"]["end_year"].getInt());
   // Metadata fields
   EXPECT_EQ(200, parsed["logId"].getInt());
   EXPECT_TRUE(parsed.count("timestamp"));
   EXPECT_TRUE(parsed.count("seq"));
 }

 TEST_F(KafkaListenerTest, VertexRemoveJsonPayload) {
   std::string vid = "player01";
   auto key = NebulaKeyUtils::tagKey(vIdLen_, partId_, vid, tagId_);

   ASSERT_TRUE(NebulaKeyUtils::isTag(vIdLen_, key));
   auto tagId = NebulaKeyUtils::getTagId(vIdLen_, key);
   auto vertexId = normalizeStringVid(NebulaKeyUtils::getVertexId(vIdLen_, key).toString());

   folly::dynamic payload = folly::dynamic::object;
   payload["type"] = "vertex";
   payload["vertexId"] = vertexId;
   payload["tagId"] = tagId;
   payload["operation"] = "REMOVE";

   auto json = folly::toJson(payload);
   auto parsed = folly::parseJson(json);

   EXPECT_EQ("vertex", parsed["type"].getString());
   EXPECT_EQ("player01", parsed["vertexId"].getString());
   EXPECT_EQ("REMOVE", parsed["operation"].getString());
   // REMOVE payloads should not have properties
   EXPECT_FALSE(parsed.count("properties"));
 }

 // ==================== Mock Adapter Tests ====================

 TEST_F(KafkaListenerTest, MockAdapterCapturesMessages) {
   KafkaMessage msg1;
   msg1.key = "key1";
   msg1.value = "value1";
   KafkaMessage msg2;
   msg2.key = "key2";
   msg2.value = "value2";

   ASSERT_TRUE(mockAdapter_->send(msg1).ok());
   EXPECT_EQ(1, mockAdapter_->getMessages().size());

   ASSERT_TRUE(mockAdapter_->send(msg2).ok());
   EXPECT_EQ(2, mockAdapter_->getMessages().size());
   EXPECT_EQ("key1", mockAdapter_->getMessages()[0].key);
   EXPECT_EQ("key2", mockAdapter_->getMessages()[1].key);
 }

 TEST_F(KafkaListenerTest, MockAdapterBatchSendAndClear) {
   std::vector<KafkaMessage> batch;
   for (int i = 0; i < 5; i++) {
     KafkaMessage msg;
     msg.key = folly::to<std::string>("key", i);
     msg.value = folly::to<std::string>("val", i);
     batch.emplace_back(std::move(msg));
   }

   ASSERT_TRUE(mockAdapter_->sendBatch(batch).ok());
   EXPECT_EQ(5, mockAdapter_->getMessages().size());

   mockAdapter_->clear();
   EXPECT_EQ(0, mockAdapter_->getMessages().size());
 }

 // ==================== BatchHolder Tests ====================

 TEST_F(KafkaListenerTest, BatchHolderPutAndRemove) {
   BatchHolder batch;
   auto tagKey = NebulaKeyUtils::tagKey(vIdLen_, partId_, "player01", tagId_);
   auto tagVal = encodeTagValue("Tim Duncan", 42);
   batch.put(std::string(tagKey), std::string(tagVal));

   auto edgeKey = NebulaKeyUtils::edgeKey(vIdLen_, partId_, "player01", edgeType_, 2019, "team0001");
   auto edgeVal = encodeEdgeValue(2016, 2023);
   batch.put(std::string(edgeKey), std::string(edgeVal));

   // Add a remove
   auto removeKey = NebulaKeyUtils::tagKey(vIdLen_, partId_, "player02", tagId_);
   batch.remove(std::string(removeKey));

   const auto& logs = batch.getBatch();
   ASSERT_EQ(3, logs.size());

   // First entry: tag PUT
   EXPECT_EQ(BatchLogType::OP_BATCH_PUT, std::get<0>(logs[0]));
   EXPECT_TRUE(NebulaKeyUtils::isTag(vIdLen_, std::get<1>(logs[0])));

   // Second entry: edge PUT
   EXPECT_EQ(BatchLogType::OP_BATCH_PUT, std::get<0>(logs[1]));
   EXPECT_TRUE(NebulaKeyUtils::isEdge(vIdLen_, std::get<1>(logs[1])));

   // Third entry: tag REMOVE
   EXPECT_EQ(BatchLogType::OP_BATCH_REMOVE, std::get<0>(logs[2]));
   EXPECT_TRUE(NebulaKeyUtils::isTag(vIdLen_, std::get<1>(logs[2])));
 }

 TEST_F(KafkaListenerTest, BatchIterationSkipsNonTagEdge) {
   BatchHolder batch;

   // Add a tag key (should be processed)
   auto tagKey = NebulaKeyUtils::tagKey(vIdLen_, partId_, "player01", tagId_);
   batch.put(std::string(tagKey), encodeTagValue("Tim Duncan", 42));

   // Add an edge key (should be processed)
   auto edgeKey = NebulaKeyUtils::edgeKey(vIdLen_, partId_, "player01", edgeType_, 2019, "team0001");
   batch.put(std::string(edgeKey), encodeEdgeValue(2016, 2023));

   // Add a reverse edge (should be skipped by apply logic)
   auto revKey = NebulaKeyUtils::edgeKey(vIdLen_, partId_, "team0001", -edgeType_, 2019, "player01");
   batch.put(std::string(revKey), encodeEdgeValue(2016, 2023));

   // Simulate apply() iteration: count how many messages would be produced
   int messageCount = 0;
   for (const auto& log : batch.getBatch()) {
     const auto& key = std::get<1>(log);
     bool isTag = NebulaKeyUtils::isTag(vIdLen_, key);
     bool isEdge = NebulaKeyUtils::isEdge(vIdLen_, key);
     if (!(isTag || isEdge)) {
       continue;
     }
     if (isEdge) {
       auto et = NebulaKeyUtils::getEdgeType(vIdLen_, key);
       if (et < 0) continue;  // skip reverse edge
     }
     messageCount++;
   }
   // Only forward tag + forward edge should be counted
   EXPECT_EQ(2, messageCount);
 }

 // ==================== Integration Test Infrastructure ====================

 /**
  * @brief MockKafkaAdapter that writes to a shared external message store.
  * Each call to getKafkaAdapter() creates a new instance, but all share the same vector.
  */
 class SharedMockKafkaAdapter : public KafkaAdapter {
  public:
   explicit SharedMockKafkaAdapter(std::shared_ptr<std::vector<KafkaMessage>> store)
       : KafkaAdapter(KafkaClientConfig{}),
         store_(std::move(store)) {}

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

 /**
  * @brief Testable KafkaListener that overrides getKafkaAdapter() to inject mock.
  * Exposes captured Kafka messages for verification.
  */
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

 using nebula::meta::ListenerHosts;
 using nebula::meta::PartHosts;

 /**
  * @brief Integration test fixture following NebulaListenerTest pattern.
  * Sets up full Raft cluster with TestableKafkaListener as the listener.
  */
 class KafkaListenerIntegrationTest
     : public ::testing::TestWithParam<std::tuple<int32_t, int32_t, int32_t>> {
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
     // tag: player(name string, age int64), tagId = 3
     auto tagSchema = std::make_shared<meta::NebulaSchemaProvider>(0);
     tagSchema->addField("name", nebula::cpp2::PropertyType::STRING);
     tagSchema->addField("age", nebula::cpp2::PropertyType::INT64);
     schemaMan_->addTagSchema(spaceId_, tagId_, tagSchema);

     // edge: serve(start_year int64, end_year int64), edgeType = 5
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
       partMan->partsMap()[spaceId_][partId] = std::move(ph);
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

   std::unique_ptr<fs::TempDir> rootPath_;
   std::unique_ptr<mock::AdHocSchemaManager> schemaMan_;
   std::vector<HostAddr> peers_;
   std::vector<HostAddr> listenerHosts_;
   std::vector<std::unique_ptr<NebulaStore>> stores_;
   std::vector<std::unique_ptr<NebulaStore>> listeners_;
   std::unordered_map<PartitionID, std::shared_ptr<TestableKafkaListener>> kafkaListeners_;
 };

 // ==================== Integration Test Cases ====================

 TEST_P(KafkaListenerIntegrationTest, VertexPutTest) {
   LOG(INFO) << "Insert vertex data with proper tag keys";
   // AdHocSchemaManager returns vIdLen=32
   int32_t vIdLen = 32;
   for (int32_t partId = 1; partId <= partCount_; partId++) {
     std::vector<KV> data;
     for (int32_t i = 0; i < 10; i++) {
       auto vid = folly::stringPrintf("player_%d_%d", partId, i);
       auto key = NebulaKeyUtils::tagKey(vIdLen, partId, vid, tagId_);
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

   // Wait for Raft sync to listener
   sleep(FLAGS_raft_heartbeat_interval_secs + 1);

   LOG(INFO) << "Verify Kafka messages";
   for (int32_t partId = 1; partId <= partCount_; partId++) {
     auto kafka = kafkaListeners_[partId];
     const auto& messages = kafka->getMessages();
     ASSERT_EQ(10, messages.size()) << "partId=" << partId;

     // Verify first message payload
     auto parsed = folly::parseJson(messages[0].value);
     EXPECT_EQ("vertex", parsed["type"].getString());
     EXPECT_EQ("PUT", parsed["operation"].getString());
     EXPECT_EQ(tagId_, parsed["tagId"].getInt());
     EXPECT_TRUE(parsed.count("properties"));
     EXPECT_TRUE(parsed["properties"].count("name"));
     EXPECT_TRUE(parsed["properties"].count("age"));
     // Verify metadata fields
     EXPECT_TRUE(parsed.count("logId"));
     EXPECT_TRUE(parsed.count("timestamp"));
     EXPECT_TRUE(parsed.count("seq"));
     // Verify string values are clean (no extra quotes)
     EXPECT_TRUE(parsed["properties"]["name"].isString());
     // Verify int values are proper integers
     EXPECT_TRUE(parsed["properties"]["age"].isInt());
   }
 }

 TEST_P(KafkaListenerIntegrationTest, EdgePutTest) {
   LOG(INFO) << "Insert edge data with proper edge keys";
   int32_t vIdLen = 32;
   for (int32_t partId = 1; partId <= partCount_; partId++) {
     std::vector<KV> data;
     for (int32_t i = 0; i < 10; i++) {
       auto src = folly::stringPrintf("player_%d_%d", partId, i);
       auto dst = folly::stringPrintf("team_%d_%d", partId, i);
       // Forward edge
       auto key = NebulaKeyUtils::edgeKey(vIdLen, partId, src, edgeType_, 2020 + i, dst);
       auto val = encodeEdgeValue(2016, 2020 + i);
       data.emplace_back(std::move(key), std::move(val));
       // Reverse edge (should be skipped by apply)
       auto revKey = NebulaKeyUtils::edgeKey(vIdLen, partId, dst, -edgeType_, 2020 + i, src);
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
     // 20 KVs written (10 forward + 10 reverse), only 10 forward should produce messages
     ASSERT_EQ(10, messages.size()) << "partId=" << partId;

     auto parsed = folly::parseJson(messages[0].value);
     EXPECT_EQ("edge", parsed["type"].getString());
     EXPECT_EQ("PUT", parsed["operation"].getString());
     EXPECT_EQ(edgeType_, parsed["edgeType"].getInt());
     EXPECT_TRUE(parsed.count("properties"));
     // Verify metadata fields
     EXPECT_TRUE(parsed.count("logId"));
     EXPECT_TRUE(parsed.count("timestamp"));
     EXPECT_TRUE(parsed.count("seq"));
   }
 }

 TEST_P(KafkaListenerIntegrationTest, CommitSnapshotTest) {
   LOG(INFO) << "Commit snapshot with vertex data";
   int32_t vIdLen = 32;
   for (int32_t partId = 1; partId <= partCount_; partId++) {
     std::vector<std::string> rows;
     int64_t totalSize = 0;
     for (int32_t i = 0; i < 10; i++) {
       auto vid = folly::stringPrintf("player_%d_%d", partId, i);
       auto key = NebulaKeyUtils::tagKey(vIdLen, partId, vid, tagId_);
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
       // Snapshot messages should have metadata
       EXPECT_TRUE(parsed.count("logId"));
       EXPECT_TRUE(parsed.count("timestamp"));
       EXPECT_TRUE(parsed.count("seq"));
     }
   }
 }

 INSTANTIATE_TEST_SUITE_P(PartCount_Replicas_ListenerCount,
                          KafkaListenerIntegrationTest,
                          ::testing::Values(std::make_tuple(1, 1, 1)));

 }  // namespace kvstore
 }  // namespace nebula

 int main(int argc, char** argv) {
   testing::InitGoogleTest(&argc, argv);
   folly::init(&argc, &argv, true);
   google::SetStderrLogging(google::INFO);
   return RUN_ALL_TESTS();
 }
 
