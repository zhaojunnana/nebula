/* Copyright (c) 2024 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef KVSTORE_LISTENER_KAFKA_KAFKA_ADAPTER_H_
#define KVSTORE_LISTENER_KAFKA_KAFKA_ADAPTER_H_

#include "common/base/Base.h"
#include "common/base/Status.h"
#include "common/network/NetworkUtils.h"

typedef struct rd_kafka_s rd_kafka_t;

namespace nebula {
namespace kvstore {

/**
 * @brief Kafka client configuration
 */
struct KafkaClientConfig {
  std::string brokers;
  std::string securityProtocol{"PLAINTEXT"};
  std::string saslMechanism;
  std::string username;
  std::string password;
  int32_t batchSize{1048576};
  int32_t lingerMs{50};
};

/**
 * @brief Kafka message structure
 */
struct KafkaMessage {
  std::string key;
  std::string value;
};

/**
 * @brief Kafka adapter for sending messages to Kafka with at-least-once semantics.
 * Uses idempotent producer for within-session dedup. Cross-session duplicates
 * are tolerable because all downstream operations (UPSERT/DELETE) are idempotent.
 */
class KafkaAdapter {
 public:
  KafkaAdapter(KafkaClientConfig config, const std::string& topic, PartitionID partId);
  KafkaAdapter(const KafkaAdapter&) = delete;
  KafkaAdapter& operator=(const KafkaAdapter&) = delete;

  virtual Status send(const KafkaMessage& message);

  /**
   * @brief Send a batch of messages to Kafka.
   * Produces all messages and flushes. Delivery failures are detected via callback.
   */
  virtual Status sendBatch(const std::vector<KafkaMessage>& messages);

  virtual ~KafkaAdapter();

  bool isValid() const {
    return producer_ != nullptr;
  }

  // Delivery tracking counters, accessed by librdkafka callback in .cpp
  std::atomic<int64_t> deliverySuccessCount_{0};
  std::atomic<int64_t> deliveryFailureCount_{0};

 private:
  Status initProducer();
  Status produce(const KafkaMessage& message);

  KafkaClientConfig config_;
  std::string topic_;
  rd_kafka_t* producer_{nullptr};
  const PartitionID partid_;
};

}  // namespace kvstore
}  // namespace nebula
#endif
