/* Copyright (c) 2024 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#ifndef KVSTORE_LISTENER_KAFKA_KAFKA_ADAPTER_H_
#define KVSTORE_LISTENER_KAFKA_KAFKA_ADAPTER_H_

#include "common/base/Base.h"
#include "common/base/Status.h"
#include "common/network/NetworkUtils.h"

namespace nebula {
namespace kvstore {

/**
 * @brief Kafka client configuration
 */
struct KafkaClientConfig {
  std::string brokers;
  std::string topic;
  std::string securityProtocol{"PLAINTEXT"};
  std::string saslMechanism;
  std::string username;
  std::string password;
  int32_t batchSize{100};
  int32_t lingerMs{10};
  // Stable transactional ID for exactly-once semantics.
  // Must be unique per listener partition and survive restarts.
  std::string transactionalId;
};

/**
 * @brief Kafka message structure
 */
struct KafkaMessage {
  std::string key;
  std::string value;
  int32_t partition{-1};  // -1 means auto partition
};

/**
 * @brief Kafka adapter for sending messages to Kafka with exactly-once semantics.
 * Uses Kafka transactions to guarantee no duplicates on crash-restart.
 */
class KafkaAdapter {
 public:
  /**
   * @brief Construct Kafka adapter with a single config
   *
   * @param config Kafka client configuration (brokers, topic, auth, etc.)
   */
  explicit KafkaAdapter(KafkaClientConfig config);

  /**
   * @brief Send a single message to Kafka within a transaction
   *
   * @param message Kafka message
   * @return Status
   */
  virtual Status send(const KafkaMessage& message);

  /**
   * @brief Send a batch of messages to Kafka within a single transaction.
   * Either all messages are committed or none are (atomic).
   *
   * @param messages List of Kafka messages
   * @return Status
   */
  virtual Status sendBatch(const std::vector<KafkaMessage>& messages);

  /**
   * @brief Virtual destructor
   */
  virtual ~KafkaAdapter();

  /**
   * @brief Check if adapter is valid
   *
   * @return True if valid
   */
  bool isValid() const {
    return producer_ != nullptr && transactionReady_;
  }

  // Delivery tracking counters, accessed by librdkafka callback in .cpp
  std::atomic<int64_t> deliverySuccessCount_{0};
  std::atomic<int64_t> deliveryFailureCount_{0};

 private:
  /**
   * @brief Initialize librdkafka transactional producer
   *
   * @return Status
   */
  Status initProducer();

  /**
   * @brief Send message via librdkafka with iterative retry on queue full
   *
   * @param message Kafka message
   * @return Status
   */
  Status produce(const KafkaMessage& message);

  KafkaClientConfig config_;
  void* producer_{nullptr};  // rd_kafka_t*
  bool transactionReady_{false};
};

}  // namespace kvstore
}  // namespace nebula
#endif
