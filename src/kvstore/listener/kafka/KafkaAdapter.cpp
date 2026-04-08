/* Copyright (c) 2024 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "kvstore/listener/kafka/KafkaAdapter.h"

#include <librdkafka/rdkafka.h>

#include <atomic>

#include "common/base/Base.h"

namespace nebula {
namespace kvstore {

namespace {

void onDeliveryReport(rd_kafka_t* /*rk*/, const rd_kafka_message_t* rkmessage, void* opaque) {
  auto* adapter = static_cast<KafkaAdapter*>(opaque);
  if (rkmessage->err) {
    adapter->deliveryFailureCount_.fetch_add(1, std::memory_order_relaxed);
    LOG(ERROR) << "Kafka delivery failed: " << rd_kafka_err2str(rkmessage->err);
  } else {
    adapter->deliverySuccessCount_.fetch_add(1, std::memory_order_relaxed);
  }
}

}  // anonymous namespace

KafkaAdapter::KafkaAdapter(KafkaClientConfig config) : config_(std::move(config)) {
  if (config_.brokers.empty()) {
    LOG(WARNING) << "KafkaAdapter initialized with empty brokers";
  } else {
    auto status = initProducer();
    if (!status.ok()) {
      LOG(ERROR) << "KafkaAdapter initProducer failed: " << status;
    }
  }
}

KafkaAdapter::~KafkaAdapter() {
  if (producer_) {
    rd_kafka_t* rk = static_cast<rd_kafka_t*>(producer_);
    rd_kafka_flush(rk, 10 * 1000);
    rd_kafka_destroy(rk);
    producer_ = nullptr;
  }
}

Status KafkaAdapter::initProducer() {
  char errstr[512];
  rd_kafka_conf_t* conf = rd_kafka_conf_new();

  if (rd_kafka_conf_set(
          conf, "bootstrap.servers", config_.brokers.c_str(), errstr, sizeof(errstr)) !=
      RD_KAFKA_CONF_OK) {
    rd_kafka_conf_destroy(conf);
    return Status::Error(folly::stringPrintf("Invalid bootstrap.servers: %s", errstr));
  }
  if (config_.lingerMs > 0) {
    rd_kafka_conf_set(conf, "linger.ms", std::to_string(config_.lingerMs).c_str(), nullptr, 0);
  }
  if (config_.batchSize > 0) {
    rd_kafka_conf_set(conf, "batch.size", std::to_string(config_.batchSize).c_str(), nullptr, 0);
  }

  // lz4 compression: reduces network I/O with minimal CPU cost
  rd_kafka_conf_set(conf, "compression.type", "lz4", nullptr, 0);

  // Large internal queue to accommodate big batches without blocking on produce()
  rd_kafka_conf_set(conf, "queue.buffering.max.messages", "500000", nullptr, 0);
  rd_kafka_conf_set(conf, "queue.buffering.max.kbytes", "1048576", nullptr, 0);  // 1GB

  // SASL/SSL if configured
  if (!config_.securityProtocol.empty() && config_.securityProtocol != "PLAINTEXT") {
    rd_kafka_conf_set(conf, "security.protocol", config_.securityProtocol.c_str(), nullptr, 0);
  }
  if (!config_.saslMechanism.empty()) {
    rd_kafka_conf_set(conf, "sasl.mechanism", config_.saslMechanism.c_str(), nullptr, 0);
  }
  if (!config_.username.empty()) {
    rd_kafka_conf_set(conf, "sasl.username", config_.username.c_str(), nullptr, 0);
  }
  if (!config_.password.empty()) {
    rd_kafka_conf_set(conf, "sasl.password", config_.password.c_str(), nullptr, 0);
  }

  rd_kafka_conf_set_dr_msg_cb(conf, onDeliveryReport);
  rd_kafka_conf_set_opaque(conf, this);

  rd_kafka_t* rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
  if (!rk) {
    return Status::Error(folly::stringPrintf("Failed to create Kafka producer: %s", errstr));
  }

  producer_ = rk;
  return Status::OK();
}

Status KafkaAdapter::send(const KafkaMessage& message) {
  return sendBatch({message});
}

Status KafkaAdapter::sendBatch(const std::vector<KafkaMessage>& messages) {
  if (messages.empty()) {
    return Status::OK();
  }

  if (!producer_) {
    return Status::Error("Kafka producer not initialized");
  }

  rd_kafka_t* rk = static_cast<rd_kafka_t*>(producer_);

  // Reset delivery counters
  deliverySuccessCount_.store(0, std::memory_order_relaxed);
  deliveryFailureCount_.store(0, std::memory_order_relaxed);

  // Produce all messages (enqueue to librdkafka internal buffer)
  for (const auto& message : messages) {
    auto status = produce(message);
    if (!status.ok()) {
      LOG(ERROR) << "Failed to produce message: " << status;
      return status;
    }
  }

  // Flush: wait for all in-flight messages to be delivered (or fail)
  rd_kafka_resp_err_t flushErr = rd_kafka_flush(rk, 30 * 1000);
  if (flushErr) {
    return Status::Error("Kafka flush timed out");
  }

  // Check delivery results
  auto failCount = deliveryFailureCount_.load(std::memory_order_relaxed);
  if (failCount > 0) {
    auto successCount = deliverySuccessCount_.load(std::memory_order_relaxed);
    return Status::Error(folly::stringPrintf(
        "Kafka delivery failed for %ld of %ld messages", failCount, failCount + successCount));
  }

  return Status::OK();
}

Status KafkaAdapter::produce(const KafkaMessage& message) {
  rd_kafka_t* rk = static_cast<rd_kafka_t*>(producer_);
  int32_t partition = message.partition >= 0 ? message.partition : RD_KAFKA_PARTITION_UA;

  static constexpr int kMaxRetries = 5;
  static constexpr int kInitialBackoffMs = 100;

  for (int attempt = 0; attempt <= kMaxRetries; ++attempt) {
    rd_kafka_resp_err_t produceErr;
    if (message.key.empty()) {
      produceErr = rd_kafka_producev(
          rk,
          RD_KAFKA_V_TOPIC(config_.topic.c_str()),
          RD_KAFKA_V_PARTITION(partition),
          RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
          RD_KAFKA_V_VALUE(const_cast<char*>(message.value.data()), message.value.size()),
          RD_KAFKA_V_END);
    } else {
      produceErr = rd_kafka_producev(
          rk,
          RD_KAFKA_V_TOPIC(config_.topic.c_str()),
          RD_KAFKA_V_PARTITION(partition),
          RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
          RD_KAFKA_V_KEY(const_cast<char*>(message.key.data()), message.key.size()),
          RD_KAFKA_V_VALUE(const_cast<char*>(message.value.data()), message.value.size()),
          RD_KAFKA_V_END);
    }

    if (!produceErr) {
      return Status::OK();
    }

    if (produceErr == RD_KAFKA_RESP_ERR__QUEUE_FULL) {
      if (attempt < kMaxRetries) {
        int backoffMs = kInitialBackoffMs * (1 << attempt);
        rd_kafka_poll(rk, backoffMs);
        continue;
      }
      return Status::Error("Kafka produce queue full after max retries");
    }

    return Status::Error(
        folly::stringPrintf("Kafka produce failed: %s", rd_kafka_err2str(produceErr)));
  }

  return Status::Error("Kafka produce failed after max retries");
}

}  // namespace kvstore
}  // namespace nebula
