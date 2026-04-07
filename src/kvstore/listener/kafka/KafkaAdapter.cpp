/* Copyright (c) 2024 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "kvstore/listener/kafka/KafkaAdapter.h"

#include <atomic>

#include <librdkafka/rdkafka.h>

#include "common/base/Base.h"

namespace nebula {
namespace kvstore {

namespace {

/**
 * @brief Delivery report callback with proper librdkafka signature.
 * opaque points to KafkaAdapter instance for tracking delivery stats.
 */
void onDeliveryReport(rd_kafka_t* /*rk*/,
                      const rd_kafka_message_t* rkmessage,
                      void* opaque) {
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
    rd_kafka_flush(rk, 10 * 1000);  // wait up to 10 seconds
    rd_kafka_destroy(rk);
    producer_ = nullptr;
  }
}

Status KafkaAdapter::initProducer() {
  char errstr[512];
  rd_kafka_conf_t* conf = rd_kafka_conf_new();

  if (rd_kafka_conf_set(conf, "bootstrap.servers", config_.brokers.c_str(),
                        errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
    rd_kafka_conf_destroy(conf);
    return Status::Error(folly::stringPrintf("Invalid bootstrap.servers: %s", errstr));
  }

  // Transactional producer implies idempotence
  if (config_.transactionalId.empty()) {
    rd_kafka_conf_destroy(conf);
    return Status::Error("transactionalId is required for exactly-once semantics");
  }
  rd_kafka_conf_set(conf, "transactional.id", config_.transactionalId.c_str(), nullptr, 0);
  // Idempotence is automatically enabled by transactional.id, but set explicitly for clarity
  rd_kafka_conf_set(conf, "enable.idempotence", "true", nullptr, 0);

  if (config_.lingerMs > 0) {
    rd_kafka_conf_set(conf, "linger.ms", std::to_string(config_.lingerMs).c_str(), nullptr, 0);
  }

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

  // Register delivery report callback for tracking delivery failures
  rd_kafka_conf_set_dr_msg_cb(conf, onDeliveryReport);
  rd_kafka_conf_set_opaque(conf, this);

  rd_kafka_t* rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
  if (!rk) {
    return Status::Error(folly::stringPrintf("Failed to create Kafka producer: %s", errstr));
  }
  producer_ = rk;

  // Initialize transactions. This call:
  // 1. Registers transactional.id with the broker's transaction coordinator
  // 2. Fences off any previous producer instance with the same transactional.id
  //    (aborts their in-flight transactions), preventing duplicates on restart
  // 3. Recovers any pending transactions from a previous crash
  rd_kafka_error_t* err = rd_kafka_init_transactions(rk, 30 * 1000);  // 30s timeout
  if (err) {
    auto errMsg = folly::stringPrintf("init_transactions failed: %s", rd_kafka_error_string(err));
    bool isFatal = rd_kafka_error_is_fatal(err);
    rd_kafka_error_destroy(err);
    if (isFatal) {
      rd_kafka_destroy(rk);
      producer_ = nullptr;
    }
    return Status::Error(errMsg);
  }

  transactionReady_ = true;
  LOG(INFO) << "Kafka transactional producer initialized, txn.id=" << config_.transactionalId;
  return Status::OK();
}

Status KafkaAdapter::send(const KafkaMessage& message) {
  return sendBatch({message});
}

Status KafkaAdapter::sendBatch(const std::vector<KafkaMessage>& messages) {
  if (messages.empty()) {
    return Status::OK();
  }

  if (!producer_ || !transactionReady_) {
    return Status::Error("Kafka transactional producer not initialized");
  }

  rd_kafka_t* rk = static_cast<rd_kafka_t*>(producer_);

  // ---- BEGIN TRANSACTION ----
  rd_kafka_error_t* err = rd_kafka_begin_transaction(rk);
  if (err) {
    auto errMsg = folly::stringPrintf("begin_transaction failed: %s", rd_kafka_error_string(err));
    rd_kafka_error_destroy(err);
    return Status::Error(errMsg);
  }

  // Reset delivery counters
  deliverySuccessCount_.store(0, std::memory_order_relaxed);
  deliveryFailureCount_.store(0, std::memory_order_relaxed);

  // Produce all messages
  for (const auto& message : messages) {
    auto status = produce(message);
    if (!status.ok()) {
      LOG(ERROR) << "Failed to produce message, aborting transaction: " << status;
      // Abort: discard all messages in this transaction
      rd_kafka_error_t* abortErr = rd_kafka_abort_transaction(rk, 10 * 1000);
      if (abortErr) {
        LOG(ERROR) << "abort_transaction failed: " << rd_kafka_error_string(abortErr);
        rd_kafka_error_destroy(abortErr);
      }
      return status;
    }
  }

  // Flush all queued messages to broker. Use -1 (infinite) to guarantee no
  // messages are silently dropped due to timeout.
  rd_kafka_flush(rk, -1);

  // Check delivery results
  auto failCount = deliveryFailureCount_.exchange(0, std::memory_order_relaxed);
  if (failCount > 0) {
    auto successCount = deliverySuccessCount_.exchange(0, std::memory_order_relaxed);
    LOG(ERROR) << "Delivery failures detected, aborting transaction";
    rd_kafka_error_t* abortErr = rd_kafka_abort_transaction(rk, 10 * 1000);
    if (abortErr) {
      LOG(ERROR) << "abort_transaction failed: " << rd_kafka_error_string(abortErr);
      rd_kafka_error_destroy(abortErr);
    }
    return Status::Error(
        folly::stringPrintf("Kafka delivery failed for %ld of %ld messages",
                            failCount, failCount + successCount));
  }

  // ---- COMMIT TRANSACTION ----
  // This is the atomic commit point. Either all messages become visible to
  // consumers (with isolation.level=read_committed) or none do.
  err = rd_kafka_commit_transaction(rk, 30 * 1000);
  if (err) {
    auto errMsg = folly::stringPrintf("commit_transaction failed: %s", rd_kafka_error_string(err));
    bool isRetriable = rd_kafka_error_is_retriable(err);
    bool isTxnRequiresAbort = rd_kafka_error_txn_requires_abort(err);
    rd_kafka_error_destroy(err);

    if (isTxnRequiresAbort) {
      rd_kafka_error_t* abortErr = rd_kafka_abort_transaction(rk, 10 * 1000);
      if (abortErr) {
        LOG(ERROR) << "abort_transaction failed: " << rd_kafka_error_string(abortErr);
        rd_kafka_error_destroy(abortErr);
      }
    }
    (void)isRetriable;  // Let caller retry the whole batch
    return Status::Error(errMsg);
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
        int backoffMs = kInitialBackoffMs * (1 << attempt);  // 100, 200, 400, 800, 1600
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
