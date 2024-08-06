#pragma once

#include <unordered_map>
#include <queue>
#include <functional>
#include "concurrency/transaction.h"
#include "storage/table/tuple.h"

namespace bustub {

  /**
   * @brief tracks all the read timestamps.
   *
   */
  class Watermark {
  public:
    explicit Watermark(timestamp_t commit_ts) : commit_ts_(commit_ts), watermark_(commit_ts) {}

    auto AddTxn(timestamp_t read_ts) -> void;

    auto RemoveTxn(timestamp_t read_ts) -> void;

    /** The caller should update commit ts before removing the txn from the watermark so that we can track watermark
     * correctly. */
    auto UpdateCommitTs(timestamp_t commit_ts) { commit_ts_ = commit_ts; }

    auto GetWatermark() -> timestamp_t {
      if (current_reads_.empty()) {
        return commit_ts_;
      }
      return watermark_;
    }

    timestamp_t commit_ts_;
    //Watermark is the lowest read timestamp among all in-progress transactions.
    timestamp_t watermark_;

    std::unordered_map<timestamp_t, int> current_reads_;

    //自定义的优先队列
    std::priority_queue<timestamp_t, std::vector<timestamp_t>,std::function<bool(timestamp_t&, timestamp_t&)>> heap_ {
        [&](timestamp_t& t1, timestamp_t& t2) ->bool { return t1 > t2; }
    };
  };

};  // namespace bustub
