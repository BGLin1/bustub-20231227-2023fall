#include "concurrency/watermark.h"
#include <exception>
#include "common/exception.h"

namespace bustub {

  auto Watermark::AddTxn(timestamp_t read_ts) -> void {
    if (read_ts < commit_ts_) {
      throw Exception("read ts < commit ts");
    }

    // TODO(fall2023): implement me!
    if (this->current_reads_.count(read_ts) != 0) {
      this->current_reads_[read_ts]++;
    } else {
      this->current_reads_[read_ts] = 1;
      heap_.emplace(read_ts);
    }

    if (read_ts < this->watermark_) {
      this->watermark_ = read_ts;
    }

  }

  auto Watermark::RemoveTxn(timestamp_t read_ts) -> void {
    // TODO(fall2023): implement me!
    if (this->current_reads_.count(read_ts) == 0 || this->current_reads_[read_ts] <= 0) {
      throw Exception("read ts not exist or count <= 0");
    }
    this->current_reads_[read_ts]--;
    if (this->current_reads_[read_ts] <= 0) {
      this->current_reads_.erase(read_ts);
      while (!this->heap_.empty() && this->current_reads_.count(this->heap_.top()) == 0) {
        this->heap_.pop();
      }
    }
    if (read_ts == this->watermark_) {
      if (this->heap_.empty()) {
        this->watermark_ = BUSTUB_INT64_MAX;
        return;
      }
    }
    this->watermark_ = this->heap_.top();
  }

}  // namespace bustub
