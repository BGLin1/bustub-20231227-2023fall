//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// transaction_manager.cpp
//
// Identification: src/concurrency/transaction_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/transaction_manager.h"

#include <memory>
#include <mutex>  // NOLINT
#include <optional>
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>

#include "catalog/catalog.h"
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "concurrency/transaction.h"
#include "execution/execution_common.h"
#include "storage/table/table_heap.h"
#include "storage/table/tuple.h"
#include "type/type_id.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

  auto TransactionManager::Begin(IsolationLevel isolation_level) -> Transaction* {
    std::unique_lock<std::shared_mutex> l(txn_map_mutex_);
    auto txn_id = next_txn_id_++;
    auto txn = std::make_unique<Transaction>(txn_id, isolation_level);
    auto* txn_ref = txn.get();
    txn_map_.insert(std::make_pair(txn_id, std::move(txn)));

    // TODO(fall2023): set the timestamps here. Watermark updated below.
    txn_ref->read_ts_ = this->last_commit_ts_.load();

    running_txns_.AddTxn(txn_ref->read_ts_);
    return txn_ref;
  }

  auto TransactionManager::VerifyTxn(Transaction* txn) -> bool { return true; }

  auto TransactionManager::Commit(Transaction* txn) -> bool {
    std::unique_lock<std::mutex> commit_lck(commit_mutex_);

    // TODO(fall2023): acquire commit ts!
    if (txn->state_ != TransactionState::RUNNING) {
      throw Exception("txn not in running state");
    }

    if (txn->GetIsolationLevel() == IsolationLevel::SERIALIZABLE) {
      if (!VerifyTxn(txn)) {
        commit_lck.unlock();
        Abort(txn);
        return false;
      }
    }

    // TODO(fall2023): Implement the commit logic!

    std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);

    // TODO(fall2023): set commit timestamp + update last committed timestamp here.
    txn->commit_ts_ = ++last_commit_ts_;
    txn->state_ = TransactionState::COMMITTED;
    //根据事件的 write set 修改所有被影响到的tuple的时间戳
    for (auto& [tid, set] : txn->write_set_) {
      // 一张表内的所有写过的tuple
      for (auto& rid : set) {
        auto table_info = catalog_->GetTable(tid);
        if (table_info == Catalog::NULL_TABLE_INFO) {
          throw Exception{ "Invalid table id" };
        }
        //根据RID找到对应的meta
        auto& table = table_info->table_;
        auto meta = table->GetTupleMeta(rid);
        meta.ts_ = txn->commit_ts_;
        //修改meta
        table->UpdateTupleMeta(meta, rid);
      }
    }
    running_txns_.UpdateCommitTs(txn->commit_ts_);
    running_txns_.RemoveTxn(txn->read_ts_);

    return true;
  }

  void TransactionManager::Abort(Transaction* txn) {
    if (txn->state_ != TransactionState::RUNNING && txn->state_ != TransactionState::TAINTED) {
      throw Exception("txn not in running / tainted state");
    }

    // TODO(fall2023): Implement the abort logic!
    std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);
    txn->state_ = TransactionState::ABORTED;
    running_txns_.RemoveTxn(txn->read_ts_);
  }

  // remove all transactions that do not contain any undo log that is visible to the transaction with the lowest read_ts (watermark).
  void TransactionManager::GarbageCollection() {
    // UNIMPLEMENTED("not implemented");
    std::unordered_set<txn_id_t> txn_set;
    std::vector<std::string> table_names = catalog_->GetTableNames();
    timestamp_t water_mark = GetWatermark();
    fmt::println(stderr, "Watermark {}, {} txn in GC", water_mark, txn_map_.size());
    //扫描所有表
    for (const auto& name : table_names) {
      TableInfo* table_info = catalog_->GetTable(name);
      TableIterator table_iter = table_info->table_->MakeIterator();
      while (!table_iter.IsEnd()) {
        //表中的每一个Tuple
        const auto& [meta, tuple] = table_iter.GetTuple();
        if (meta.ts_ > water_mark) {
          std::optional<UndoLink> undo_link_optional = GetUndoLink(tuple.GetRid());
          if (undo_link_optional.has_value()) {
            bool is_not_first = false;
            while (undo_link_optional.value().IsValid()) {
              std::optional<UndoLog> undo_log_optinal = GetUndoLogOptional(undo_link_optional.value());
              if (undo_log_optinal.has_value()) {
                //当前undolog的时间戳小于水位 
                if (undo_log_optinal.value().ts_ <= water_mark) {
                  if (is_not_first) {
                    break;
                  }
                  is_not_first = true;
                }
                txn_id_t txn_id = undo_link_optional.value().prev_txn_;
                if (txn_set.count(txn_id) == 0) {
                  txn_set.insert(txn_id);
                }
                undo_link_optional = undo_log_optinal.value().prev_version_;
              } else {
                break;
              }
            }
          }
        }
        ++table_iter;
      }
    }

    std::unique_lock<std::shared_mutex> lk(txn_map_mutex_);
    for (auto i = txn_map_.begin(); i != txn_map_.end();) {
      if (txn_set.count(i->first) == 0 && ((i->second->GetTransactionState() == TransactionState::COMMITTED) ||
        (i->second->GetTransactionState() == TransactionState::ABORTED))) {
        txn_map_.erase(i++);
      } else {
        ++i;
      }
    }

  }

}  // namespace bustub
