#include "execution/execution_common.h"
#include <memory>
// #include <mutex>
#include <optional>
#include <shared_mutex>
#include <sstream>
#include <string>
#include <vector>
#include "catalog/catalog.h"
#include "catalog/schema.h"
#include "common/config.h"
#include "common/macros.h"
#include "concurrency/transaction_manager.h"
#include "fmt/core.h"
#include "fmt/format.h"
#include "fmt/ostream.h"
#include "fmt/color.h"
#include "storage/table/table_heap.h"
#include "storage/table/tuple.h"
#include "type/value.h"
#include "type/value_factory.h"

//该文件中均为execution能用到的公共函数
namespace bustub {

  // 根据undologs重构事务应该查找出的tuple
  auto ReconstructTuple(const Schema* schema, const Tuple& base_tuple, const TupleMeta& base_meta,
    const std::vector<UndoLog>& undo_logs) -> std::optional<Tuple> {
    // UNIMPLEMENTED("not implemented");
    if (base_meta.is_deleted_ && undo_logs.empty()) {
      return std::nullopt;
    }
    Tuple res_tuple = base_tuple;
    bool is_deleted = false;
    for (const auto& undo_log : undo_logs) {
      // before delete a tuple in table heap, it has a full undoLog
      // (modified is all true) so ReplayUndoLog could handle base tuple is nullopt

      if (undo_log.is_deleted_) {
        // if it has delete undo log, then tuple becomes nullopt
        is_deleted = true;
      } else {
        is_deleted = false;
        // construct undo schema
        Schema undo_schema = GetUndoLogSchema(undo_log, schema);

        // construct new tuple
        std::vector<Value> values;
        uint32_t tuple_sz = schema->GetColumnCount();
        values.reserve(tuple_sz);
        for (uint32_t i = 0, j = 0; i < tuple_sz; ++i) {
          //如果有修改就从undologs中拿,反之就从base_tuple中拿
          if (undo_log.modified_fields_[i]) {
            values.push_back(undo_log.tuple_.GetValue(&undo_schema, j++));
          } else {
            values.push_back(res_tuple.GetValue(schema, i));
          }
        }
        res_tuple = Tuple(std::move(values), schema);
      }
    }
    if (is_deleted) {
      return std::nullopt;
    }
    return res_tuple;
  }

  auto ReplayUndoLog(const Schema* schema, const Tuple& base_tuple, const UndoLog& undo_log) -> std::optional<Tuple> {
    if (undo_log.is_deleted_) {
      return std::nullopt;
    }
    std::vector<Value> values;
    uint32_t tuple_sz = schema->GetColumnCount();

    // construct undo schema
    Schema undo_schema = GetUndoLogSchema(undo_log, schema);

    // construct new tuple
    values.reserve(tuple_sz);
    for (uint32_t i = 0, j = 0; i < tuple_sz; ++i) {
      if (undo_log.modified_fields_[i]) {
        values.push_back(undo_log.tuple_.GetValue(&undo_schema, j++));
      } else {
        values.push_back(base_tuple.GetValue(schema, i));
      }
    }
    Tuple res_tuple = Tuple(std::move(values), schema);
    return res_tuple;
  }
  /*
  // could handle deleted base tuple and log
  auto ReconstructTuple(const Schema *schema, const Tuple &base_tuple, const TupleMeta &base_meta,
                        const std::vector<UndoLog> &undo_logs) -> std::optional<Tuple> {
    // UNIMPLEMENTED("not implemented");
    if (base_meta.is_deleted_ && undo_logs.empty()) {
      return std::nullopt;
    }
    std::optional<Tuple> res_tuple = base_tuple;
    bool is_deleted = false;
    for (const auto &undo_log : undo_logs) {
      // before delete a tuple in table heap, it has a full undoLog
      // (modified is all true) so ReplayUndoLog could handle base tuple is nullopt
      auto temp_tuple = ReplayUndoLog(schema, *res_tuple, undo_log);
      if (!temp_tuple.has_value()) {
        is_deleted = true;
      } else {
        is_deleted = false;
        res_tuple = temp_tuple;
      }

      //////////////////////
      if (undo_log.is_deleted_) {
        // if it has delete undo log, then tuple becomes nullopt
        is_deleted = true;
      } else {
        is_deleted = false;
        // construct undo schema
        Schema undo_schema = GetUndoLogSchema(undo_log, schema);

        // construct new tuple
        std::vector<Value> values;
        uint32_t tuple_sz = schema->GetColumnCount();
        values.reserve(tuple_sz);
        for (uint32_t i = 0, j = 0; i < tuple_sz; ++i) {
          if (undo_log.modified_fields_[i]) {
            values.push_back(undo_log.tuple_.GetValue(&undo_schema, j++));
          } else {
            values.push_back(res_tuple.GetValue(schema, i));
          }
        }
        res_tuple = Tuple(std::move(values), schema);
      }
      ///////////////////////////

      // only debug for ReconstructTuple()
      // fmt::println(stderr, UndoLogTupleString(undo_log, schema));
    }
    //////////////////////////////
    if (is_deleted) {
      return std::nullopt;
    }
    //////////////////////////////////
    return is_deleted ? std::nullopt : res_tuple;
  }
  */
  //用于打印版本链的debug函数
  void TxnMgrDbg(const std::string& info, TransactionManager* txn_mgr, const TableInfo* table_info,
    TableHeap* table_heap) {
    // always use stderr for printing logs...
    fmt::println(stderr, "\033[36mTxnMgrDbg: start");
    fmt::println(stderr, "debug_hook: {}", info);
    // fmt::println(
    //    stderr,
    //    "You see this line of text because you have not implemented `TxnMgrDbg`. You should do this once you have "
    //    "finished task 2. Implementing this helper function will save you a lot of time for debugging in later
    //    tasks.");

    for (TableIterator i = table_heap->MakeIterator(); !i.IsEnd(); ++i) {
      auto [meta, tuple] = i.GetTuple();
      RID rid = tuple.GetRid();
      timestamp_t ts = meta.ts_;
      std::stringstream tuple_os;
      tuple_os << fmt::format("RID={}/{} ", rid.GetPageId(), rid.GetSlotNum());
      if ((ts & TXN_START_ID) != 0) {
        // ts is a txn id (还未提交的tuple)
        tuple_os << fmt::format("txn id = {} ", ts ^ TXN_START_ID);
      } else {
        // ts is timestamp (已经提交的tuple)
        tuple_os << fmt::format("timestamp={} ", ts);
      }

      // print tuple info
      if (meta.is_deleted_) {
        tuple_os << "<del marker> tuple=";
        tuple_os << tuple.ToString(&table_info->schema_);
        // tuple_os << GenerateNullTupleForSchema(&table_info->schema_).ToString(&table_info->schema_);
        /*
        bool is_first = true;
        for (uint32_t i = 0; i < table_info->schema_.GetColumnCount(); ++i) {
          if (is_first) {
            is_first = false;
          } else {
            tuple_os << ", ";
          }
          tuple_os << "<NULL>";
        }
        tuple_os << ")";
        */
      } else {
        tuple_os << "tuple=";
        tuple_os << tuple.ToString(&table_info->schema_);
      }
      fmt::println(stderr, tuple_os.str());

      // print version info
      std::optional<UndoLink> undo_link_optional = txn_mgr->GetUndoLink(rid);
      if (undo_link_optional.has_value()) {
        uint32_t n = 0;
        while (undo_link_optional.has_value() && undo_link_optional.value().IsValid()) {
          if (n > 20) {
            fmt::println(stderr, "dead loop");
            break;
          }
          std::optional<UndoLog> undo_log_optional = txn_mgr->GetUndoLogOptional(undo_link_optional.value());
          if (undo_log_optional.has_value() && undo_log_optional.value().ts_ != INVALID_TS) {
            std::stringstream version_os;
            version_os << fmt::format("  txn{}@{} ", undo_link_optional.value().prev_txn_ ^ TXN_START_ID, n);
            if (undo_log_optional.value().is_deleted_) {
              version_os << "<del>";
            } else {
              ++n;
              version_os << UndoLogTupleString(undo_log_optional.value(), &table_info->schema_);
            }
            version_os << fmt::format(" ts={}", undo_log_optional.value().ts_);
            fmt::println(stderr, version_os.str());

            undo_link_optional = undo_log_optional.value().prev_version_;
          } else {
            break;
          }
        }
      }
    }
    // We recommend implementing this function as traversing the table heap and print the version chain. An example
    // output of our reference solution:
    //
    // debug_hook: before verify scan
    // RID=0/0 ts=txn8 tuple=(1, <NULL>, <NULL>)
    //   txn8@0 (2, _, _) ts=1
    // RID=0/1 ts=3 tuple=(3, <NULL>, <NULL>)
    //   txn5@0 <del> ts=2
    //   txn3@0 (4, <NULL>, <NULL>) ts=1
    // RID=0/2 ts=4 <del marker> tuple=(<NULL>, <NULL>, <NULL>)
    //   txn7@0 (5, <NULL>, <NULL>) ts=3
    // RID=0/3 ts=txn6 <del marker> tuple=(<NULL>, <NULL>, <NULL>)
    //   txn6@0 (6, <NULL>, <NULL>) ts=2
    //   txn3@1 (7, _, _) ts=1
    fmt::println(stderr, "\033[36mTxnMgrDbg: end\033[0m");
  }

  auto GetUndoLogSchema(const UndoLog& undo_log, const Schema* schema) -> Schema {
    std::vector<uint32_t> cols;
    uint32_t tuple_sz = schema->GetColumnCount();
    for (uint32_t i = 0; i < tuple_sz; ++i) {
      if (undo_log.modified_fields_[i]) {
        cols.push_back(i);
      }
    }
    return bustub::Schema::CopySchema(schema, cols);
  }

  auto UndoLogTupleString(const UndoLog& undo_log, const Schema* schema) -> std::string {
    std::stringstream os;
    Schema undolog_schema = GetUndoLogSchema(undo_log, schema);
    bool is_first = true;

    if (undo_log.is_deleted_) {
      /*
      for (uint32_t i = 0; i < schema->GetColumnCount(); ++i) {
        if (is_first) {
          is_first = false;
        } else {
          os << ", ";
        }
        os << "<NULL>";
      }
      */
      os << GenerateNullTupleForSchema(&undolog_schema).ToString(&undolog_schema);
    } else {
      os << "(";
      for (uint32_t i = 0, j = 0; i < schema->GetColumnCount(); ++i) {
        if (is_first) {
          is_first = false;
        } else {
          os << ", ";
        }

        if (undo_log.modified_fields_[i]) {
          if (undo_log.tuple_.IsNull(&undolog_schema, j)) {
            os << "<NULL>";
          } else {
            os << undo_log.tuple_.GetValue(&undolog_schema, j).ToString();
          }
          ++j;
        } else {
          os << "_";
        }
      }
      os << ")";
    }
    return os.str();
  }

  auto GenerateNullTupleForSchema(const Schema* schema) -> Tuple {
    std::vector<Value> values;
    for (uint32_t i = 0; i < schema->GetColumnCount(); ++i) {
      values.push_back(ValueFactory::GetNullValueByType((schema->GetColumn(i)).GetType()));
    }
    return { values, schema };
  }

  // 更新/删除操作中 根据当前tulpe的undolog生成新的tuple
  /* undolog:
      is_deleted_
      modified_fields_
      tuple_
      ts_
      prev_version_
   */
  auto GenerateDiffLog(const Tuple& old_tuple, const TupleMeta& old_tuple_meta, const Tuple& new_tuple, const TupleMeta& new_tuple_meta, const Schema* schema) -> UndoLog {
    uint32_t sz = schema->GetColumnCount();
    std::vector<Value> values;
    UndoLog undo_log;
    undo_log.ts_ = old_tuple_meta.ts_;
    // 老的tuple已经是删除状态了 后一次操作直接返回时间戳和删除标记
    if (old_tuple_meta.is_deleted_) {
      undo_log.is_deleted_ = true;
      return undo_log;
    }
    //老的tuple不是被删除状态
    undo_log.is_deleted_ = false;
    //新操作是一个删除操作,undolog要记录原来的tuple的所有值
    if (new_tuple_meta.is_deleted_) {
      undo_log.modified_fields_ = std::vector<bool>(sz, true);
      for (uint32_t i = 0; i < sz; ++i) {
        values.push_back(old_tuple.GetValue(schema, i));
      }
      undo_log.tuple_ = Tuple(std::move(values), schema);
    } else {
      //新老tuple都不是被删除状态
      //合并两次tuple的更改
      undo_log.modified_fields_ = std::vector<bool>(sz, false);
      for (uint32_t i = 0; i < sz; ++i) {
        if (!(old_tuple.GetValue(schema, i).CompareExactlyEquals(new_tuple.GetValue(schema, i)))) {
          values.push_back(old_tuple.GetValue(schema, i));
          undo_log.modified_fields_[i] = true;
        }
      }
      Schema undo_log_schema = GetUndoLogSchema(undo_log, schema);
      undo_log.tuple_ = Tuple(std::move(values), &undo_log_schema);
    }
    return undo_log;
  }

  auto IsWriteWriteConflict(const Transaction* txn, const TupleMeta& meta) -> bool {
    if (meta.ts_ >= TXN_START_ID && txn->GetTransactionTempTs() != meta.ts_) {
      return true;
    }
    if (meta.ts_ < TXN_START_ID && meta.ts_ > txn->GetReadTs()) {
      return true;
    }
    return false;
  }

  // schema is origin tuple schema
  auto MergeUndoLog(const UndoLog& new_undo_log, const UndoLog& origin_undo_log, const Schema* schema) -> UndoLog {
    // if last UndoLog is_deleted, don't need merge, return is_deleted empty UndoLog
    if (origin_undo_log.is_deleted_) {
      return origin_undo_log;
    }
    UndoLog res = origin_undo_log;
    Schema new_undo_log_schema = GetUndoLogSchema(new_undo_log, schema);
    Schema origin_undo_log_schema = GetUndoLogSchema(origin_undo_log, schema);
    uint32_t sz = schema->GetColumnCount();
    std::vector<Value> values;
    for (uint32_t i = 0, j = 0, k = 0; i < sz; ++i) {
      if ((!new_undo_log.modified_fields_[i] && origin_undo_log.modified_fields_[i]) ||
        (new_undo_log.modified_fields_[i] && !origin_undo_log.modified_fields_[i])) {
        res.modified_fields_[i] = true;
        values.push_back(new_undo_log.modified_fields_[i]
          ? new_undo_log.tuple_.GetValue(&new_undo_log_schema, j++)
          : origin_undo_log.tuple_.GetValue(&origin_undo_log_schema, k++));
      } else if (new_undo_log.modified_fields_[i] && origin_undo_log.modified_fields_[i]) {
        values.push_back(origin_undo_log.tuple_.GetValue(&origin_undo_log_schema, k));
        ++j;
        ++k;
      }
    }
    Schema res_schema = GetUndoLogSchema(res, schema);
    res.tuple_ = Tuple(std::move(values), &res_schema);
    return res;
  }

  auto LockVersionLink(RID rid, TransactionManager* txn_mgr) -> bool {
    std::optional<VersionUndoLink> version_link_optional = txn_mgr->GetVersionLink(rid);
    if (version_link_optional.has_value()) {
      return txn_mgr->UpdateVersionLink(
        rid, VersionUndoLink{ version_link_optional->prev_, true },
        [version_link_optional](std::optional<VersionUndoLink> origin_version_link_optional) -> bool {
          // version_link_optional->prev_ == origin_version_link_optional->prev_ in case TOCTTOU problem
          // at first line we get version link, and copy it later in UpdateVersionLink
          // maybe during this time another txn update this tuple and commit, then the version link prev is invalid
          return (!(origin_version_link_optional->in_progress_) &&
            (origin_version_link_optional->prev_ == version_link_optional->prev_));
        });
    }
    return txn_mgr->UpdateVersionLink(rid, VersionUndoLink{ UndoLink(), true },
      [](std::optional<VersionUndoLink> origin_version_link_optional) {
        return !origin_version_link_optional.has_value();
      });
  }

  auto UnlockVersionLink(RID rid, TransactionManager* txn_mgr) -> bool {
    std::optional<VersionUndoLink> version_link_optional = txn_mgr->GetVersionLink(rid);
    if (version_link_optional.has_value()) {
      return txn_mgr->UpdateVersionLink(
        rid, VersionUndoLink{ version_link_optional->prev_, true },
        [version_link_optional](std::optional<VersionUndoLink> origin_version_link_optional) -> bool {
          return (!origin_version_link_optional->in_progress_ &&
            version_link_optional->prev_ == origin_version_link_optional->prev_);
        });
    }
    return false;
  }

  auto SetInProgress(RID rid, TransactionManager* txn_mgr) -> bool {
    std::unique_lock<std::shared_mutex> lck(txn_mgr->version_info_mutex_);
    std::shared_ptr<TransactionManager::PageVersionInfo> pg_ver_info = nullptr;
    auto iter = txn_mgr->version_info_.find(rid.GetPageId());
    pg_ver_info = iter->second;
    std::unique_lock<std::shared_mutex> lck2(pg_ver_info->mutex_);
    lck.unlock();
    auto iter2 = pg_ver_info->prev_version_.find(rid.GetSlotNum());
    if (iter2->second.in_progress_) {
      return false;
    }
    iter2->second.in_progress_ = true;
    return true;
  }

  void UnsetInProgress(RID rid, TransactionManager* txn_mgr) {
    std::unique_lock<std::shared_mutex> lck(txn_mgr->version_info_mutex_);
    std::shared_ptr<TransactionManager::PageVersionInfo> pg_ver_info = nullptr;
    auto iter = txn_mgr->version_info_.find(rid.GetPageId());
    pg_ver_info = iter->second;
    std::unique_lock<std::shared_mutex> lck2(pg_ver_info->mutex_);
    lck.unlock();
    auto iter2 = pg_ver_info->prev_version_.find(rid.GetSlotNum());
    iter2->second.in_progress_ = false;
  }

  void LockAndCheck(RID rid, TransactionManager* txn_mgr, Transaction* txn, const TableInfo* table_info) {
    // if not self-modification and can't lock
    // is modifying by other txn
    if (!LockVersionLink(rid, txn_mgr)) {
      // txn_mgr->Abort(txn);
      // std::cerr << "    LockVersionLink failed" << std::endl;
      MyAbort(txn);
      // throw ExecutionException("Abort");
    }
    // IsWWconflict in case tuple and meta hasn't been modified
    // don't have lock until now
    if (IsWriteWriteConflict(txn, table_info->table_->GetTupleMeta(rid))) {
      // txn_mgr->Abort(txn);
      // std::cerr << "    IsWriteWriteConflict failed" << std::endl;

      // if has been locked, must unset in_progress before abort
      auto version_link_optional = txn_mgr->GetVersionLink(rid);
      txn_mgr->UpdateVersionLink(rid, VersionUndoLink{ version_link_optional->prev_, false }, nullptr);

      MyAbort(txn);
      // throw ExecutionException("Abort");
    }
  }

  void MyAbort(Transaction* txn) {
    txn->SetTainted();
    throw ExecutionException("abort");
  }

  void UpdateTuple(const TableInfo* table_info, const Schema* schema, TransactionManager* txn_mgr, Transaction* txn, TupleMeta old_tuple_meta, Tuple& old_tuple, TupleMeta new_tuple_meta, Tuple new_tuple, RID rid) {
    // if self-modification
    if (old_tuple_meta.ts_ == txn->GetTransactionTempTs()) {
      std::optional<UndoLink> undo_link_optional = txn_mgr->GetUndoLink(rid);
      //有undolog
      if (undo_link_optional.has_value()) {
        UndoLog temp_undo_log = GenerateDiffLog(old_tuple, old_tuple_meta, new_tuple, new_tuple_meta, schema);

        UndoLog old_undo_log = txn_mgr->GetUndoLogOptional(undo_link_optional.value()).value();

        temp_undo_log.prev_version_ = old_undo_log.prev_version_;
        UndoLog merged_undo_log = MergeUndoLog(temp_undo_log, old_undo_log, schema);
        txn->ModifyUndoLog(undo_link_optional.value().prev_log_idx_, merged_undo_log);
      }
    } else {
      // LockAndCheck(rid, txn_mgr, txn, table_info);
      //判断写写冲突
      if (IsWriteWriteConflict(txn, table_info->table_->GetTupleMeta(rid))) {
        auto version_link_optional = txn_mgr->GetVersionLink(rid);
        txn_mgr->UpdateVersionLink(rid, VersionUndoLink{ version_link_optional->prev_, false }, nullptr);
        MyAbort(txn);
        return;
      }
      std::optional<UndoLink> undo_link_optional = txn_mgr->GetUndoLink(rid);
      // 生成新的undo log
      UndoLog new_undo_log = GenerateDiffLog(old_tuple, old_tuple_meta, new_tuple, new_tuple_meta, schema);
      new_undo_log.prev_version_ = *undo_link_optional;
      UndoLink new_undo_link = txn->AppendUndoLog(new_undo_log);
      txn_mgr->UpdateVersionLink(rid, VersionUndoLink{ new_undo_link, true }, nullptr);
    }
    // if is self_modification and have no version link(inserted by this txn)
    // only update table heap
    table_info->table_->UpdateTupleInPlace(new_tuple_meta, new_tuple, rid, nullptr);
    // std::cerr << "tuple rid  " << temp_rid << " append to write set " << std::endl;
    txn->AppendWriteSet(table_info->oid_, rid);

  }
  void DeleteTuple(const TableInfo* table_info, const Schema* schema, TransactionManager* txn_mgr, Transaction* txn, TupleMeta old_tuple_meta, Tuple& delete_tuple, RID rid) {


    auto new_tuple_meta = TupleMeta{ txn->GetTransactionTempTs(), true };
    //找到tuple的undolink
    std::optional<UndoLink> undo_link_optional = txn_mgr->GetUndoLink(rid);
    //如果要删除的tuple是事务自己产生的(self-modification)
    if (old_tuple_meta.ts_ == txn->GetTransactionTempTs()) {

      //undolink不为空,说明tuple不是本事务插入的,但本事务已经做过了修改,需要生产新的undolog替换旧的undolog(合并两次修改)
      if (undo_link_optional->IsValid()) {
        UndoLog temp_undo_log = GenerateDiffLog(delete_tuple, old_tuple_meta, Tuple{}, new_tuple_meta, schema);
      }
      //undolink为空,说明是本事务新插入的tuple,别的事务是一定看不见的直接删除就好了
    } else {
      //no self-modification
      //在这里判断加锁和写写冲突
      // LockAndCheck(rid, txn_mgr, txn, table_info);
      if (IsWriteWriteConflict(txn, table_info->table_->GetTupleMeta(rid))) {
        auto version_link_optional = txn_mgr->GetVersionLink(rid);
        txn_mgr->UpdateVersionLink(rid, VersionUndoLink{ version_link_optional->prev_, false }, nullptr);
        MyAbort(txn);
        return;
      }
      std::optional<UndoLink> undo_link_optional = txn_mgr->GetUndoLink(rid);
      //生成新的new undo log
      UndoLog new_undo_log = GenerateDiffLog(delete_tuple, old_tuple_meta, Tuple{}, new_tuple_meta, schema);
      if (undo_link_optional.has_value()) {
        new_undo_log.prev_version_ = undo_link_optional.value();
      }
      UndoLink new_undo_link = txn->AppendUndoLog(new_undo_log);
      txn_mgr->UpdateVersionLink(rid, VersionUndoLink{ new_undo_link, true }, nullptr);
    }

    // delete tuple in table heap
    table_info->table_->UpdateTupleMeta(new_tuple_meta, rid);
    txn->AppendWriteSet(table_info->oid_, rid);
  }



  void InsertTuple(const IndexInfo* primary_key_idx_info, const TableInfo* table_info, TransactionManager* txn_mgr,
    Transaction* txn, LockManager* lock_mgr, Tuple& child_tuple, const Schema* output_schema) {
    auto new_tuple_meta = TupleMeta{ txn->GetTransactionTempTs(), false };
    // check the primary index first
    std::vector<RID> res;
    // index operation is thread safe
    primary_key_idx_info->index_->ScanKey(child_tuple.KeyFromTuple(table_info->schema_, primary_key_idx_info->key_schema_,
      primary_key_idx_info->index_->GetKeyAttrs()),
      &res, txn);
    // std::cerr << "    entre InsertTuple()" << std::endl;
    // if primary key already existed
    if (!res.empty()) {
      // std::cerr << "  a txn enter this condition primary key is existed" << std::endl;
      RID target_rid = res.front();
      const auto& [target_meta, target_tuple] = table_info->table_->GetTuple(target_rid);
      if (!target_meta.is_deleted_) {
        // txn_mgr->Abort(txn);
        // std::cerr << "    " << child_tuple.ToString(output_schema)
        //          << " inserted into table heap failed because tuple is not deleted" << std::endl;
        MyAbort(txn);
      } else {
        // if is deleted by other txn and has commited
        // construct new delete undoLog
        // else is self modification(is deleted by this txn before), update in place directly
        if (target_meta.ts_ != txn->GetTransactionTempTs()) {
          // std::cerr << "  a txn enter this condition before LockAndCheck()" << std::endl;
          LockAndCheck(target_rid, txn_mgr, txn, table_info);
          // std::cerr << "a txn enter this condition before GenerateDiffLog()" << std::endl;
          UndoLog temp_undo_log = GenerateDiffLog(target_tuple, target_meta, Tuple{}, new_tuple_meta, output_schema);
          auto version_link_optional = txn_mgr->GetVersionLink(target_rid);
          temp_undo_log.prev_version_ = version_link_optional->prev_;

          UndoLink new_undo_link = txn->AppendUndoLog(temp_undo_log);
          txn_mgr->UpdateVersionLink(target_rid, VersionUndoLink{ new_undo_link, true });
          // Add tuple rid to txn's write set
          txn->AppendWriteSet(table_info->oid_, target_rid);
        }
        table_info->table_->UpdateTupleInPlace(new_tuple_meta, child_tuple, target_rid);
        // std::cerr << "    " << child_tuple.ToString(output_schema) << " inserted into table heap" << std::endl;
      }
    } else {
      // std::cerr << "  a txn enter this condition primary key don't exist" << std::endl;
      // insert tuple into table heap directly, not change schema,
      // because The planner will ensure that the values have the same schema as the table
      // insert into table heap is thread safe
      auto rid_optional = table_info->table_->InsertTuple(new_tuple_meta, child_tuple, lock_mgr, txn, table_info->oid_);
      // modify primary key index
      // insert into primary key is thread safe and can maintain unique constraint
      bool is_inserted = primary_key_idx_info->index_->InsertEntry(
        child_tuple.KeyFromTuple(table_info->schema_, primary_key_idx_info->key_schema_,
          primary_key_idx_info->index_->GetKeyAttrs()),
        *rid_optional, txn);
      // if primary key already existed in primary index
      if (!is_inserted) {
        // std::cerr << "    " << child_tuple.ToString(output_schema)
        //          << " inserted into table heap failed because primary key is existed" << std::endl;
        LockVersionLink(*rid_optional, txn_mgr);
        txn->AppendWriteSet(table_info->oid_, *rid_optional);
        // txn_mgr->Abort(txn);
        MyAbort(txn);
        // throw ExecutionException("Abort");
      } else {
        // std::cerr << "befor update undolink" << std::endl;
        // update txn_mgr_ version info map
        txn_mgr->UpdateUndoLink(*rid_optional, std::nullopt);
        // std::cerr << "after update undolink" << std::endl;
        // std::cerr << "    " << child_tuple.ToString(output_schema) << " inserted into table heap" << std::endl;
        LockVersionLink(*rid_optional, txn_mgr);
        // std::cerr << "after lock undolink" << std::endl;
        // Add tuple rid to txn's write set
        txn->AppendWriteSet(table_info->oid_, *rid_optional);
      }
    }
  }

  auto CheckOverlap(const std::vector<AbstractExpressionRef>& predicates, const Tuple* tuple, const Schema& schema)
    -> bool {
    for (const auto& predicate : predicates) {
      // std::cerr << "predicate value is " << predicate->Evaluate(tuple, schema).ToString() << std::endl;
      if (predicate->Evaluate(tuple, schema).CompareExactlyEquals(ValueFactory::GetBooleanValue(true))) {
        // std::cerr << " CheckOverlap endter true case" << std::endl;
        return true;
      }
    }
    // std::cerr << " CheckOverlap endter false case" << std::endl;
    return false;
  }
}  // namespace bustub
