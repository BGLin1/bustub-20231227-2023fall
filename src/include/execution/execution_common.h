#pragma once

#include <string>
#include <vector>

#include "catalog/catalog.h"
#include "catalog/schema.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include "storage/table/tuple.h"

namespace bustub {

auto ReplayUndoLog(const Schema *schema, const Tuple &base_tuple, const UndoLog &undo_log) -> std::optional<Tuple>;

auto ReconstructTuple(const Schema *schema, const Tuple &base_tuple, const TupleMeta &base_meta,
                      const std::vector<UndoLog> &undo_logs) -> std::optional<Tuple>;

void TxnMgrDbg(const std::string &info, TransactionManager *txn_mgr, const TableInfo *table_info,
               TableHeap *table_heap);

auto GetUndoLogSchema(const UndoLog &undo_log, const Schema *schema) -> Schema;

// get tuple string according to undolog, such as (1, _, NULL)
// could used in TxnMgrDbg() and ReconstructTuple()
auto UndoLogTupleString(const UndoLog &undo_log, const Schema *schema) -> std::string;

auto GenerateNullTupleForSchema(const Schema *schema) -> Tuple;

// if tuple is deleted in table heap, generate empty undo log(only set is_deleted_)
// if tuple is delete, generate all tuple value in tuple_
// else only generate different value in tuple
auto GenerateDiffLog(const Tuple &old_tuple, const TupleMeta &old_tuple_meta, const Tuple &new_tuple,
                     const TupleMeta &new_tuple_meta, const Schema *schema) -> UndoLog;

auto IsWriteWriteConflict(const Transaction *txn, const TupleMeta &meta) -> bool;

auto MergeUndoLog(const UndoLog &new_undo_log, const UndoLog &origin_undo_log, const Schema *schema) -> UndoLog;

// check in_progress and set in progress
// return whether succeed
auto LockVersionLink(RID rid, TransactionManager *txn_mgr) -> bool;

void LockAndCheck(RID rid, TransactionManager *txn_mgr, Transaction *txn, const TableInfo *table_info);
// set in_progress atomically
// if success return true, otherwise return false
auto SetInProgress(RID rid, TransactionManager *txn_mgr) -> bool;
void UnsetInProgress(RID rid, TransactionManager *txn_mgr);

void MyAbort(Transaction *txn);

void DeleteTuple(const TableInfo *table_info, const Schema *schema, TransactionManager *txn_mgr, Transaction *txn,
                 TupleMeta old_tuple_meta, Tuple &delete_tuple, RID rid);

void InsertTuple(const IndexInfo *primary_key_idx_info, const TableInfo *table_info, TransactionManager *txn_mgr,
    Transaction* txn, LockManager* lock_mgr, Tuple& child_tuple, const Schema* output_schema);

void UpdateTuple(const TableInfo* table_info, const Schema* schema, TransactionManager* txn_mgr, Transaction* txn, TupleMeta old_tuple_meta, Tuple& old_tuple, TupleMeta new_tuple_meta, Tuple new_Tuple, RID rid);

// check is write set is overlap with read set
// true is overlap, should abort in serializable check
auto CheckOverlap(const std::vector<AbstractExpressionRef> &predicate, const Tuple *tuple, const Schema &schema)
    -> bool;
// Add new functions as needed... You are likely need to define some more functions.
//
// To give you a sense of what can be shared across executors / transaction manager, here are the
// list of helper function names that we defined in the reference solution. You should come up with
// your own when you go through the process.
// * CollectUndoLogs
// * WalkUndoLogs
// * Modify
// * IsWriteWriteConflict
// * GenerateDiffLog
// * GenerateNullTupleForSchema
// * GetUndoLogSchema
//
// We do not provide the signatures for these functions because it depends on the your implementation
// of other parts of the system. You do not need to define the same set of helper functions in
// your implementation. Please add your own ones as necessary so that you do not need to write
// the same code everywhere.

}  // namespace bustub
