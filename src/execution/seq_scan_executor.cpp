//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"
#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"

#include "unistd.h"

namespace bustub {

    SeqScanExecutor::SeqScanExecutor(ExecutorContext* exec_ctx, const SeqScanPlanNode* plan) : AbstractExecutor(exec_ctx) {
        this->plan_ = plan;
    }

    void SeqScanExecutor::Init() {
        this->table_heap_ = GetExecutorContext()->GetCatalog()->GetTable(plan_->GetTableOid())->table_.get();
        auto it = this->table_heap_->MakeIterator();
        this->rids_.clear();
        for (;!it.IsEnd();++it) {
            this->rids_.emplace_back(it.GetRID());
        }
        rid_iter_ = rids_.begin();
    }

    auto SeqScanExecutor::Next(Tuple* tuple, RID* rid) -> bool {
        //p3:没有引入事务的代码
    //   std::pair<TupleMeta, Tuple> cur_rid;
    //      do {
    //         if (rid_iter_ == rids_.end()) {
    //             //喷发完了
    //             return false;
    //         }
    //         cur_rid = table_heap_->GetTuple(*rid_iter_);
    //         //检测获取到的tuple是不是被删除的
    //         if (!cur_rid.first.is_deleted_) {
    //             //把tuple喷发出去
    //             *tuple = cur_rid.second;
    //             *rid = *rid_iter_;
    //         }
    //         ++rid_iter_;
    //         //每次喷发有效数据就退出循环
    //         //数据无效的定义是:is_deleted_为真 或者在存在filter_predicate_前提下  当前的tuple不满足plan中的筛选条件
    //     } while (cur_rid.first.is_deleted_ || (plan_->filter_predicate_ && !plan_->filter_predicate_
    //             ->Evaluate(tuple, plan_->OutputSchema()).GetAs<bool>()));

    //     return true; 
        // p4:引入事务的代码

        bool is_find = false;
        do {
            is_find = false;
            if (this->rid_iter_ == this->rids_.end()) {
                return false;
            }
            const std::pair<TupleMeta, Tuple>& tuple_pair = table_heap_->GetTuple(*rid_iter_);
            auto txn = this->exec_ctx_->GetTransaction();
            auto txn_mgr = this->exec_ctx_->GetTransactionManager();
            //tuple是当前事务临时插入的tuple
            if (tuple_pair.first.ts_ == txn->GetTransactionTempTs()) {
                if (tuple_pair.first.is_deleted_) {
                    // is delete by this txn, can't read by this txn
                    rid_iter_++;
                    continue;
                }
                // is modifying by this txn
                *tuple = tuple_pair.second;
                *rid = tuple->GetRid();
                is_find = true;
            } else {
                //tuple不是当前事务临时插入的tuple 
                timestamp_t txn_ts = txn->GetReadTs();
                timestamp_t tuple_ts = tuple_pair.first.ts_;
                std::vector<UndoLog> undo_logs;

                // tuple的时间戳大于事务时间戳 需要检查undolog
                if (txn_ts < tuple_ts) {
                    std::optional<UndoLink> undo_link_optional = txn_mgr->GetUndoLink(tuple_pair.second.GetRid());
                    std::optional<UndoLog> undo_log_optional = txn_mgr->GetUndoLogOptional(undo_link_optional.value());
                    if (undo_link_optional.has_value()) {
                        while (undo_log_optional.has_value() && undo_link_optional.value().IsValid()) {
                            undo_log_optional = txn_mgr->GetUndoLogOptional(undo_link_optional.value());
                            if (undo_log_optional.has_value()) {
                                //找到第一个tuple时间戳小于等于事务时间戳的
                                if (txn_ts >= undo_log_optional.value().ts_) {
                                    undo_logs.push_back(std::move(undo_log_optional.value()));
                                    //根据undologs重构tuple
                                    std::optional<Tuple> res_tuple_optional =
                                        ReconstructTuple(&GetOutputSchema(), tuple_pair.second, tuple_pair.first, undo_logs);
                                    if (res_tuple_optional.has_value()) {
                                        *tuple = res_tuple_optional.value();
                                        *rid = tuple_pair.second.GetRid();
                                        is_find = true;
                                    }
                                    break;
                                }
                                //否则把当前的log插入undolog中
                                undo_logs.push_back(std::move(undo_log_optional.value()));
                                undo_link_optional = undo_log_optional.value().prev_version_;
                            }

                        }
                    }
                } else {
                    // return tuple in table heap directly
                    if (!tuple_pair.first.is_deleted_) {
                        is_find = true;
                        *tuple = tuple_pair.second;
                        *rid = tuple->GetRid();
                    }
                }
            }
            rid_iter_++;
        } while (!is_find || (plan_->filter_predicate_ &&
            !(plan_->filter_predicate_->Evaluate(tuple, plan_->OutputSchema()).GetAs<bool>())));
        return true;
    }
}  // namespace bustub

