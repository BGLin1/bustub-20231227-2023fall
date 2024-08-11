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
        while (rid_iter_ != rids_.end()) {
            const auto next_rid = *rid_iter_;
            const auto [next_base_meta, next_base_tuple] = table_heap_->GetTuple(next_rid);
            rid_iter_++;
            //判断当前tuple是否满足plan中的筛选条件
            if (plan_->filter_predicate_ && !plan_->filter_predicate_->Evaluate(&next_base_tuple, plan_->OutputSchema()).GetAs<bool>()) {
                LOG_TRACE("Failed to get tuple %s due to dissatisfy conditions", next_tuple.ToString(&output_schema).c_str());
                continue;
            }
            // 判断当前tuple对是否满足事务可见性
            const auto reconstructed_tuple = RetrieveTuple(next_base_tuple, next_base_meta, next_rid, plan_->OutputSchema());
            if (!reconstructed_tuple.has_value()) {
                continue;
            }
            *tuple = *reconstructed_tuple;
            *rid = next_rid;
            LOG_TRACE("Succeed in getting tuple %s, RID %s in sequential scan.", tuple->ToString(&output_schema).c_str(),
                rid->ToString().c_str());
            return true;
        }
        return false;
    }

    auto SeqScanExecutor::RetrieveTuple(const Tuple& next_base_tuple, const TupleMeta& next_base_meta, const RID& next_rid, const Schema& schema) const -> std::optional<Tuple> {

        auto* transaction_manager = exec_ctx_->GetTransactionManager();
        const auto* transaction = exec_ctx_->GetTransaction();
        //tuple的时间戳等于当前事务id 或时间戳小于当前读时间戳
        if (transaction->GetReadTs() >= next_base_meta.ts_ || transaction->GetTransactionTempTs() == next_base_meta.ts_) {
            //再判断当前tuple是不是被删除的
            if (next_base_meta.is_deleted_) {
                return std::nullopt;
            } else {
                return next_base_tuple;
            }
        }
        //如果不满足 进入 从undolog中找满足的tuple
        std::vector<UndoLog> undo_logs;
        std::optional<UndoLink> undo_link_optional = this->exec_ctx_->GetTransactionManager()->GetUndoLink(next_rid);
        if (undo_link_optional.has_value()) {
            while (undo_link_optional.has_value() && undo_link_optional.value().IsValid()) {
                const auto undo_log_optional = transaction_manager->GetUndoLogOptional(undo_link_optional.value());
                if (undo_log_optional.has_value()) {
                    auto undo_log = undo_log_optional.value();
                    if (undo_log.ts_ >= transaction->GetReadTs()) {
                        //找到第一个时间戳小于当前读时间戳的log
                        undo_logs.push_back(undo_log);
                    } else {
                        break;
                    }
                    undo_link_optional = undo_log_optional.value().prev_version_;
                } else {
                    break;
                }


            }
        }

        if (undo_logs.empty()) {
            return std::nullopt;
        }
        return ReconstructTuple(&schema, next_base_tuple, next_base_meta, undo_logs);
    }
}  // namespace bustub
