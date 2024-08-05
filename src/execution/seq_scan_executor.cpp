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
        std::pair<TupleMeta, Tuple> cur_rid;
        do {
            if (rid_iter_ == rids_.end()) {
                //喷发完了
                return false;
            }
            cur_rid = table_heap_->GetTuple(*rid_iter_);
            //检测获取到的tuple是不是被删除的
            if (!cur_rid.first.is_deleted_) {
                //把tuple喷发出去
                *tuple = cur_rid.second;
                *rid = *rid_iter_;
            }
            ++rid_iter_;
            //每次喷发有效数据就退出循环
            //数据无效的定义是:is_deleted_为真 或者在存在filter_predicate_前提下  当前的tuple不满足plan中的筛选条件
        } while (cur_rid.first.is_deleted_ || (plan_->filter_predicate_ && !plan_->filter_predicate_
                ->Evaluate(tuple, plan_->OutputSchema()).GetAs<bool>()));
       
        return true;
    }

}  // namespace bustub
