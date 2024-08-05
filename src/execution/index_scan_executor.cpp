//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
    IndexScanExecutor::IndexScanExecutor(ExecutorContext* exec_ctx, const IndexScanPlanNode* plan)
        : AbstractExecutor(exec_ctx) {
        this->plan_ = plan;
    }

    void IndexScanExecutor::Init() {
        this->rids_.clear();
        this->has_scan_ = false;
        auto table_info = this->exec_ctx_->GetCatalog()->GetTable(this->plan_->table_oid_);
        this->table_heap_ = table_info->table_.get();

        auto index_info = this->exec_ctx_->GetCatalog()->GetIndex(this->plan_->GetIndexOid());
        this->hash_table_ = dynamic_cast<HashTableIndexForTwoIntegerColumn*>(index_info->index_.get());
        //创建索引键
        Tuple index_key{ {this->plan_->pred_key_->val_} ,&index_info->key_schema_ };
        //用索引把所有找到的结果放在rids中
        this->hash_table_->ScanKey(index_key, &rids_, this->exec_ctx_->GetTransaction());
    }

    auto IndexScanExecutor::Next(Tuple* tuple, RID* rid) -> bool {
        if (has_scan_) {
            return false;
        }
        has_scan_ = true;
        if (rids_.empty()) {
            return false;
        }
        TupleMeta meta{};
        meta = table_heap_->GetTuple(*rids_.begin()).first;
        // 确保索引到的元组是没有被删除的
        if (!meta.is_deleted_) {
            *tuple = table_heap_->GetTuple(*rids_.begin()).second;
            *rid = *rids_.begin();
        }
        return true;

    }

}  // namespace bustub
