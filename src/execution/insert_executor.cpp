//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

    InsertExecutor::InsertExecutor(ExecutorContext* exec_ctx, const InsertPlanNode* plan,
        std::unique_ptr<AbstractExecutor>&& child_executor)
        : AbstractExecutor(exec_ctx) {
        this->plan_ = plan;
        this->child_executor_ = std::move(child_executor);
    }

    void InsertExecutor::Init() {
        this->child_executor_->Init();
        this->has_insert_ = false;

    }

    auto InsertExecutor::Next([[maybe_unused]] Tuple* tuple, RID* rid) -> bool {
        if (has_insert_) {
            return false;
        }
        has_insert_ = true;
        auto table_info = this->exec_ctx_->GetCatalog()->GetTable(this->plan_->GetTableOid());
        //和这个表有关的所有index
        auto index_infos = this->exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_);
        int cnt = 0;
        while (this->child_executor_->Next(tuple, rid)) {
            //next返回true才是找到了这个节点目标的节点
            cnt++;
            table_info->table_->InsertTuple(TupleMeta{ 0, false }, *tuple);
            //修改index
            for (auto& info : index_infos) {
                //建立在tuple哪一个key上的index
                auto key = tuple->KeyFromTuple(table_info->schema_, info->key_schema_, info->index_->GetKeyAttrs());
                info->index_->InsertEntry(key, *rid, this->exec_ctx_->GetTransaction());
            }
        }
        *tuple = Tuple{ {{TypeId::INTEGER,cnt}},&GetOutputSchema() };
        return true;
    }

}  // namespace bustub
