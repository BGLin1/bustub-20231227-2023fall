//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

    DeleteExecutor::DeleteExecutor(ExecutorContext* exec_ctx, const DeletePlanNode* plan,
        std::unique_ptr<AbstractExecutor>&& child_executor)
        : AbstractExecutor(exec_ctx) {
        this->plan_ = plan;
        this->child_executor_ = std::move(child_executor);

    }

    void DeleteExecutor::Init() {
        this->child_executor_->Init();
        this->has_deleted_ = false;
    }

    auto DeleteExecutor::Next([[maybe_unused]] Tuple* tuple, RID* rid) -> bool {
        if (has_deleted_) {
            return false;
        }
        has_deleted_ = true;
        auto table_info = this->exec_ctx_->GetCatalog()->GetTable(this->plan_->GetTableOid());
        auto index_infos = this->exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_);
        int cnt = 0;
        while (this->child_executor_->Next(tuple, rid)) {
            //next返回true才是找到了这个节点目标的节点
            cnt++;
            //通过修改meta标记为已删除
            table_info->table_->UpdateTupleMeta(TupleMeta{ 0,true }, *rid);
            //修改index
            for (auto& info : index_infos) {
                auto key = tuple->KeyFromTuple(table_info->schema_, info->key_schema_, info->index_->GetKeyAttrs());
                info->index_->DeleteEntry(key, *rid, this->exec_ctx_->GetTransaction());
            }
        }
        *tuple = Tuple{ {{TypeId::INTEGER,cnt}},&GetOutputSchema() };
        return true;

    }

}  // namespace bustub
