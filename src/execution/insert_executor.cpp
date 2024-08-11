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
#include "concurrency/transaction_manager.h"

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
    //p3未加入事务的版本
    /* auto InsertExecutor::Next([[maybe_unused]] Tuple* tuple, RID* rid) -> bool {
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
    } */
    //p4加入事务的版本
    auto InsertExecutor::Next([[maybe_unused]] Tuple* tuple, RID* rid) -> bool {
        LOG_TRACE("Insert executor Next");
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
            //为tuple设置事务id
            auto inserted_rid = table_info->table_->InsertTuple(TupleMeta{ exec_ctx_->GetTransaction()->GetTransactionId(), false }, *tuple);
            
            //修改versionlink
            auto txn_mgr = this->exec_ctx_->GetTransactionManager();
            txn_mgr->UpdateVersionLink(*inserted_rid, std::nullopt, nullptr);

            //修改index
            for (auto& info : index_infos) {
                //建立在tuple哪一个key上的index
                auto key = tuple->KeyFromTuple(table_info->schema_, info->key_schema_, info->index_->GetKeyAttrs());
                info->index_->InsertEntry(key, *inserted_rid, this->exec_ctx_->GetTransaction());
            }

            //在 Transaction 的 write set 中维护新写入的 RID
            auto txn = txn_mgr->txn_map_[exec_ctx_->GetTransaction()->GetTransactionId()].get();
            txn->AppendWriteSet(plan_->GetTableOid(), *inserted_rid);
        }
        *tuple = Tuple{ {{TypeId::INTEGER,cnt}},&GetOutputSchema() };
        return true;
    }

}  // namespace bustub
