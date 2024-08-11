//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"
#include "execution/execution_common.h"


namespace bustub {

  UpdateExecutor::UpdateExecutor(ExecutorContext* exec_ctx, const UpdatePlanNode* plan,
    std::unique_ptr<AbstractExecutor>&& child_executor)
    : AbstractExecutor(exec_ctx) {
    this->plan_ = plan;
    this->child_executor_ = std::move(child_executor);
  }

  void UpdateExecutor::Init() {
    this->child_executor_->Init();
    this->has_update_ = false;
    table_info_ = GetExecutorContext()->GetCatalog()->GetTable(plan_->GetTableOid());
    buffer_.clear();
    txn_mgr_ = exec_ctx_->GetTransactionManager();
    txn_ = exec_ctx_->GetTransaction();
    Tuple old_tuple{};
    RID rid;
    while (child_executor_->Next(&old_tuple, &rid)) {
      // use buffered old tuple because Visibility is guaranteed by the child executor
      if (rid.GetPageId() != INVALID_PAGE_ID) {
        buffer_.emplace_back(rid, old_tuple);
      }
    }
  }

  //p3没有事务的版本
  //总体的思路:删除tuple再插入tuple
/*   auto UpdateExecutor::Next([[maybe_unused]] Tuple* tuple, RID* rid) -> bool {
    if (has_update_) {
      return false;
    }
    has_update_ = true;
    auto table_info = this->exec_ctx_->GetCatalog()->GetTable(this->plan_->GetTableOid());
    auto index_infos = this->exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_);
    Tuple child_tuple{};
    RID child_rid{};
    int cnt = 0;

    while (this->child_executor_->Next(&child_tuple, &child_rid)) {
      cnt++;
      table_info->table_->UpdateTupleMeta(TupleMeta{ 0, true }, child_rid);
      std::vector<Value> new_values{};
      new_values.reserve(this->plan_->target_expressions_.size());
      for (const auto& expr : this->plan_->target_expressions_) {
        new_values.emplace_back(expr->Evaluate(&child_tuple, child_executor_->GetOutputSchema()));
      }
      auto new_tuple = Tuple{ new_values,&table_info->schema_ };
      auto new_rid = table_info->table_->InsertTuple(TupleMeta{ 0,false }, new_tuple).value();
      for (auto& info : index_infos) {
        auto index = info->index_.get();
        auto key_attrs = info->index_->GetKeyAttrs();
        auto old_key = child_tuple.KeyFromTuple(table_info->schema_, *index->GetKeySchema(), key_attrs);
        auto new_key = new_tuple.KeyFromTuple(table_info->schema_, *index->GetKeySchema(), key_attrs);
        index->DeleteEntry(old_key, child_rid, this->exec_ctx_->GetTransaction());
        index->InsertEntry(new_key, child_rid, this->exec_ctx_->GetTransaction());
      }
    }
    *tuple = Tuple{ {{TypeId::INTEGER,cnt}},&GetOutputSchema() };
    return true;
  } */
  //p4有事务的版本
  auto UpdateExecutor::Next(Tuple* tuple, RID* rid) -> bool {
   
    if (this->has_update_) {
      return false;
    }
    has_update_ = true;
    Tuple old_tuple{};
    TupleMeta old_tuple_meta;
    RID temp_rid;
    int update_num = 0;

    std::vector<IndexInfo*> indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
    const Schema* schema = &child_executor_->GetOutputSchema();
    const TupleMeta new_tuple_meta = TupleMeta{ txn_->GetTransactionTempTs(), false };
    std::vector<Value> values;
    values.reserve(schema->GetColumnCount());
    while (!buffer_.empty()) {
      values.clear();
      auto tuple_pair = buffer_.front();
      temp_rid = tuple_pair.first;
      old_tuple = std::move(tuple_pair.second);
      buffer_.pop_front();
      old_tuple_meta = table_info_->table_->GetTupleMeta(temp_rid);
      // update column values according to expressions
      for (const auto& expr : plan_->target_expressions_) {
        values.push_back(expr->Evaluate(&old_tuple, *schema));
      }
      // 更新完毕的新tuple
      Tuple new_tuple = Tuple(values, schema);
      UpdateTuple(table_info_, schema, txn_mgr_, txn_, old_tuple_meta, old_tuple, new_tuple_meta, new_tuple, temp_rid);
      ++update_num;
    }
    if (update_num == 0) {
      return false;
    }
    *tuple = Tuple{ {{TypeId::INTEGER,update_num}},&GetOutputSchema() };
    return true;
  }

}  // namespace bustub
