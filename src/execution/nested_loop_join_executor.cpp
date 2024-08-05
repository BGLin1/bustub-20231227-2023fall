//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "type/value_factory.h"

namespace bustub {

  NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext* exec_ctx, const NestedLoopJoinPlanNode* plan,
    std::unique_ptr<AbstractExecutor>&& left_executor,
    std::unique_ptr<AbstractExecutor>&& right_executor)
    : AbstractExecutor(exec_ctx)

    , plan_(plan)
    , left_executor_(std::move(left_executor))
    , right_executor_(std::move(right_executor)) {

    //只需要完成 left join and inner join.
    if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
      // Note for 2023 Fall: You ONLY need to implement left join and inner join.
      throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
    }
  }

  void NestedLoopJoinExecutor::Init() {
    left_executor_->Init();
    right_executor_->Init();
    Tuple child_tuple{};
    RID child_rid{};
    std::vector<Tuple> left_tuples;
    std::vector<Tuple> right_tuples;
    while (this->left_executor_->Next(&child_tuple, &child_rid)) {
      left_tuples.push_back(child_tuple);
    }
    while (this->right_executor_->Next(&child_tuple, &child_rid)) {
      right_tuples.push_back(child_tuple);
    }
    //left join
    if (plan_->GetJoinType() == JoinType::LEFT) {
      for (auto& left_tuple : left_tuples) {
        auto is_join_success = false;
        for (auto& right_tuple : right_tuples) {
          //从predicate_中判断这两个tuple是否要join
          auto join_value = this->plan_->predicate_->EvaluateJoin(&left_tuple, left_executor_->GetOutputSchema(), &right_tuple, right_executor_->GetOutputSchema());
          //join success
          if (!join_value.IsNull() && join_value.GetAs<bool>()) {
            is_join_success = true;
            std::vector<Value> cur_tuple_value;
            for (size_t i = 0;i < left_executor_->GetOutputSchema().GetColumnCount();i++) {
              cur_tuple_value.emplace_back(left_tuple.GetValue(&left_executor_->GetOutputSchema(), i));
            }
            for (size_t i = 0;i < right_executor_->GetOutputSchema().GetColumnCount();i++) {
              cur_tuple_value.emplace_back(right_tuple.GetValue(&right_executor_->GetOutputSchema(), i));
            }
            this->left_join_res.emplace(cur_tuple_value, &GetOutputSchema());
          }
        }
        if (!is_join_success) {
          //left join,如果没join上，右边的tuple输出为空
          std::vector<Value> cur_tuple_value;
          for (size_t i = 0;i < left_executor_->GetOutputSchema().GetColumnCount();i++) {
            cur_tuple_value.emplace_back(left_tuple.GetValue(&left_executor_->GetOutputSchema(), i));
          }
          for (size_t i = 0;i < right_executor_->GetOutputSchema().GetColumnCount();i++) {
            cur_tuple_value.emplace_back(ValueFactory::GetNullValueByType(right_executor_->GetOutputSchema().GetColumn(i).GetType()));
          }
          this->left_join_res.emplace(cur_tuple_value, &GetOutputSchema());
        }
      }
    } else if (plan_->GetJoinType() == JoinType::INNER) { //inner join 和left join前半部分一样
      for (auto& left_tuple : left_tuples) {
        for (auto& right_tuple : right_tuples) {
          auto join_value = this->plan_->predicate_->EvaluateJoin(&left_tuple, left_executor_->GetOutputSchema(), &right_tuple, right_executor_->GetOutputSchema());
          if (!join_value.IsNull() && join_value.GetAs<bool>()) {
            std::vector<Value> cur_tuple_value;
            for (size_t i = 0;i < left_executor_->GetOutputSchema().GetColumnCount();i++) {
              cur_tuple_value.emplace_back(left_tuple.GetValue(&left_executor_->GetOutputSchema(), i));
            }
            for (size_t i = 0;i < right_executor_->GetOutputSchema().GetColumnCount();i++) {
              cur_tuple_value.emplace_back(right_tuple.GetValue(&right_executor_->GetOutputSchema(), i));
            }
            this->left_join_res.emplace(cur_tuple_value, &GetOutputSchema());
          }
        }
      }
    }
  }

  auto NestedLoopJoinExecutor::Next(Tuple* tuple, RID* rid) -> bool {
    if (plan_->GetJoinType() == JoinType::LEFT) {
      if (this->left_join_res.empty()) {
        return false;
      }
      *tuple = this->left_join_res.front();
      *rid = tuple->GetRid();
      this->left_join_res.pop();
      return true;
    }
    if (plan_->GetJoinType() == JoinType::INNER) {
      if (this->inner_join_res.empty()) {
        return false;
      }
      *tuple = this->inner_join_res.front();
      *rid = tuple->GetRid();
      this->inner_join_res.pop();
      return true;
    }
    return false;
  }

}  // namespace bustub
