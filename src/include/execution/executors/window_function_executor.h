//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// window_function_executor.h
//
// Identification: src/include/execution/executors/window_function_executor.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/window_plan.h"
#include "storage/table/tuple.h"
#include "type/value_factory.h"
#include "execution/plans/aggregation_plan.h"

namespace bustub {

  /**
   * The WindowFunctionExecutor executor executes a window function for columns using window function.
   *
   * Window function is different from normal aggregation as it outputs one row for each inputing rows,
   * and can be combined with normal selected columns. The columns in WindowFunctionPlanNode contains both
   * normal selected columns and placeholder columns for window functions.
   *
   * For example, if we have a query like:
   *    SELECT 0.1, 0.2, SUM(0.3) OVER (PARTITION BY 0.2 ORDER BY 0.3), SUM(0.4) OVER (PARTITION BY 0.1 ORDER BY 0.2,0.3)
   *      FROM table;
   *
   * The WindowFunctionPlanNode contains following structure:
   *    columns: std::vector<AbstractExpressionRef>{0.1, 0.2, 0.-1(placeholder), 0.-1(placeholder)}
   *    window_functions_: {
   *      3: {
   *        partition_by: std::vector<AbstractExpressionRef>{0.2}
   *        order_by: std::vector<AbstractExpressionRef>{0.3}
   *        functions: std::vector<AbstractExpressionRef>{0.3}
   *        window_func_type: WindowFunctionType::SumAggregate
   *      }
   *      4: {
   *        partition_by: std::vector<AbstractExpressionRef>{0.1}
   *        order_by: std::vector<AbstractExpressionRef>{0.2,0.3}
   *        functions: std::vector<AbstractExpressionRef>{0.4}
   *        window_func_type: WindowFunctionType::SumAggregate
   *      }
   *    }
   *
   * Your executor should use child executor and exprs in columns to produce selected columns except for window
   * function columns, and use window_agg_indexes, partition_bys, order_bys, functionss and window_agg_types to
   * generate window function columns results. Directly use placeholders for window function columns in columns is
   * not allowed, as it contains invalid column id.
   *
   * Your WindowFunctionExecutor does not need to support specified window frames (eg: 1 preceding and 1 following).
   * You can assume that all window frames are UNBOUNDED FOLLOWING AND CURRENT ROW when there is ORDER BY clause, and
   * UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING when there is no ORDER BY clause.
   *
   */
   /**
    * A simplified hash table that has all the necessary functionality for window functions
    */
  class SimpleWindowHashTable {
  public:
    /**
     * Construct a new SimpleWindowHashTable instance.
     * @param window_agg_exprs the window aggregation expressions
     * @param window_agg_types the types of window aggregations
     */
    explicit SimpleWindowHashTable(const WindowFunctionType& window_function_type)
      : window_function_type_(window_function_type) {}

    /** @return The initial window aggregate value for this window executor*/
    auto GenerateInitialWindowAggregateValue() -> Value {
      Value value;
      switch (window_function_type_) {
      case WindowFunctionType::CountStarAggregate:
        return ValueFactory::GetIntegerValue(0);
      case WindowFunctionType::Rank:
      case WindowFunctionType::CountAggregate:
      case WindowFunctionType::SumAggregate:
      case WindowFunctionType::MinAggregate:
      case WindowFunctionType::MaxAggregate:
        return ValueFactory::GetNullValueByType(TypeId::INTEGER);
      }
      return {};
    }

    /**
     * Combines the input into the aggregation result.
     * @param[out] result The output rows of aggregate value corresponding to one key
     * @param input The input value
     */
    auto CombineAggregateValues(Value* result, const Value& input) -> Value {
      Value& old_val = *result;
      const Value& new_val = input;
      switch (window_function_type_) {
      case WindowFunctionType::CountStarAggregate:
        old_val = old_val.Add(Value(TypeId::INTEGER, 1));
        break;
      case WindowFunctionType::CountAggregate:
        if (!new_val.IsNull()) {
          if (old_val.IsNull()) {
            old_val = ValueFactory::GetIntegerValue(0);
          }
          old_val = old_val.Add(Value(TypeId::INTEGER, 1));
        }
        break;
      case WindowFunctionType::SumAggregate:
        if (!new_val.IsNull()) {
          if (old_val.IsNull()) {
            old_val = new_val;
          } else {
            old_val = old_val.Add(new_val);
          }
        }
        break;
      case WindowFunctionType::MinAggregate:
        if (!new_val.IsNull()) {
          if (old_val.IsNull()) {
            old_val = new_val;
          } else {
            old_val = new_val.CompareLessThan(old_val) == CmpBool::CmpTrue ? new_val.Copy() : old_val;
          }
        }
        break;
      case WindowFunctionType::MaxAggregate:
        if (!new_val.IsNull()) {
          if (old_val.IsNull()) {
            old_val = new_val;
          } else {
            old_val = new_val.CompareGreaterThan(old_val) == CmpBool::CmpTrue ? new_val.Copy() : old_val;
          }
        }
        break;
      case WindowFunctionType::Rank:
        ++rank_count_;
        if (old_val.CompareEquals(new_val) != CmpBool::CmpTrue) {
          old_val = new_val;
          last_rank_count_ = rank_count_;
        }
        return ValueFactory::GetIntegerValue(last_rank_count_);
      }
      return old_val;
    }

    /**
     * Inserts a value into the hash table and then combines it with the current aggregation
     * @param win_key the key to be inserted
     * @param win_val the value to be inserted
     */
    auto InsertCombine(const AggregateKey& win_key, const Value& win_value) -> Value {
      if (ht_.count(win_key) == 0) {
        ht_.insert({ win_key, GenerateInitialWindowAggregateValue() });
      }
      return CombineAggregateValues(&ht_[win_key], win_value);
    }

    /**
     * Find a value with give key
     * @param win_key the key to be used to find its corresponding value
     */
    auto Find(const AggregateKey& win_key) -> Value { return ht_.find(win_key)->second; }
    /**
     * Clear the hash table
     */
    void Clear() { ht_.clear(); }

  private:

    const WindowFunctionType window_function_type_;
    std::unordered_map<AggregateKey, Value> ht_;
    uint32_t rank_count_ = 0;
    uint32_t last_rank_count_ = 0;
  };



  class WindowFunctionExecutor : public AbstractExecutor {
  public:
    /**
     * Construct a new WindowFunctionExecutor instance.
     * @param exec_ctx The executor context
     * @param plan The window aggregation plan to be executed
     */
    WindowFunctionExecutor(ExecutorContext* exec_ctx, const WindowFunctionPlanNode* plan,
      std::unique_ptr<AbstractExecutor>&& child_executor);

    /** Initialize the window aggregation */
    void Init() override;

    /**
     * Yield the next tuple from the window aggregation.
     * @param[out] tuple The next tuple produced by the window aggregation
     * @param[out] rid The next tuple RID produced by the window aggregation
     * @return `true` if a tuple was produced, `false` if there are no more tuples
     */
    auto Next(Tuple* tuple, RID* rid) -> bool override;

    /** @return The output schema for the window aggregation plan */
    auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

  private:

    auto MakeWinKey(const Tuple* tuple, const std::vector<AbstractExpressionRef>& partition_bys) -> AggregateKey {
      std::vector<Value> keys;
      keys.reserve(partition_bys.size());
      for (const auto& expr : partition_bys) {
        keys.emplace_back(expr->Evaluate(tuple, child_executor_->GetOutputSchema()));
      }
      return { keys };
    }

    auto MakeWinValue(const Tuple* Tuple, const AbstractExpressionRef& window_expr) -> Value {
      return window_expr->Evaluate(Tuple, child_executor_->GetOutputSchema());
    }

    /** The window aggregation plan node to be executed */
    const WindowFunctionPlanNode* plan_;

    /** The child executor from which tuples are obtained */
    std::unique_ptr<AbstractExecutor> child_executor_;

    //自己添加的私有变量
    std::vector<SimpleWindowHashTable> whts_;
    std::deque<std::vector<Value>> tuples_;

  };
}  // namespace bustub
