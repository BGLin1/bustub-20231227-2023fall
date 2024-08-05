//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// sort_executor.h
//
// Identification: src/include/execution/executors/sort_executor.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/sort_plan.h"
#include "storage/table/tuple.h"

namespace bustub {
  //自定义排序比较器
  class Comparator {
  public:
    Comparator() { schema_ = nullptr; }
    Comparator(const Schema* schema, std::vector<std::pair<OrderByType, AbstractExpressionRef>> order_bys)
      : schema_(schema), order_bys_(std::move(order_bys)) {}

    auto operator()(const Tuple& t1, const Tuple& t2) -> bool {
      for (auto const& order_by : this->order_bys_) {
        const auto order_type = order_by.first;
        // 使用Evaluate获取值
        AbstractExpressionRef expr = order_by.second;
        Value v1 = expr->Evaluate(&t1, *schema_);
        Value v2 = expr->Evaluate(&t2, *schema_);
        if (v1.CompareEquals(v2) == CmpBool::CmpTrue) {
          continue;
        }
        // 如果是升序（ASC 或 DEFAULT），比较 v1 是否小于 v2（CompareLessThan）
        if (order_type == OrderByType::ASC || order_type == OrderByType::DEFAULT) {
          return v1.CompareLessThan(v2) == CmpBool::CmpTrue;
        }
        // 如果是降序（DESC），比较 v1 是否大于 v2（CompareGreaterThan）
        return v1.CompareGreaterThan(v2) == CmpBool::CmpTrue;
      }
      // 两个Tuple所有键都相等
      return false;
    }
  private:
    const Schema* schema_;
    // 两个参数：升序还是降序，用那个键的值
    std::vector<std::pair<OrderByType, AbstractExpressionRef>> order_bys_;
  };

  /**
 * The SortExecutor executor executes a sort.
 */
  class SortExecutor : public AbstractExecutor {
  public:
    /**
     * Construct a new SortExecutor instance.
     * @param exec_ctx The executor context
     * @param plan The sort plan to be executed
     */
    SortExecutor(ExecutorContext* exec_ctx, const SortPlanNode* plan, std::unique_ptr<AbstractExecutor>&& child_executor);

    /** Initialize the sort */
    void Init() override;

    /**
     * Yield the next tuple from the sort.
     * @param[out] tuple The next tuple produced by the sort
     * @param[out] rid The next tuple RID produced by the sort
     * @return `true` if a tuple was produced, `false` if there are no more tuples
     */
    auto Next(Tuple* tuple, RID* rid) -> bool override;

    /** @return The output schema for the sort */
    auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

  private:
    /** The sort plan node to be executed */
    const SortPlanNode* plan_;
    //自己定义的私有变量
    std::unique_ptr<AbstractExecutor> child_executor_;
    std::vector<Tuple> tuples_;
    std::vector<Tuple>::iterator iter_;
  };
}  // namespace bustub
