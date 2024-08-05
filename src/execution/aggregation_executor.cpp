//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

    AggregationExecutor::AggregationExecutor(ExecutorContext* exec_ctx, const AggregationPlanNode* plan, std::unique_ptr<AbstractExecutor>&& child_executor) :
        AbstractExecutor(exec_ctx),
        plan_(plan),
        child_executor_(std::move(child_executor)),
        aht_(plan->GetAggregates(), plan->GetAggregateTypes()),
        aht_iterator_(aht_.Begin())
    {}

    void AggregationExecutor::Init() {
        this->child_executor_->Init();
        Tuple child_tuple{};
        RID child_rid{};
        //把聚合键和聚合值都插入hash表
        while (this->child_executor_->Next(&child_tuple, &child_rid)) {
            this->aht_.InsertCombine(MakeAggregateKey(&child_tuple), MakeAggregateValue(&child_tuple));
        }
        this->aht_iterator_ = this->aht_.Begin();
    }

    auto AggregationExecutor::Next(Tuple* tuple, RID* rid) -> bool {
        if (aht_.Begin() != aht_.End()) {
            if (aht_iterator_ == aht_.End()) {
                return false;
            }
            // 获取聚合键和聚合值
            auto agg_key = aht_iterator_.Key();
            auto agg_val = aht_iterator_.Val();
            //根据聚合键和聚合值生成查询结果tuple
            std::vector<Value> values{};
            // 遍历聚合键和聚合值，生成查询结果元组
            // 根据要求，有groupby和aggregate两个部分的情况下，groupby也要算上，都添加到value中
            values.reserve(agg_key.group_bys_.size() + agg_val.aggregates_.size());
            for (auto& group_values : agg_key.group_bys_) {
                values.emplace_back(group_values);
            }
            for (auto& agg_value : agg_val.aggregates_) {
                values.emplace_back(agg_value);
            }
            *tuple = { values, &GetOutputSchema() };
            //迭代到下一个聚合键和聚合值
            ++aht_iterator_;
            // 表示成功返回了一个聚合结果
            return true;
        }
        if (has_inserted_) {
            return false;
        }
        has_inserted_ = true;
        // 没有groupby语句则生成一个初始的聚合值元组并返回
        if (plan_->GetGroupBys().empty()) {
            std::vector<Value> values{};
            Tuple tuple_buffer{};
            // 检查当前表是否为空，如果为空生成默认的聚合值
            // 默认聚合值要求由GenerateInitialAggregateValue实现
            for (auto& agg_value : aht_.GenerateInitialAggregateValue().aggregates_) {
                values.emplace_back(agg_value);
            }
            *tuple = { values, &GetOutputSchema() };
            return true;
        }
        return false;

    }

    auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor* { return child_executor_.get(); }

}  // namespace bustub
