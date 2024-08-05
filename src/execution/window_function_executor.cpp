#include "execution/executors/window_function_executor.h"
#include "execution/plans/window_plan.h"
#include "storage/table/tuple.h"
#include "execution/executors/sort_executor.h"

namespace bustub {

    WindowFunctionExecutor::WindowFunctionExecutor(ExecutorContext* exec_ctx, const WindowFunctionPlanNode* plan,
        std::unique_ptr<AbstractExecutor>&& child_executor)
        : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

    void WindowFunctionExecutor::Init() {
        child_executor_->Init();
        // 获取窗口函数的信息
        auto window_functions = plan_->window_functions_;
        // 获取列数
        auto cloumn_size = plan_->columns_.size();
        //创建各类vection用于存储窗口函数的具体信息
        // 是否需要排序
        std::vector<bool> is_order_by(plan_->columns_.size());
        // 窗口函数表达式
        std::vector<AbstractExpressionRef> window_exprs(cloumn_size);
        // 窗口函数类型
        std::vector<WindowFunctionType> window_function_types(cloumn_size);
        // 分组条件
        std::vector<std::vector<AbstractExpressionRef>> partition_by(cloumn_size);
        // 排序条件
        std::vector<std::vector<std::pair<OrderByType, AbstractExpressionRef>>> order_bys(cloumn_size);
        // 是否是函数表达式
        std::vector<bool> is_function_expr(cloumn_size);
        // 获取窗口函数中的值，并且将相应的值存入vector中
        for (uint32_t i = 0; i < cloumn_size; i++) {
            // 如果没有窗口函数，则直接将列存入vector中，说明只是单纯的数值列
            if (window_functions.find(i) == window_functions.end()) {
                // 直接将列存入vector中
                window_exprs[i] = plan_->columns_[i];
                // 说明只是单纯的数值列
                is_function_expr[i] = false;
                // 没有排序
                is_order_by[i] = false;
                // 将空的窗口函数类型也存入SimpleWindowHashTable的vector中，方便後續遍歷使用
                whts_.emplace_back(window_function_types[i]);
                continue;
            }
            // 说明是函数表达式
            is_function_expr[i] = true;
            // 获取窗口函数
            const auto& window_function = window_functions.find(i)->second;
            // 将窗口函数存入vector中
            window_exprs[i] = window_function.function_;
            // 获取窗口函数类型
            window_function_types[i] = window_function.type_;
            // 获取分组条件
            partition_by[i] = window_function.partition_by_;
            // 获取排序条件
            order_bys[i] = window_function.order_by_;
            // 判断是否需要排序，因為即使有窗口函數，但是也有可能不需要排序
            is_order_by[i] = !window_function.order_by_.empty();
            // 创建SimpleWindowHashTable
            whts_.emplace_back(window_function_types[i]);
        }
        Tuple tuple{};
        RID rid{};
        std::vector<Tuple> tuples;
        // 获取符合条件的所有元组
        while (child_executor_->Next(&tuple, &rid)) {
            tuples.emplace_back(tuple);
        }
        // 获取order_by_，这里因为文档中说了，所有的窗口函数都只支持一个order_by，所以直接取第一个即可
        const auto& order_by(window_functions.begin()->second.order_by_);
        if (!order_by.empty()) {
            // 如果order_by不为空，则对元组进行排序
            std::sort(tuples.begin(), tuples.end(), bustub::Comparator(&child_executor_->GetOutputSchema(), order_by));
        }
        // 用于存储窗口函数的key
        std::vector<std::vector<AggregateKey>> tuple_keys;
        // 获取窗口函数中的聚合函数或者rank函数
        for (const auto& this_tuple : tuples) {
            std::vector<Value> values{};
            std::vector<AggregateKey> keys;
            // 遍历元组列，判断符合条件的列
            for (uint32_t i = 0; i < cloumn_size; ++i) {
                // 如果是函数表达式，则需要处理
                if (is_function_expr[i]) {
                    // 获取窗口函数的key
                    auto agg_key = MakeWinKey(&this_tuple, partition_by[i]);
                    // 如果是rank函数，则需要特殊处理
                    if (window_function_types[i] == WindowFunctionType::Rank) {
                        // 获取该列的最新值
                        auto new_value = order_by[0].second->Evaluate(&this_tuple, this->GetOutputSchema());
                        // 这里是rank函数，需要判断该值是否与之前的值相同，如果相同则，rank等级一样
                        values.emplace_back(whts_[i].InsertCombine(agg_key, new_value));
                        keys.emplace_back(agg_key);
                        continue;
                    }
                    // 聚合函数的情况下，与前面聚合函数的处理一样
                    auto agg_val = MakeWinValue(&this_tuple, window_exprs[i]);
                    values.emplace_back(whts_[i].InsertCombine(agg_key, agg_val));
                    keys.emplace_back(agg_key);
                    continue;
                }
                // 对于没有窗口函数的列，直接将列存入vector中
                values.emplace_back(window_exprs[i]->Evaluate(&this_tuple, this->GetOutputSchema()));
                keys.emplace_back();
            }
            // 将更新后的列值存入tuple的vector中
            tuples_.emplace_back(std::move(values));
            // 将更新后的key存入tuple_keys的vector中
            tuple_keys.emplace_back(std::move(keys));
        }
        // 这次用于处理没有order_by的情况下，不需要对每个元组单独进行窗口函数处理，每一个元组的列值都是相同的，且是最新值
        for (uint32_t tuple_idx = 0; tuple_idx < tuples_.size(); ++tuple_idx) {
            auto& tuplenew = tuples_[tuple_idx];
            for (uint32_t i = 0; i < tuplenew.size(); ++i) {
                if (is_function_expr[i] && !is_order_by[i]) {
                    // 将每个元组窗口函数的列值更新为最新值
                    tuplenew[i] = whts_[i].Find(tuple_keys[tuple_idx][i]);
                }
            }
        }
    }

    auto WindowFunctionExecutor::Next(Tuple* tuple, RID* rid) -> bool {
        if (tuples_.empty()) {
            return false;
        }
        // 获取元组
        *tuple = Tuple(tuples_.front(), &this->GetOutputSchema());
        *rid = tuple->GetRid();
        // 删除已经处理过的元组
        tuples_.pop_front();
        return true;
    }
}  // namespace bustub
