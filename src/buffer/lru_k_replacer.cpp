//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"

namespace bustub {


    LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

    //驱逐一个frame并把他的id返回给frame_id
    auto LRUKReplacer::Evict(frame_id_t* frame_id) -> bool {
        std::unique_lock<std::mutex> lock(this->latch_);
        if (this->curr_size_ == 0) {
            return false;
        }
        auto lru_node_iter = this->node_store_.end();
        for (auto iter = node_store_.begin();iter != this->node_store_.end();iter++) {
            auto node = iter->second;
            if (node.is_evictable_) {

                //还没有找到驱逐的节点
                if (lru_node_iter == this->node_store_.end()) {
                    lru_node_iter = iter;
                    continue;
                }
                //备选的驱逐节点访问次数小于k 换成经典的LRU算法判断 两个访问次数小于k的frame比较history
                if (lru_node_iter->second.k_ < this->k_) {
                    if (node.k_ >= this->k_) {
                        continue;
                    } else {
                        // std::cout << "node.history_.back()" << node.history_.back() << std::endl;
                        if (node.history_.back() < lru_node_iter->second.history_.back()) {
                            lru_node_iter = iter;
                            continue;
                        }
                    }
                }
                //备选的驱逐节点访问次数大于k 找到了一个访问次数小于k的节点
                if (lru_node_iter->second.k_ >= this->k_ && node.k_ < this->k_) {
                    lru_node_iter = iter;
                    continue;
                }

                //备选的驱逐节点访问次数大于k 找到了一个最近使用时间戳比备选的驱逐节点远的节点
                if (lru_node_iter->second.history_.back() > iter->second.history_.back()) {
                    lru_node_iter = iter;
                }
            }
        }
        *frame_id = lru_node_iter->first;
        lock.unlock();
        this->Remove(lru_node_iter->first);
        lock.lock();
        return true;
    }

    //访问一个frame,更新时间戳
    void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
        std::unique_lock<std::mutex> lock(this->latch_);
        current_timestamp_++;
        BUSTUB_ASSERT(frame_id > static_cast<frame_id_t> (replacer_size_), "frame_id bigger than replacer_size_");
        int k = 0;
        if (auto iter = node_store_.find(frame_id); iter == node_store_.end()) {
            //全新的frame
            auto new_node = LRUKNode(frame_id);
            new_node.history_.emplace_back(current_timestamp_);
            node_store_.emplace(frame_id, new_node);
            k = 1;
        } else {
            iter->second.history_.emplace_back(current_timestamp_);
            k = ++iter->second.k_;
        }

        // std::cout << "RecordAccess frame_id:" << frame_id << " current_timestamp_:" << current_timestamp_ << " curent_k:" << k << std::endl;

    }
    //设置一个frame是否可被驱逐
    void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
        std::unique_lock<std::mutex> lock(this->latch_);
        if (auto iter = node_store_.find(frame_id); iter == node_store_.end()) {
            throw bustub::Exception("frame_id is invalue");
        } else {
            if (iter->second.is_evictable_ && !set_evictable) {
                this->curr_size_--;
            } else if (!iter->second.is_evictable_ && set_evictable) {
                this->curr_size_++;
            }
            iter->second.is_evictable_ = set_evictable;
        }
    }
    //无条件驱逐frameid的frame
    void LRUKReplacer::Remove(frame_id_t frame_id) {
        std::unique_lock<std::mutex> lock(this->latch_);
        if (auto iter = node_store_.find(frame_id); iter == node_store_.end()) {
            return;
        } else {
            if (iter->second.is_evictable_) {
                node_store_.erase(frame_id);
                // std::cout << "Remove frame_id:" << frame_id << std::endl;
                this->curr_size_--;
            } else {
                throw bustub::Exception("remove a disevictable frame");
            }
        }
        return;
    }

    auto LRUKReplacer::Size() -> size_t { return curr_size_; }

}  // namespace bustub
