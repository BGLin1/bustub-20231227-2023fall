//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_htable_directory_page.cpp
//
// Identification: src/storage/page/extendible_htable_directory_page.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/extendible_htable_directory_page.h"

#include <algorithm>
#include <unordered_map>

#include "common/config.h"
#include "common/logger.h"

namespace bustub {

  void ExtendibleHTableDirectoryPage::Init(uint32_t max_depth) {
    this->max_depth_ = max_depth;
    this->global_depth_ = 0;
    memset(local_depths_, 0, sizeof(uint8_t) * HTABLE_DIRECTORY_ARRAY_SIZE);
    memset(bucket_page_ids_, 0, sizeof(page_id_t) * HTABLE_DIRECTORY_ARRAY_SIZE);
  }

  auto ExtendibleHTableDirectoryPage::HashToBucketIndex(uint32_t hash) const -> uint32_t {
    return hash % (1 << global_depth_);
  }

  auto ExtendibleHTableDirectoryPage::GetBucketPageId(uint32_t bucket_idx) const -> page_id_t {
    if (bucket_idx >= HTABLE_DIRECTORY_ARRAY_SIZE) {
      return INVALID_PAGE_ID;
    }
    return bucket_page_ids_[bucket_idx];
  }

  void ExtendibleHTableDirectoryPage::SetBucketPageId(uint32_t bucket_idx, page_id_t bucket_page_id) {
    BUSTUB_ASSERT(bucket_idx >= (1 << global_depth_), "Bucket index out of range");
    bucket_page_ids_[bucket_idx] = bucket_page_id;
  }
  //找到分裂后的下一个桶的编号 也就是当前桶编号最高位前加一个一
  auto ExtendibleHTableDirectoryPage::GetSplitImageIndex(uint32_t bucket_idx) const -> uint32_t {
    return bucket_idx + (1 << (global_depth_ - 1));
  }

  auto ExtendibleHTableDirectoryPage::GetGlobalDepthMask() const -> uint32_t {
    return (1 << (global_depth_ - 1));
  }

  auto ExtendibleHTableDirectoryPage::GetLocalDepthMask(uint32_t bucket_idx) const->uint32_t {
    BUSTUB_ASSERT(bucket_idx >= (1 << global_depth_), "Bucket index out of range");
    return (1 << (local_depths_[bucket_idx] - 1));
  }

  auto ExtendibleHTableDirectoryPage::GetGlobalDepth() const -> uint32_t {
    return global_depth_;
  }
  auto ExtendibleHTableDirectoryPage::GetMaxDepth() const -> uint32_t {
    return max_depth_;
  }
  //在全局深度增加时,Directory中的每一个索引都会分裂
  void ExtendibleHTableDirectoryPage::IncrGlobalDepth() {
    if (global_depth_ >= max_depth_) {
      return;
    }
    for (int i = 0;i < (1 << global_depth_);i++) {
      bucket_page_ids_[i + (1 << global_depth_)] = bucket_page_ids_[i];
      local_depths_[i + (1 << global_depth_)] = local_depths_[i];
    }
    global_depth_++;
  }

  void ExtendibleHTableDirectoryPage::DecrGlobalDepth() {
    if (global_depth_ <= 0) {
      return;
    }
    global_depth_--;
  }

  auto ExtendibleHTableDirectoryPage::CanShrink() -> bool {
    if (global_depth_ == 0) {
      return false;
    }
    // 检查所有桶的局部深度是否都小于全局深度
    for (uint32_t i = 0; i < Size(); i++) {
      // 有局部深度等于全局深度的->不能收缩
      if (local_depths_[i] == global_depth_) {
        return false;
      }
    }
    return true;
  }

  auto ExtendibleHTableDirectoryPage::Size() const -> uint32_t {
    return 1 << global_depth_;
  }

  auto ExtendibleHTableDirectoryPage::GetLocalDepth(uint32_t bucket_idx) const -> uint32_t {
    BUSTUB_ASSERT(bucket_idx >= (1 << global_depth_), "Bucket index out of range");
    return local_depths_[bucket_idx];
  }

  void ExtendibleHTableDirectoryPage::SetLocalDepth(uint32_t bucket_idx, uint8_t local_depth) {
    BUSTUB_ASSERT(bucket_idx >= (1 << global_depth_), "Bucket index out of range");
    local_depths_[bucket_idx] = local_depth;
  }

  void ExtendibleHTableDirectoryPage::IncrLocalDepth(uint32_t bucket_idx) {
    if (local_depths_[bucket_idx] < global_depth_) {
      ++local_depths_[bucket_idx];
    }
  }

  void ExtendibleHTableDirectoryPage::DecrLocalDepth(uint32_t bucket_idx) {
    if (local_depths_[bucket_idx] > 0) {
      --local_depths_[bucket_idx];
    }
  }

}  // namespace bustub
