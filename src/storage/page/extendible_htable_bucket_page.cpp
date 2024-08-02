//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_htable_bucket_page.cpp
//
// Identification: src/storage/page/extendible_htable_bucket_page.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <optional>
#include <utility>

#include "common/exception.h"
#include "storage/page/extendible_htable_bucket_page.h"
#include "storage/page/b_plus_tree_page.h"

namespace bustub {

  template <typename KeyType, typename ValueType, typename KeyComparator>
  void ExtendibleHTableBucketPage<KeyType, ValueType, KeyComparator>::Init(uint32_t max_size) {
    this->max_size_ = max_size;
    this->size_ = 0;
    memset(array_, 0, HTableBucketArraySize(sizeof(MappingType)) * sizeof(MappingType));

  }

  template <typename K, typename V, typename KC>
  auto ExtendibleHTableBucketPage<K, V, KC>::Lookup(const K& key, V& value, const KC& cmp) const -> bool {
    for (uint32_t i = 0; i < size_; ++i) {
      if (cmp(array_[i].first, key) == 0) {
        value = array_[i].second;
        std::cout << "ExtendibleHTableBucketPage Lookup: K:" << key << " value:" << value << std::endl;
        return true;
      }
    }
    return false;
  }

  template <typename K, typename V, typename KC>
  auto ExtendibleHTableBucketPage<K, V, KC>::Insert(const K& key, const V& value, const KC& cmp) -> bool {
    if (this->IsFull()) {
      return false;
    }
    for (uint32_t i = 0; i < size_; ++i) {
      if (cmp(this->array_[i].first, key) == 0) {
        return false;
      }
    }
    array_[size_++] = std::make_pair(key, value);
    std::cout << "ExtendibleHTableBucketPage Insert: K:" << key << " value:" << value << std::endl;
    return true;
  }

  template <typename K, typename V, typename KC>
  auto ExtendibleHTableBucketPage<K, V, KC>::Remove(const K& key, const KC& cmp) -> bool {
    for (uint32_t i = 0; i < size_; ++i) {
      if (cmp(array_[i].first, key) == 0) {
        // 找到键并删除
        for (uint32_t j = i + 1; j < size_; ++j) {
          array_[j - 1] = array_[j];
        }
        size_--;
        std::cout << "ExtendibleHTableBucketPage Remove: K:" << key << std::endl;
        return true;
      }
    }
    return false;
  }

  template <typename K, typename V, typename KC>
  void ExtendibleHTableBucketPage<K, V, KC>::RemoveAt(uint32_t bucket_idx) {
    BUSTUB_ASSERT(bucket_idx > size_, "bucket_idx should be less than size_");
    for (uint32_t i = bucket_idx; i <= size_; ++i) {
      array_[i] = array_[i + 1];
    }
    size_--;
  }
  template <typename KeyType, typename ValueType, typename KeyComparator>
  void ExtendibleHTableBucketPage<KeyType, ValueType, KeyComparator>::Clear() {
    this->size_ = 0;
    memset(array_, 0, HTableBucketArraySize(sizeof(MappingType)) * sizeof(MappingType));
  }

  template <typename K, typename V, typename KC>
  auto ExtendibleHTableBucketPage<K, V, KC>::KeyAt(uint32_t bucket_idx) const -> K {
    return EntryAt(bucket_idx).first;
  }

  template <typename K, typename V, typename KC>
  auto ExtendibleHTableBucketPage<K, V, KC>::ValueAt(uint32_t bucket_idx) const -> V {

    return EntryAt(bucket_idx).second;
  }

  template <typename K, typename V, typename KC>
  auto ExtendibleHTableBucketPage<K, V, KC>::EntryAt(uint32_t bucket_idx) const -> const std::pair<K, V>& {
    return array_[bucket_idx];
  }

  template <typename K, typename V, typename KC>
  auto ExtendibleHTableBucketPage<K, V, KC>::Size() const -> uint32_t {
    return this->size_;
  }

  template <typename K, typename V, typename KC>
  auto ExtendibleHTableBucketPage<K, V, KC>::IsFull() const -> bool {
    return this->size_ == this->max_size_;
  }

  template <typename K, typename V, typename KC>
  auto ExtendibleHTableBucketPage<K, V, KC>::IsEmpty() const -> bool {
    return this->size_ == 0;
  }

  template class ExtendibleHTableBucketPage<int, int, IntComparator>;
  template class ExtendibleHTableBucketPage<GenericKey<4>, RID, GenericComparator<4>>;
  template class ExtendibleHTableBucketPage<GenericKey<8>, RID, GenericComparator<8>>;
  template class ExtendibleHTableBucketPage<GenericKey<16>, RID, GenericComparator<16>>;
  template class ExtendibleHTableBucketPage<GenericKey<32>, RID, GenericComparator<32>>;
  template class ExtendibleHTableBucketPage<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
