//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_extendible_hash_table.cpp
//
// Identification: src/container/disk/hash/disk_extendible_hash_table.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"
#include "common/rid.h"
#include "common/util/hash_util.h"
#include "container/disk/hash/disk_extendible_hash_table.h"
#include "storage/index/hash_comparator.h"
#include "storage/page/extendible_htable_bucket_page.h"
#include "storage/page/extendible_htable_directory_page.h"
#include "storage/page/extendible_htable_header_page.h"
#include "storage/page/page_guard.h"

namespace bustub {

  template <typename K, typename V, typename KC>
  DiskExtendibleHashTable<K, V, KC>::DiskExtendibleHashTable(const std::string& name, BufferPoolManager* bpm,
    const KC& cmp, const HashFunction<K>& hash_fn,
    uint32_t header_max_depth, uint32_t directory_max_depth,
    uint32_t bucket_max_size) :
    index_name_(name),
    bpm_(bpm),
    cmp_(cmp),
    hash_fn_(std::move(hash_fn)),
    header_max_depth_(header_max_depth),
    directory_max_depth_(directory_max_depth),
    bucket_max_size_(bucket_max_size) {

    //初始化header页面
    header_page_id_ = INVALID_PAGE_ID;
    auto head_page_guard = bpm_->NewPageGuarded(&header_page_id_);
    auto head_page = head_page_guard.AsMut<ExtendibleHTableHeaderPage>();
    head_page->Init(this->header_max_depth_);

  }

  /*****************************************************************************
   * SEARCH
   *****************************************************************************/
  template <typename K, typename V, typename KC>
  auto DiskExtendibleHashTable<K, V, KC>::GetValue(const K& key, std::vector<V>* result, Transaction* transaction) const-> bool {
    auto head_page_guard = bpm_->FetchPageRead(header_page_id_);
    auto head_page = head_page_guard.As<ExtendibleHTableHeaderPage>();
    auto hash_value = this->Hash(key);
    //找到dir page id
    page_id_t directory_page_id = head_page->GetDirectoryPageId(head_page->HashToDirectoryIndex(hash_value));
    head_page_guard.Drop();
    if (directory_page_id == INVALID_PAGE_ID) {
      return false;
    }
    //获得dir page
    auto directory_page_guard = bpm_->FetchPageRead(directory_page_id);
    auto directory_page = directory_page_guard.As<ExtendibleHTableDirectoryPage>();

    //找到bucket page id
    page_id_t bucket_page_id = directory_page->GetBucketPageId(directory_page->HashToBucketIndex(hash_value));
    directory_page_guard.Drop();
    if (bucket_page_id == INVALID_PAGE_ID) {
      return false;
    }
    //获得bucket page
    auto bucket_page_guard = bpm_->FetchPageRead(bucket_page_id);
    auto bucket_page = bucket_page_guard.As<ExtendibleHTableBucketPage<K, V, KC>>();
    //查找key
    V value;
    if (bucket_page->Lookup(key, value, cmp_)) {
      result->push_back(value);
      return true;
    }
    return false;
  }

  /*****************************************************************************
   * INSERTION
   *****************************************************************************/

  template <typename K, typename V, typename KC>
  auto DiskExtendibleHashTable<K, V, KC>::Insert(const K& key, const V& value, Transaction* transaction) -> bool {
    std::vector<V> values;
    if (GetValue(key, &values, transaction)) {
      return false;
    }
    auto hash_value = this->Hash(key);

    ReadPageGuard head_page_guard = bpm_->FetchPageRead(header_page_id_);
    auto head_page = head_page_guard.As<ExtendibleHTableHeaderPage>();
    //找到dir page id
    auto directory_id = head_page->HashToDirectoryIndex(hash_value);
    page_id_t directory_page_id = head_page->GetDirectoryPageId(directory_id);
    if (directory_page_id == INVALID_PAGE_ID) {
      //dir不存在 插入dir
      return InsertToNewDirectory(const_cast<ExtendibleHTableHeaderPage*>(head_page), directory_id, hash_value, key, value);
    }
    head_page_guard.Drop();
    //获得dir page
    WritePageGuard directory_page_guard = bpm_->FetchPageWrite(directory_page_id);
    auto directory_page = directory_page_guard.AsMut<ExtendibleHTableDirectoryPage>();
    //找到bucket page 
    auto bucket_id = directory_page->HashToBucketIndex(hash_value);
    page_id_t bucket_page_id = directory_page->GetBucketPageId(bucket_id);
    if (bucket_page_id == INVALID_PAGE_ID) {
      //bucket不存在 插入bucket
      return InsertToNewBucket(directory_page, bucket_page_id, key, value);
    }
    WritePageGuard bucket_page_guard = bpm_->FetchPageWrite(bucket_page_id);
    ExtendibleHTableBucketPage<K, V, KC>* bucket_page = bucket_page_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
    if (bucket_page->Insert(key, value, cmp_)) {
      //正常插入
      return true;
    }
    //插入失败 bucket满了
    //是否需要增加全局深度了
    if (directory_page->GetGlobalDepth() == directory_page->GetLocalDepth(bucket_id)) {
      if (directory_page->GetGlobalDepth() >= directory_page->GetMaxDepth()) {
        //全局深度已经到达最大值
        return false;
      }
      directory_page->IncrGlobalDepth();
    }
    auto new_bucket_id = directory_page->GetSplitImageIndex(bucket_id);
    // std::cout << "bucket_id:" << bucket_id << std::endl;
    // std::cout << "new_bucket_id:" << new_bucket_id << std::endl;
    directory_page->IncrLocalDepth(bucket_id);
    directory_page->IncrLocalDepth(new_bucket_id);

    //新的bucket准备好了 对原来bucket里面的数据进行拆分
    page_id_t new_bucket_page_id = INVALID_PAGE_ID;
    WritePageGuard new_bucket_page_guard = this->bpm_->NewPageGuarded(&new_bucket_page_id).UpgradeWrite();
    if (new_bucket_page_id == INVALID_PAGE_ID) {
      return false;
    }
    ExtendibleHTableBucketPage<K, V, KC>* new_bucket_page = new_bucket_page_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
    new_bucket_page->Init(bucket_max_size_);
    directory_page->SetBucketPageId(new_bucket_id, new_bucket_page_id);
    directory_page->SetLocalDepth(new_bucket_id, directory_page->GetLocalDepth(bucket_id));
    // std::cout << "new_bucket_id:" << new_bucket_id << std::endl;
    // std::cout << "new_bucket_page_id:" << new_bucket_page_id << std::endl;
    // std::cout << "new_bucket_page_local_depth:" << directory_page->GetLocalDepth(new_bucket_id) << std::endl;
    //满的bucket数据提取
    std::list<std::pair<K, V>> temp_data;
    for (int i = 0; i < bucket_page->Size(); i++) {
      temp_data.emplace_back(bucket_page->EntryAt(i));
    }
    bucket_page->Clear();
    for (std::pair<K, V> data : temp_data) {
      //按深度增长后新的hash值把数据打散到两个bucket中
      page_id_t data_bucket_page_id = directory_page->GetBucketPageId(directory_page->HashToBucketIndex(Hash(data.first)));
      if (data_bucket_page_id == bucket_page_id) {
        bucket_page->Insert(data.first, data.second, cmp_);
      } else {
        new_bucket_page->Insert(data.first, data.second, cmp_);
      }
    }
    directory_page_guard.Drop();
    bucket_page_guard.Drop();
    //桶拆分过后重新插入
    return Insert(key, value, transaction);
  }

  template <typename K, typename V, typename KC>
  auto DiskExtendibleHashTable<K, V, KC>::InsertToNewDirectory(ExtendibleHTableHeaderPage* header, uint32_t directory_idx, uint32_t hash, const K& key, const V& value) -> bool {
    page_id_t directory_page_id = INVALID_PAGE_ID;
    WritePageGuard directory_guard = bpm_->NewPageGuarded(&directory_page_id).UpgradeWrite();
    auto directory_page = directory_guard.AsMut<ExtendibleHTableDirectoryPage>();
    directory_page->Init(directory_max_depth_);
    // std::cout << "************************" << std::endl;
    // std::cout << directory_idx << " " << directory_page_id << std::endl;
    header->SetDirectoryPageId(directory_idx, directory_page_id);
    uint32_t bucket_idx = directory_page->HashToBucketIndex(hash);
    return InsertToNewBucket(directory_page, bucket_idx, key, value);
  }

  template <typename K, typename V, typename KC>
  auto DiskExtendibleHashTable<K, V, KC>::InsertToNewBucket(ExtendibleHTableDirectoryPage* directory, uint32_t bucket_idx, const K& key, const V& value) -> bool {
    page_id_t bucket_page_id = INVALID_PAGE_ID;
    auto bucket_page_guard = bpm_->NewPageGuarded(&bucket_page_id).UpgradeWrite();
    auto bucket_page = bucket_page_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
    bucket_page->Init(this->bucket_max_size_);
    directory->SetBucketPageId(bucket_idx, bucket_page_id);
    //最后插入
    return bucket_page->Insert(key, value, cmp_);
  }

  template <typename K, typename V, typename KC>
  void DiskExtendibleHashTable<K, V, KC>::UpdateDirectoryMapping(ExtendibleHTableDirectoryPage* directory, uint32_t new_bucket_idx, page_id_t new_bucket_page_id, uint32_t new_local_depth, uint32_t local_depth_mask) {
    for (uint32_t i = 0; i < (1U << directory->GetGlobalDepth()); ++i) {
      //更新指向new_bucket_idx的所有目录项 并更新局部深度
      if (directory->GetBucketPageId(i) == directory->GetBucketPageId(new_bucket_idx)) {
        if (i & local_depth_mask) {
          // 如果这个目录项的在新局部深度位上的值为1，应该指向新桶, 否则，它仍然指向原桶
          directory->SetBucketPageId(i, new_bucket_page_id);
          directory->SetLocalDepth(i, new_local_depth);
        } else {
          directory->SetLocalDepth(i, new_local_depth);
        }
      }
    }

  }

  /*****************************************************************************
   * REMOVE
   *****************************************************************************/
  template <typename K, typename V, typename KC>
  auto DiskExtendibleHashTable<K, V, KC>::Remove(const K& key, Transaction* transaction) -> bool {
    auto head_page_guard = bpm_->FetchPageWrite(header_page_id_);
    auto head_page = head_page_guard.As<ExtendibleHTableHeaderPage>();
    auto hash_value = this->Hash(key);
    //找到dir page id
    page_id_t directory_page_id = head_page->GetDirectoryPageId(head_page->HashToDirectoryIndex(hash_value));
    head_page_guard.Drop();
    if (directory_page_id == INVALID_PAGE_ID) {
      return false;
    }
    //获得dir page
    auto directory_page_guard = bpm_->FetchPageWrite(directory_page_id);
    auto directory_page = directory_page_guard.AsMut<ExtendibleHTableDirectoryPage>();

    //找到bucket page id
    page_id_t bucket_page_id = directory_page->GetBucketPageId(directory_page->HashToBucketIndex(hash_value));
    directory_page_guard.Drop();
    if (bucket_page_id == INVALID_PAGE_ID) {
      return false;
    }
    //获得bucket page
    auto bucket_page_guard = bpm_->FetchPageWrite(bucket_page_id);
    ExtendibleHTableBucketPage<K, V, KC>* bucket_page = bucket_page_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
    if (!bucket_page->Remove(key, cmp_)) {
      return false;
    }
    //删除了key后判断是否需要收缩
    bucket_page_guard.Drop();
    //收缩条件:被删除的bucket和与他分裂出的bucket有一个是空 且两个bucket的局部深度相同
    uint32_t bucket_id = directory_page->HashToBucketIndex(hash_value);
    uint32_t local_depth = directory_page->GetLocalDepth(bucket_id);
    uint32_t global_depth = directory_page->GetGlobalDepth();

    while (local_depth > 0) {
      uint32_t depth_mask = 1 << (local_depth - 1);
      auto bucket_id_1 = bucket_id;
      auto bucket_id_2 = bucket_id ^ depth_mask;
      //判断二者的深度相等且不为0 
      if (directory_page->GetLocalDepth(bucket_id_1) == 0 || directory_page->GetLocalDepth(bucket_id_2) == 0) {
        break;
      }
      if (directory_page->GetLocalDepth(bucket_id_1) != directory_page->GetLocalDepth(bucket_id_2)) {
        break;
      }
      auto bucket_page_guard_1 = bpm_->FetchPageRead(directory_page->GetBucketPageId(bucket_id_1));
      auto bucket_page_guard_2 = bpm_->FetchPageRead(directory_page->GetBucketPageId(bucket_id_2));
      const ExtendibleHTableBucketPage<K, V, KC>* bucket_page_1 = bucket_page_guard_1.As<ExtendibleHTableBucketPage<K, V, KC>>();
      const ExtendibleHTableBucketPage<K, V, KC>* bucket_page_2 = bucket_page_guard_2.As<ExtendibleHTableBucketPage<K, V, KC>>();
      //判断二者是不是有一个为空
      if (!bucket_page_1->IsEmpty() && !bucket_page_2->IsEmpty()) {
        break;
      }
      //满足条件，则进行收缩
      if (bucket_page_1->IsEmpty()) {
        bpm_->DeletePage(directory_page->GetBucketPageId(bucket_id_1));
        directory_page->SetBucketPageId(bucket_id_1, directory_page->GetBucketPageId(bucket_id_2));
        directory_page->DecrLocalDepth(bucket_id_2);
      } else {
        bpm_->DeletePage(directory_page->GetBucketPageId(bucket_id_2));
        directory_page->SetBucketPageId(bucket_id_2, directory_page->GetBucketPageId(bucket_id_1));
        directory_page->DecrLocalDepth(bucket_id_1);
      }
      bucket_page_guard_1.Drop();
      bucket_page_guard_2.Drop();


    }
    //判断是否需要全局收缩
    while (directory_page->CanShrink()) {
      directory_page->DecrGlobalDepth();
    }

    return true;
  }

  template class DiskExtendibleHashTable<int, int, IntComparator>;
  template class DiskExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
  template class DiskExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
  template class DiskExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
  template class DiskExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
  template class DiskExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
