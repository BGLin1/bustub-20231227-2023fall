//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {
  using std::cout;
  using std::endl;
  BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager* disk_manager, size_t replacer_k,
    LogManager* log_manager)
    : pool_size_(pool_size), disk_scheduler_(std::make_unique<DiskScheduler>(disk_manager)), log_manager_(log_manager) {

    // we allocate a consecutive memory space for the buffer pool
    pages_ = new Page[pool_size_];
    replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

    // Initially, every page is in the free list.
    for (size_t i = 0; i < pool_size_; ++i) {
      free_list_.emplace_back(static_cast<int>(i));
    }
  }

  BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

  auto BufferPoolManager::NewPage(page_id_t* page_id) -> Page* {
    std::unique_lock<std::mutex> lock(this->latch_);
    frame_id_t frame_id;
    if (!free_list_.empty()) {
      //还有空闲的frame
      frame_id = this->free_list_.front();
      this->free_list_.pop_front();
    } else if (replacer_->Size() > 0) {
      //没有空闲的frame 但是存在可以驱逐的frame
      replacer_->Evict(&frame_id);
    } else {
      //没有空闲的frame 也不存在可以驱逐的frame
      cout<< "creat page error: no free frame and no evictable frame" << endl;
      return nullptr;
    }
    //确认了新page的frame_id
    //如果这个frame是一个脏页
    if (this->pages_[frame_id].is_dirty_) {
      auto promise = this->disk_scheduler_->CreatePromise();
      auto future = promise.get_future();
      this->disk_scheduler_->Schedule(
        { .is_write_ = true,
          .data_ = this->pages_[frame_id].data_,
        .page_id_ = this->pages_[frame_id].page_id_,
        .callback_ = std::move(promise)
        });
      future.get();
      this->pages_[frame_id].is_dirty_ = false;
    }
    //为新的page分配page_id
    *page_id = AllocatePage();

    //更新page_table
    this->page_table_.erase(pages_[frame_id].page_id_);
    this->page_table_.emplace(*page_id, frame_id);

    //更新page本身
    pages_[frame_id].page_id_ = *page_id;
    pages_[frame_id].pin_count_ = 1;

    //为新page清空内存
    pages_[frame_id].ResetMemory();

    //防止新创建的page被迅速驱除先标记为false (也就是pin操作)
    this->replacer_->RecordAccess(frame_id);
    this->replacer_->SetEvictable(frame_id, false);
    
    cout << "New page created with page_id: " << *page_id << " in frame:" << frame_id << endl;
    return &pages_[frame_id];
  }


  auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page* {
    std::unique_lock<std::mutex> lock(this->latch_);
    // First search for page_id in the buffer pool
    if (auto frame_iter = this->page_table_.find(page_id);frame_iter != this->page_table_.end()) {
      auto frame_id = frame_iter->second;
      //frame中存在页
      this->replacer_->SetEvictable(frame_id, false);
      this->replacer_->RecordAccess(frame_id);
      this->pages_[frame_id].pin_count_++;
      cout << "Page fetched with page_id: " << page_id << " in frame:" << frame_id << endl;
      return &this->pages_[frame_id];
    }
    //frame中不存在的页
    frame_id_t frame_id;
    if (!free_list_.empty()) {
      //还有空闲的frame
      frame_id = this->free_list_.front();
      this->free_list_.pop_front();
    } else if (replacer_->Size() > 0) {
      //没有空闲的frame 但是存在可以驱逐的frame
      replacer_->Evict(&frame_id);
    } else {
      //没有空闲的frame 也不存在可以驱逐的frame
      return nullptr;
    }
    //确认了新page的frame_id
    //如果这个frame是一个脏页
    if (this->pages_[frame_id].is_dirty_) {
      auto promise = this->disk_scheduler_->CreatePromise();
      auto future = promise.get_future();
      this->disk_scheduler_->Schedule(
        { .is_write_ = true,
          .data_ = this->pages_[frame_id].data_,
        .page_id_ = this->pages_[frame_id].page_id_,
        .callback_ = std::move(promise)
        });
      future.get();
      this->pages_[frame_id].is_dirty_ = false;
    }

    //更新page_table
    this->page_table_.erase(pages_[frame_id].page_id_);
    this->page_table_.emplace(page_id, frame_id);

    //更新page本身
    pages_[frame_id].page_id_ = page_id;
    pages_[frame_id].ResetMemory();


    //防止新创建的page被迅速驱除先标记为false (也就是pin操作)
    pages_[frame_id].pin_count_ = 1;
    this->replacer_->RecordAccess(frame_id);
    this->replacer_->SetEvictable(frame_id, false);
    //把数据从磁盘读入内存
    auto promise = this->disk_scheduler_->CreatePromise();
    auto future = promise.get_future();
    this->disk_scheduler_->Schedule(
      { .is_write_ = false,
        .data_ = this->pages_[frame_id].data_,
      .page_id_ = this->pages_[frame_id].page_id_,
      .callback_ = std::move(promise)
      });
    future.get();
    this->pages_[frame_id].is_dirty_ = false;
    cout << "Page fetched with page_id: " << page_id << " in frame:" << frame_id << endl;
    return &pages_[frame_id];
  }

  auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
    std::unique_lock<std::mutex> lock(this->latch_);
    if (page_id == INVALID_PAGE_ID) {
      return false;
    }
    // First search for page_id in the buffer pool
    if (auto frame_iter = this->page_table_.find(page_id);frame_iter != this->page_table_.end()) {
      auto frame_id = frame_iter->second;
      if (this->pages_[frame_id].pin_count_ == 0) {
        return false;
      }
      if (is_dirty) {
        this->pages_[frame_id].is_dirty_ = is_dirty;
      }
      this->pages_[frame_id].pin_count_--;
      if (this->pages_[frame_id].pin_count_ == 0) {
        this->replacer_->SetEvictable(frame_id, true);
      }
      cout << "Page unpinned with page_id: " << page_id << " in frame:" << frame_id << endl;
      return true;
    }
    return false;
  }

  auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
    std::unique_lock<std::mutex> lock(this->latch_);
    if (page_id == INVALID_PAGE_ID) {
      return false;
    }
    if (auto frame_iter = this->page_table_.find(page_id);frame_iter != this->page_table_.end()) {
      auto frame_id = frame_iter->second;
      auto promise = this->disk_scheduler_->CreatePromise();
      auto future = promise.get_future();
      this->disk_scheduler_->Schedule(
        { .is_write_ = true,
          .data_ = this->pages_[frame_id].data_,
        .page_id_ = this->pages_[frame_id].page_id_,
        .callback_ = std::move(promise)
        });
      future.get();
      this->pages_[frame_id].is_dirty_ = false;
      cout << "Page flushed with page_id: " << page_id << " in frame:" << frame_id << endl;
      return true;
    }
    return false;
  }

  void BufferPoolManager::FlushAllPages() {
    std::unique_lock<std::mutex> lock(this->latch_);
    for (size_t i = 0;i < this->pool_size_;i++) {
      lock.unlock();
      FlushPage(i);
      lock.lock();
    }
  }

  auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
    if (page_id == INVALID_PAGE_ID) {
      return false;
    }
    std::unique_lock<std::mutex> lock(this->latch_);
    if (page_table_.find(page_id) != page_table_.end()) {
      auto frame_id = page_table_[page_id];
      if (this->pages_[frame_id].GetPinCount() > 0) {
        return false;
      }
      //开始清除动作
      page_table_.erase(page_id);
      free_list_.push_back(frame_id);
      replacer_->Remove(frame_id);
      pages_[frame_id].ResetMemory();
      pages_[frame_id].is_dirty_ = false;
      pages_[frame_id].pin_count_ = 0;
    }
    DeallocatePage(page_id);
    cout << "Page deleted with page_id: " << page_id << endl;
    return true;

  }

  auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

  auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard {
    return { this, this->FetchPage(page_id) };
  }

  auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
    Page* page = this->FetchPage(page_id);
    page->RLatch();
    return { this, page };
  }

  auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
    Page* page = this->FetchPage(page_id);
    page->WLatch();
    return { this, page };
  }

  auto BufferPoolManager::NewPageGuarded(page_id_t* page_id) -> BasicPageGuard {
    return { this, this->NewPage(page_id)};
  }

}  // namespace bustub
