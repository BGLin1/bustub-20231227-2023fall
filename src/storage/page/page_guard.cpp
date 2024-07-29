#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept {
    this->bpm_ = that.bpm_;
    this->page_ = that.page_;
    this->is_dirty_ = that.is_dirty_;
    that.bpm_ = nullptr;
    that.page_ = nullptr;
    that.is_dirty_ = false;
}

void BasicPageGuard::Drop() {
    if (this->bpm_ == nullptr || this->page_ == nullptr) {
        return;
    }
    this->bpm_->UnpinPage(this->PageId(), this->is_dirty_);
    this->bpm_ = nullptr;
    this->page_ = nullptr;
    this->is_dirty_ = false;

}

auto BasicPageGuard::operator=(BasicPageGuard&& that) noexcept -> BasicPageGuard& {
    if (this == &that) {
        return *this;
    }
    this->Drop();
    this->bpm_ = that.bpm_;
    this->page_ = that.page_;
    this->is_dirty_ = that.is_dirty_;
    that.bpm_ = nullptr;
    that.page_ = nullptr;
    that.is_dirty_ = false;
    return *this;
}

BasicPageGuard::~BasicPageGuard() {
    this->Drop();
};  // NOLINT

auto BasicPageGuard::UpgradeRead() -> ReadPageGuard {
    if (this->page_ != nullptr) {
        this->page_->RLatch();
    }
    return { this->bpm_,this->page_ };
}

auto BasicPageGuard::UpgradeWrite() -> WritePageGuard {
    if (this->page_ != nullptr) {
        this->page_->WLatch();
    }
    return { this->bpm_,this->page_ };
}

ReadPageGuard::ReadPageGuard(ReadPageGuard&& that) noexcept : guard_(std::move(that.guard_)) {};

auto ReadPageGuard::operator=(ReadPageGuard&& that) noexcept -> ReadPageGuard& {
    if (this == &that) {
        return *this;
    }
    this->guard_ = std::move(that.guard_);
    return *this;
}

void ReadPageGuard::Drop() {
    if (this->guard_.page_ != nullptr) {
        this->guard_.page_->RUnlatch();
    }
    this->guard_.Drop();
}

ReadPageGuard::~ReadPageGuard() {
    this->Drop();
}  // NOLINT

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept : guard_(std::move(that.guard_)) {};

auto WritePageGuard::operator=(WritePageGuard&& that) noexcept -> WritePageGuard& {
    if (this == &that) {
        return *this;
    }
    this->guard_ = std::move(that.guard_);
    return *this;
}

void WritePageGuard::Drop() {
     if (this->guard_.page_ != nullptr) {
        this->guard_.page_->WUnlatch();
    }
    this->guard_.Drop();
}

WritePageGuard::~WritePageGuard() {
    this->Drop();
}  // NOLINT

}  // namespace bustub
