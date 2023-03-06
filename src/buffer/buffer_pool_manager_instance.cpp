//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/exception.h"
#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(
  size_t pool_size, DiskManager *disk_manager, size_t replacer_k, LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
    // 编译时可以确定的类型转换才能使用
  }

  // TODO(students): remove this line after you have implemented the buffer pool manager
  // throw NotImplementedException(
  //     "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
  //     "exception line in `buffer_pool_manager_instance.cpp`.");
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * { 
  std::scoped_lock<std::mutex> lock(latch_);
  if (!free_list_.empty()) {
    auto& frame_id = *free_list_.begin();
    free_list_.pop_front();
    replacer_->SetEvictable(frame_id, false);
    replacer_->RecordAccess(frame_id);
    *page_id = AllocatePage();
    page_table_->Insert(*page_id, frame_id);
    auto& p = pages_[frame_id];
    p.page_id_ = *page_id;
    p.pin_count_ = 1;
    p.is_dirty_ = false;
    p.ResetMemory();
    return &pages_[frame_id];
  } else {
    frame_id_t evited_frame_id;
    if (replacer_->Evict(&evited_frame_id) == false) {
      return nullptr;
    } else {
      replacer_->SetEvictable(evited_frame_id, false);
      replacer_->RecordAccess(evited_frame_id);
      Page& p = pages_[evited_frame_id];
      *page_id = AllocatePage();
      if (p.IsDirty()) {
        disk_manager_->WritePage(p.GetPageId(), p.GetData());
      }
      p.page_id_ = *page_id;
      p.pin_count_ = 1;
      p.is_dirty_ = false;
      p.ResetMemory();
      return &pages_[evited_frame_id];
    }
  }
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * { return nullptr; }

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool { return false; }

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool { return false; }

void BufferPoolManagerInstance::FlushAllPgsImp() {}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool { return false; }

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

}  // namespace bustub
