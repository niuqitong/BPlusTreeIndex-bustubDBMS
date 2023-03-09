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

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
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
  std::scoped_lock lock(latch_);
  frame_id_t fid;
  if (!GetAvailableFrame(&fid)) {
    page_id = nullptr;
    return nullptr;
  }
  pages_[fid].page_id_ = AllocatePage();
  pages_[fid].ResetMemory();
  pages_[fid].pin_count_ = 1;
  page_table_->Insert(pages_[fid].page_id_, fid);
  replacer_->RecordAccess(fid);
  replacer_->SetEvictable(fid, false);
 
  *page_id = pages_[fid].page_id_;
  return &pages_[fid];
 }

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * { 
  std::scoped_lock lock(latch_);
  frame_id_t fid;
  // requested page already in buffer pool
  if (page_table_->Find(page_id, fid)) {
    ++pages_[fid].pin_count_;
    replacer_->RecordAccess(fid);
    replacer_->SetEvictable(fid, false);
    return &pages_[fid];
  }
  if (!GetAvailableFrame(&fid)) {
    return nullptr;
  }
  pages_[fid].page_id_ = page_id;
  pages_[fid].ResetMemory();
  pages_[fid].pin_count_ = 1;
  page_table_->Insert(page_id, fid);
  disk_manager_->ReadPage(page_id, pages_[fid].data_);
  replacer_->RecordAccess(fid);
  replacer_->SetEvictable(fid, false);
  return &pages_[fid];
 }

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool { 
  std::scoped_lock lock(latch_);
  frame_id_t fid;
  if (!page_table_->Find(page_id, fid) || pages_[fid].pin_count_ <= 0) {
    return false;
  }
  --pages_[fid].pin_count_;
  if (pages_[fid].pin_count_ == 0) {
    replacer_->SetEvictable(fid, true);
  }
  // an already dirty page can't be marked as not dirty
  if (!pages_[fid].is_dirty_) {
    pages_[fid].is_dirty_ = is_dirty;
  }
  return true;
 }

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool { 
  std::scoped_lock lock(latch_);
  frame_id_t fid;
  if (!page_table_->Find(page_id, fid)) {
    return false;
  }
  disk_manager_->WritePage(pages_[fid].page_id_, pages_[fid].data_);
  pages_[fid].is_dirty_ = false;
  return true;
 }

void BufferPoolManagerInstance::FlushAllPgsImp() {
  std::scoped_lock lock(latch_);
  // pages_ is a pointer-form array, can't use range-for
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].page_id_ != INVALID_PAGE_ID) {
      disk_manager_->WritePage(pages_[i].page_id_, pages_[i].data_);
      pages_[i].is_dirty_ = false;
    }
  }
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool { 
  std::scoped_lock lock(latch_);
  frame_id_t fid;
  if (!page_table_->Find(page_id, fid)) {
    return true;
  }
  if (pages_[fid].pin_count_ > 0) {
    return false;
  }
  if (pages_[fid].is_dirty_) {
    disk_manager_->WritePage(pages_[fid].page_id_, pages_[fid].data_);
  }
  pages_[fid].page_id_ = INVALID_PAGE_ID;
  pages_[fid].ResetMemory();
  pages_[fid].pin_count_ = 0;
  pages_[fid].is_dirty_ = false;
  page_table_->Remove(page_id);
  replacer_->Remove(fid);
  free_list_.emplace_back(fid);
  DeallocatePage(page_id);
  return true;
 }

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManagerInstance::GetAvailableFrame(frame_id_t *out_frame_id) -> bool {
  frame_id_t fid;
  // there is free frame remaining, get first of them
  if (!free_list_.empty()) {
    fid = free_list_.front();
    free_list_.pop_front();
    *out_frame_id = fid;
    return true;
  }
  // no free frame, find a replacement
  if (replacer_->Evict(&fid)) {
    // write back to disk if the page is dirty
    if (pages_[fid].is_dirty_) {
      disk_manager_->WritePage(pages_[fid].page_id_, pages_[fid].data_);
      pages_[fid].is_dirty_ = false;
    }
    page_table_->Remove(pages_[fid].page_id_);
    *out_frame_id = fid;
    return true;
  }
  return false;
}

}  // namespace bustub
