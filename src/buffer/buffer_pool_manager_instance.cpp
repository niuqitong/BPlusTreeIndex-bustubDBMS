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

void BufferPoolManagerInstance::page_reset(frame_id_t fid, page_id_t pgid) {
  auto& p = pages_[fid];
  p.pin_count_ = 1;
  p.is_dirty_ = false;
  p.page_id_ = pgid;
  p.ResetMemory();

  page_table_->Insert(pgid, fid);
  replacer_->SetEvictable(fid, false);
  replacer_->RecordAccess(fid);

}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * { 
  std::scoped_lock<std::mutex> lock(latch_);
  if (!free_list_.empty()) {
    auto& frame_id = *free_list_.begin();
    free_list_.pop_front();
    *page_id = AllocatePage();
    page_reset(frame_id, *page_id);
    return &pages_[frame_id];
  } else {
    frame_id_t evited_frame_id;
    if (replacer_->Evict(&evited_frame_id) == false) {
      return nullptr;
    } else {
      Page& p = pages_[evited_frame_id];
      *page_id = AllocatePage();
      if (p.IsDirty()) 
        disk_manager_->WritePage(p.GetPageId(), p.GetData());
      
      page_reset(evited_frame_id, *page_id);
      return &pages_[evited_frame_id];
    }
  }
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * { 
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t fid;
  bool found = page_table_->Find(page_id, fid);
  if (found == false) {
    if (!free_list_.empty()) {
      fid = *free_list_.begin();
      free_list_.pop_front();
      page_reset(fid, page_id);
      disk_manager_->ReadPage(page_id, pages_[fid].data_);
      return &pages_[fid];

    } else if (replacer_->Evict(&fid)) {
      Page& p = pages_[fid];
      if (p.IsDirty()) 
        disk_manager_->WritePage(p.GetPageId(), p.GetData());
      
      page_reset(fid, p.page_id_);
      disk_manager_->ReadPage(page_id, pages_[fid].data_);
      return &pages_[fid];
    } else return nullptr;

  } else {
    return &pages_[fid];
  }

 }

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool { 
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t fid;
  if (page_table_->Find(page_id, fid) == false)
    return false;
  if (pages_[fid].GetPinCount() <= 0)
    return false;
  --pages_[fid].pin_count_;
  if (pages_[fid].GetPinCount() == 0) {
    replacer_->SetEvictable(fid, true);
  }
  pages_[fid].is_dirty_ = is_dirty;
  return true;
 }

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool { 
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t fid;
  if (page_table_->Find(page_id, fid) == false)
    return false;
  disk_manager_->WritePage(page_id, pages_[fid].data_);
  pages_[fid].is_dirty_ = false;
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  std::scoped_lock<std::mutex> lock(latch_);
  auto cur = page_table_->get_begin_it();
  auto last = page_table_->get_end_it();
  if (cur == last)
    return;
  while (cur != last) {
    disk_manager_->WritePage(cur->first, pages_[cur->second].data_);
    pages_[cur->second].is_dirty_ = false;
    ++cur;
  }
  

}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t fid;
  if (page_table_->Find(page_id, fid) == false)
    return true;
  if (pages_[fid].GetPinCount() > 0)
    return false;
  replacer_->Remove(fid);
  page_table_->Remove(page_id);
  free_list_.push_back(fid);
  page_reset(fid, page_id);
  pages_[fid].pin_count_ = 0;
  DeallocatePage(page_id);
  return true;
 }

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

}  // namespace bustub
