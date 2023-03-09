//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru__k_replacer.cpp
//
// Identification: src/buffer/lru__k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  if (!fifo_.empty()) {
    for (auto it = fifo_.begin(); it != fifo_.end(); ++it) {
      if (id2frame_[*it].evitable_) {
        *frame_id = *it;
        id2frame_.erase(*it);
        --curr_size_;
        fifo_.erase(it);
        return true;
      }
    }
  }
  if (!lru_.empty()) {
    for (auto it = lru_.begin(); it != lru_.end(); ++it) {
      if (id2frame_[*it].evitable_) {
        *frame_id = *it;
        id2frame_.erase(*it);
        --curr_size_;
        lru_.erase(it);
        return true;
      }
    }
  }
  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  if (frame_id > static_cast<int>(replacer_size_)) {
    throw std::exception();
  }
  size_t cur_n_access = ++id2frame_[frame_id].n_access_;
  if (cur_n_access == 1) {
    ++curr_size_;
    fifo_.emplace_back(frame_id);
    auto it = fifo_.end();
    id2frame_[frame_id].p_ = --it;
  } else {
    if (cur_n_access == k_) {
      fifo_.erase(id2frame_[frame_id].p_);
      lru_.emplace_back(frame_id);
      auto it = lru_.end();
      id2frame_[frame_id].p_ = --it;
    } else if (cur_n_access > k_) {
      lru_.erase(id2frame_[frame_id].p_);
      lru_.emplace_back(frame_id);
      auto it = lru_.end();
      id2frame_[frame_id].p_ = --it;
    }
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::scoped_lock<std::mutex> lock(latch_);
  if (frame_id > static_cast<int>(replacer_size_)) {
    throw std::exception();
  }
  if (id2frame_.find(frame_id) == id2frame_.end()) {
    return;
  }
  if (set_evictable ^ id2frame_[frame_id].evitable_) {
    if (set_evictable) {
      ++curr_size_;
    } else {
      --curr_size_;
    }
  }
  id2frame_[frame_id].evitable_ = set_evictable;
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  if (id2frame_.find(frame_id) == id2frame_.end()) {
    return;
  }
  if (!id2frame_[frame_id].evitable_) {
    throw std::exception();
  }
  if (id2frame_[frame_id].n_access_ < k_) {
    fifo_.erase(id2frame_[frame_id].p_);
  } else {
    lru_.erase(id2frame_[frame_id].p_);
  }
  --curr_size_;
  id2frame_.erase(frame_id);
}

auto LRUKReplacer::Size() -> size_t {
  std::scoped_lock<std::mutex> lock(latch_);

  return curr_size_;
}

}  // namespace bustub
