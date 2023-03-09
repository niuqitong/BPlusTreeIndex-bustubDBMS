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

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  if (!fifo.empty()) {
    for (auto it = fifo.begin(); it != fifo.end(); ++it) {
      if (id2frame[*it].evitable) {
        *frame_id = *it;
        id2frame.erase(*it);
        --curr_size_;
        fifo.erase(it);
        return true;
      }
    }
  }
  if (!lru.empty()) {
    for (auto it = lru.begin(); it != lru.end(); ++it) {
      if (id2frame[*it].evitable) {
        *frame_id = *it;
        id2frame.erase(*it);
        --curr_size_;
        lru.erase(it);
        return true;
      }
    }
  }
  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  if (frame_id > static_cast<int>(replacer_size_)) throw std::exception();
  size_t cur_n_access = ++id2frame[frame_id].n_access;
  if (cur_n_access == 1) {
    ++curr_size_;
    fifo.emplace_back(frame_id);
    auto it = fifo.end();
    id2frame[frame_id].p = --it;
  } else {
    if (cur_n_access == k_) {
      fifo.erase(id2frame[frame_id].p);
      lru.emplace_back(frame_id);
      auto it = lru.end();
      id2frame[frame_id].p = --it;
    } else if (cur_n_access > k_) {
      lru.erase(id2frame[frame_id].p);
      lru.emplace_back(frame_id);
      auto it = lru.end();
      id2frame[frame_id].p = --it;
    }
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::scoped_lock<std::mutex> lock(latch_);
  if (frame_id > static_cast<int>(replacer_size_)) throw std::exception();
  if (id2frame.find(frame_id) == id2frame.end()) return;
  if (set_evictable ^ id2frame[frame_id].evitable) {
    if (set_evictable) {
      ++curr_size_;
    } else {
      --curr_size_;
    }
  }
  id2frame[frame_id].evitable = set_evictable;
}

void LRUKReplacer::Remove(frame_id_t frame_id) { 
  std::scoped_lock<std::mutex> lock(latch_); 
  if (id2frame.find(frame_id) == id2frame.end())
    return;
  if (!id2frame[frame_id].evitable)
    throw std::exception();
  if (id2frame[frame_id].n_access < k_) {
    fifo.erase(id2frame[frame_id].p);
  } else {
    lru.erase(id2frame[frame_id].p);
  }
  --curr_size_;
  id2frame.erase(frame_id);
  }

auto LRUKReplacer::Size() -> size_t {
  std::scoped_lock<std::mutex> lock(latch_);

  return curr_size_;
}

}  // namespace bustub
