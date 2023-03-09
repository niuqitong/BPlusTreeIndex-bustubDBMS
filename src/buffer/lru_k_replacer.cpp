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
#include <iostream>
// #include <mutex>
namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}
// LRUKReplacer::~LRUKReplacer() {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);

  if (curr_size_ > 0) {
    auto it = tree.begin();
    while (it != tree.end()) {
      auto id = it->id;

      if ((*id2it[id]).evictable == false) {
        ++it;
        continue;
      }
      *frame_id = id;
      lru.erase(id2it[id]);
      id2it.erase(id);
      tree.erase(it);

      --curr_size_;
      return true;
    }
    return false;
  } else {
    return false;
  }
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);

  if (static_cast<size_t>(frame_id) > replacer_size_) throw std::exception();
  auto now = std::chrono::system_clock::now();
  auto now_ms = std::chrono::time_point_cast<std::chrono::milliseconds>(now);
  auto value = now_ms.time_since_epoch();
  size_t duration = value.count();
  if (id2it.find(frame_id) == id2it.end()) {
    auto frm = frame(k_);
    frm.access_rec[0] = duration;
    frm.id = frame_id;
    // ++frm.cur;
    ++frm.n_access;
    tree.insert(frm);
    lru.push_back(frm);
    auto it = lru.end();
    id2it[frame_id] = --it;
  } else {
    auto &frame = *id2it[frame_id];
    if (frame.access_rec.size() < static_cast<size_t>(k_)) {
      auto it = tree.lower_bound(frame);
      while ((*it).id != frame.id) ++it;
      tree.erase(it);
      frame.access_rec.push_back(duration);
      // ++frame.cur;
      ++frame.n_access;
      tree.insert(frame);
    } else {
      // if (frame.access_rec.size() == static_cast<size_t>(k_)) {

      // }
      auto it = tree.lower_bound(frame);
      while ((*it).id != frame.id) ++it;
      tree.erase(it);
      frame.cur = (frame.cur) % frame.k;
      frame.access_rec[frame.cur] = duration;
      frame.earliest = (frame.cur + 1) % frame.k;
      ++frame.cur;
      ++frame.n_access;
      tree.insert(frame);
    }
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::scoped_lock<std::mutex> lock(latch_);

  if (static_cast<size_t>(frame_id) > replacer_size_) throw std::exception();
  if (id2it.find(frame_id) == id2it.end()) throw std::exception();
  auto &frame = *id2it[frame_id];
  if (frame.evictable ^ set_evictable) {
    if (set_evictable)
      ++curr_size_;
    else
      --(curr_size_);
  }

  frame.evictable = set_evictable;
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);

  if (static_cast<size_t>(frame_id) > replacer_size_) throw std::exception();
  if (id2it.find(frame_id) == id2it.end()) return;
  auto it = id2it[frame_id];
  // if (it->evictable)
  //   --curr_size_;
  auto &frm = *it;
  if (frm.evictable == false) throw std::exception();
  --curr_size_;
  auto tree_it = tree.lower_bound(frm);
  while (tree_it != tree.end() && tree_it->id != frame_id) ++tree_it;
  tree.erase(tree_it);
  lru.erase(it);
  id2it.erase(frame_id);
}

auto LRUKReplacer::Size() -> size_t {
  std::scoped_lock<std::mutex> lock(latch_);
  return curr_size_;
}

}  // namespace bustub
