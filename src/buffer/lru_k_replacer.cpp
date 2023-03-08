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
//   auto tree_it = tree.begin();
  // std::cout << "evicting, tree size = " << tree.size() << std::endl;
//   while (tree_it != tree.end()) {
//     std::cout << tree_it->id << std::endl;
//     ++tree_it;
//   }
  if (curr_size_ > 0) {
    auto it = tree.begin();
    while (it != tree.end()) {
    //   auto &frame = *it;
    //   auto id = frame.id;
    //   std::cout << it->id << ' ' << it->evictable << std::endl;
      auto id = it->id;

      if ((*id2it[id]).evictable == false) {
        ++it;
        continue;
      }
    //   std::cout << id << std::endl;
      *frame_id = id;
    //   if (id2it[id])
      lru.erase(id2it[id]);
      id2it.erase(id);
      tree.erase(it);
      
      --curr_size_;
      return true;
    }
    return false;
  std::cout << "evict failed\n";

  } else {
  std::cout << "evict failed\n";

    return false;
  }
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  if (static_cast<size_t>(frame_id) > replacer_size_) throw std::exception();
  std::scoped_lock<std::mutex> lock(latch_);
  auto now = std::chrono::system_clock::now();
  auto now_ms = std::chrono::time_point_cast<std::chrono::milliseconds>(now);
  auto value = now_ms.time_since_epoch();
  size_t duration = value.count();
  std::cout << "record access\n";
  if (id2it.find(frame_id) == id2it.end()) {
    auto frm = frame(k_);
    frm.access_rec[0] = duration;
    frm.id = frame_id;
    ++frm.cur;
    ++frm.n_access;
    tree.insert(frm);
    lru.push_back(std::move(frm));
    auto it = lru.end();
    id2it[frame_id] = --it;
    if (it == lru.begin()) {
        std::cout << "first frame\n";
    }
    // ++curr_size_;
  } else {
    auto &frame = *id2it[frame_id];
    if (frame.access_rec.size() < static_cast<size_t>(frame.k)) {
    //   frame.access_rec[frame.cur++] = duration;
      auto it = tree.lower_bound(frame);
      while ((*it).id != frame.id) ++it;
      tree.erase(it);
      frame.access_rec.push_back(duration);
      ++frame.cur;
      ++frame.n_access;
      tree.insert(frame);
    } else {
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
  if (static_cast<size_t>(frame_id) > replacer_size_) throw std::exception();
  std::scoped_lock<std::mutex> lock(latch_);
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
  if (static_cast<size_t>(frame_id) > replacer_size_) throw std::exception();
  std::scoped_lock<std::mutex> lock(latch_);
  if (id2it.find(frame_id) == id2it.end()) return;
  auto it = id2it[frame_id];
  auto &frm = *it;
  if (frm.evictable == false) throw std::exception();
  lru.erase(it);
  id2it.erase(frame_id);
  auto tree_it = tree.lower_bound(frm);
  while (tree_it->id != frame_id) ++tree_it;
  tree.erase(tree_it);
}

auto LRUKReplacer::Size() -> size_t {
  std::scoped_lock<std::mutex> lock(latch_);
  return curr_size_;
}

}  // namespace bustub
