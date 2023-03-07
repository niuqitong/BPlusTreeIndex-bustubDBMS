//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.h
//
// Identification: src/include/buffer/lru_k_replacer.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <limits>
#include <list>
#include <set>
#include <mutex>  // NOLINT
#include <unordered_map>
#include <vector>
#include <exception>
#include <chrono>
#include <memory>

#include "common/config.h"
#include "common/macros.h"

namespace bustub {

/**
 * LRUKReplacer implements the LRU-k replacement policy.
 *
 * The LRU-k algorithm evicts a frame whose backward k-distance is maximum
 * of all frames. Backward k-distance is computed as the difference in time between
 * current timestamp and the timestamp of kth previous access.
 *
 * A frame with less than k historical references is given
 * +inf as its backward k-distance. When multiple frames have +inf backward k-distance,
 * classical LRU algorithm is used to choose victim.
 */
class LRUKReplacer {
  friend class BufferPoolManagerInstance;

 public:
  /**
   *
   * TODO(P1): Add implementation
   *
   * @brief a new LRUKReplacer.
   * @param num_frames the maximum number of frames the LRUReplacer will be required to store
   */
  explicit LRUKReplacer(size_t num_frames, size_t k);

  DISALLOW_COPY_AND_MOVE(LRUKReplacer);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Destroys the LRUReplacer.
   */
  ~LRUKReplacer() = default;

  /**
   * TODO(P1): Add implementation
   *
   * @brief Find the frame with largest backward k-distance and evict that frame. Only frames
   * that are marked as 'evictable' are candidates for eviction.
   *
   * A frame with less than k historical references is given +inf as its backward k-distance.
   * If multiple frames have inf backward k-distance, then evict the frame with the earliest
   * timestamp overall.
   *
   * Successful eviction of a frame should decrement the size of replacer and remove the frame's
   * access history.
   *
   * @param[out] frame_id id of frame that is evicted.
   * @return true if a frame is evicted successfully, false if no frames can be evicted.
   */
  auto Evict(frame_id_t *frame_id) -> bool;

  /**
   * TODO(P1): Add implementation
   *
   * @brief Record the event that the given frame id is accessed at current timestamp.
   * Create a new entry for access history if frame id has not been seen before.
   *
   * If frame id is invalid (ie. larger than replacer_size_), throw an exception. You can
   * also use BUSTUB_ASSERT to abort the process if frame id is invalid.
   *
   * @param frame_id id of frame that received a new access.
   */
  void RecordAccess(frame_id_t frame_id);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Toggle whether a frame is evictable or non-evictable. This function also
   * controls replacer's size. Note that size is equal to number of evictable entries.
   *
   * If a frame was previously evictable and is to be set to non-evictable, then size should
   * decrement. If a frame was previously non-evictable and is to be set to evictable,
   * then size should increment.
   *
   * If frame id is invalid, throw an exception or abort the process.
   *
   * For other scenarios, this function should terminate without modifying anything.
   *
   * @param frame_id id of frame whose 'evictable' status will be modified
   * @param set_evictable whether the given frame is evictable or not
   */
  void SetEvictable(frame_id_t frame_id, bool set_evictable);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Remove an evictable frame from replacer, along with its access history.
   * This function should also decrement replacer's size if removal is successful.
   *
   * Note that this is different from evicting a frame, which always remove the frame
   * with largest backward k-distance. This function removes specified frame id,
   * no matter what its backward k-distance is.
   *
   * If Remove is called on a non-evictable frame, throw an exception or abort the
   * process.
   *
   * If specified frame is not found, directly return from this function.
   *
   * @param frame_id id of frame to be removed
   */
  void Remove(frame_id_t frame_id);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Return replacer's size, which tracks the number of evictable frames.
   *
   * @return size_t
   */
  auto Size() -> size_t;
  /*
  Backward k-distance is computed as the difference in time between current timestamp and the timestamp of kth previous
  access. A frame with less than k historical accesses is given +inf as its backward k-distance. When multipe frames
  have +inf backward k-distance, the replacer evicts the frame with the earliest timestamp.
  */

 private:
  // TODO(student): implement me! You can replace these member variables as you like.
  // Remove maybe_unused if you start using them.
  class frame;
  struct MyCompare {
    bool operator()(const frame& f1, const frame& f2) const {
      if (f1.n_access < f1.k && f2.n_access < f2.k) {
        return f1.access_rec[0] < f2.access_rec[0];
      } else if (f1.n_access < f1.k) {
        return true;
      } else if (f2.n_access < f2.k) {
        return false;
      } else {
        return f1.access_rec[f1.earliest] < f2.access_rec[f1.earliest];
      }
    }
  };
  class frame {
    public:
      frame(size_t lruk) : id(0), pgid(INVALID_PAGE_ID), evictable(false), k(lruk), n_access(0), cur(0), earliest(0), k_distance(INT32_MAX) {
        if (lruk <= 0)
          throw std::exception();
        access_rec.reserve(lruk);
      }
      frame(frame&& f) : id(f.id), pgid(f.pgid), evictable(f.evictable), k(f.k), 
                          n_access(f.n_access), access_rec(std::move(f.access_rec)), cur(f.cur), 
                          earliest(f.earliest), k_distance(f.k_distance) {}
      frame_id_t id;
      page_id_t pgid;
      bool evictable;
      int k;
      int n_access;
      std::vector<size_t> access_rec;
      int cur;
      int earliest;
      long k_distance;
  };
  std::list<frame> lru;
  std::unordered_map<frame_id_t, std::list<frame>::iterator> id2it;
  // std::list<frame_id_t> unevictable_frames;
  std::multiset<frame, MyCompare> tree;
  [[maybe_unused]] size_t current_timestamp_{0};
  size_t curr_size_{0};
  size_t replacer_size_;
  size_t k_;
  std::mutex latch_;
};

}  // namespace bustub
