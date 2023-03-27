//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/index_iterator.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
 public:
  // you may define your own constructor based on your member variables
  IndexIterator() = default;
  IndexIterator(page_id_t pg_id, int idx, BufferPoolManager* bpm) :
    pg_id_(pg_id), idx_(idx), index_bpm_(bpm) {}
  ~IndexIterator();  // NOLINT

  auto IsEnd() -> bool;

  auto operator*() -> const MappingType &;

  auto operator++() -> IndexIterator &;

  auto operator==(const IndexIterator &it) const -> bool { 
    return pg_id_ == it.pg_id_ && idx_ == it.idx_;
  }

  auto operator!=(const IndexIterator &itr) const -> bool { 
    return pg_id_ != it.pg_id_ || idx_ != it.idx_;
  }

 private:
  // add your own private member variables here
  page_id_t pg_id_ = INVALID_PAGE_ID;
  Page* pg_ = nullptr;
  B_PLUS_TREE_LEAF_PAGE_TYPE* leaf_page_ = nullptr;
  int idx_ = 0;
  BufferPoolManager* index_bpm_ = nullptr;
};

}  // namespace bustub
