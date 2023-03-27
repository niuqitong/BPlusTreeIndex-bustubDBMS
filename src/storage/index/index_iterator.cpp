/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator() = default;

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool { 
  return pg_id_ == INVALID_PAGE_ID;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & {  
  return leaf_page_->array_[idx_];
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & { 
  if (pg_id_ == INVALID_PAGE_ID) {
    return *this;
  }
  if (idx_ < leaf_page_->GetSize() - 1) {
    ++idx;
  } else {
    idx_ = 0;
    page_id_t cur_id = pg_id_;
    page_id_t nxt_id = leaf_page_->GetNextPageId();
    if (nxt_id == INVALID_PAGE_ID) {
      pg_ = nullptr;
      leaf_page_ = nullptr;
    } else {
      pg_ = index_bpm_->FetchPage(pg_id_);
      leaf_page_ = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE*>(pg_->GetData());
    }
    index_bpm_->UnpinPage(cur_id, false);
  }
  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
