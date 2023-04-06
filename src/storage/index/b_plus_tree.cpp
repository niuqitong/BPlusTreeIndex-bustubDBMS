#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool { 
  return root_page_id_ == INVALID_PAGE_ID;
 }
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetLeafPage(const KeyType& key, Transaction* trx, Operation op, bool is_first_pass = true) -> Page* {

  if (trx == nullptr && op != Operation::Read) {
    throw std::logic_error("tring to remove or insert on a null tree");
  }
  if (!is_first_pass) {
    root_latch_.WLock();
    trx->AddIntoPageSet(nullptr);
  }

  page_id_t next_page_id = root_page_id_;
  Page* prev_page = nullptr;
  while (true) {
    // 读: 每次操作释放父节点的读锁
    // 写: 当前页安全才释放父节点的写锁
    Page* page = buffer_pool_manager_->FetchPage(next_page_id);
    auto tree_node_page = reinterpret_cast<BPlusTree*>(page->GetData());
    if (is_first_pass) {

      if (tree_node_page->IsLeafPage() && op != Operation::Read) {
        page->WLatch();
        trx->AddIntoPageSet(page);
      } else {
        page->RLatch();
      }
      if (prev_page == nullptr) {
        root_latch_.RUnlock(); 
      } else {
        prev_page->RUnlatch();
        buffer_pool_manager_->UnpinPage(prev_page->GetPageId(), false);
      }

    } else { // second pass

      assert(op != Operation::Read);
      page->WLatch();
      if (IsPageSafe(tree_node_page, op)) {
        ReleaseWLatches(trx);
      }
      trx->AddIntoPageSet(page);
    }

    if (tree_node_page->IsLeafPage()) {
      if (is_first_pass && !IsPageSafe(tree_node_page, op)) {
        ReleaseWLatches(trx);
        return GetLeafPage(key, trx, op, false);
      } else {
        return page;
      }
    }
    auto internal_page = static_cast<InternalPage*>(tree_node_page);
    next_page_id = internal_page->ValueAt(internal_page->GetSize() - 1);
    
    for (int i = 1; i < internal_page->GetSize(); ++i) {
      if (comparator_(internal_page->KeyAt(i), key) > 0) {
        next_page_id = internal_page->ValueAt(i - 1);
        break;
      }
    }
    prev_page = page;
    // if (tree_node_page->IsLeafPage()) {
    //   return page;
    // }
    // auto internal_page = static_cast<InternalPage*>(tree_node_page);
    // next_page_id = internal_page->ValueAt(internal_page->GetSize() - 1);
    // for (int i = 1; i < internal_page->GetSize(); ++i) {
    //   if (comparator_(internal_page->KeyAt(i), key) > 0) {
    //     next_page_id = internal_page->VauleAt(i - 1);
    //     break;
    //   }
    // }
    // buffer_pool_manager_->UnpinPage(internal_page->GetPageId(), false);
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) -> bool {
  // read latch
  root_latch_.RLock();
  if (IsEmpty()) {
    root_latch_.RUnlock();
    return false;
  }
  Page* target_page = GetLeafPage(key);
  auto target_leaf_page = reinterpret_cast<LeafPage*>(page);
  bool exist = false;
  for (int i = 0; i < target_leaf_page->GetSize(); ++i) {
    if (comparator_(key, target_leaf_page->KeyAt(i)) == 0) {
      result->emplace_back(target_leaf_page->ValueAt(i));
      exist = true;
    }
  }
  target_page->RUnlatch();
  buffer_pool_manager_->UnpinPage(target_page->GetPageId(), false);

  return exist;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsPageSafe(BPlusTreePage* tree_page, Operation op) -> bool {
  if (op == Operation::Read) {
    return true;
  }
  if (op == Operation::Insert) {
    if (tree_page->IsLeafPage()) {
      return tree_page->GetSize() < tree_page->GetMaxSize() - 1;
    } else {
      return tree_page->GetSize() < tree_page->GetMaxSize();
    }
  }

  if (op == Operation::Remove) {
    if (tree_page->IsRootPage()) {
      if (tree_page->IsLeafPage()) {
        return tree_page->GetSize() > 1;
      } else {
        return tree_page->GetSize() > 2;
      }
    } else {
      return tree_page->GetSize() > tree_page->GetMinSize();
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ReleaseWLatches(Transaction* trx) {
  if (trx == nullptr) {
    return;
  }
  auto pages = trx->GetPageSet();
  while (!pages->empty()) {
    Page* page = pages->front();
    pages->pop_front();
    if (page == nullptr) {
      root_latch_.WUnlock(); 
      // lock(), unlock() 独占锁
      // lock_shared(), unlock_shared() 共享锁
      // unlock() 可以解锁 lock_shared()上的锁
      // 但 unlock_shared()不能解锁lock()上的锁
    } else {
      page->WUnlatch();
      buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetPageFromTrx(page_id_t page_id, Transaction* trx) -> Page* {
  assert(trx != nullptr);
  auto pages = trx->GetPageSet();
  for (auto it = pages->rbegin(); it != pages->rend(); ++it) {
    Page* page = *it;
    if (page != nullptr && page->GetPageId() == page_id) {
      return page;
    }
  }
  throw std::logic_error("error getting page from transaction");
  return nullptr;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  root_latch_.RLock();
  LeafPage* leaf_page = nullptr;
  // 1. tree is empty, insert a node treated as leaf node
  if (IsEmpty()) { 
    root_latch_.RUnlock();
    root_latch_.WLock();
    if (IsEmpty()) {
      Page* page = buffer_pool_manager_->NewPage(&root_page_id_);
      UpdateRootPageId(1);
      leaf_page = reinterpret_cast<LeafPage*>(page->GetData());
      leaf_page->Init(root_page_id_, INVALID_PAGE_ID, leaf_max_size_);
      leaf_page->SetKV(0, key, value);
      leaf_page->IncreaseSize(1);
      leaf_page->SetNextPageId(INVALID_PAGE_ID);
      buffer_pool_manager_->UnpinPage(root_page_id_, false); // false means not dirty
      root_latch_.WUnlock();
      return true;
    }
    root_latch_.WUnlock();
    root_latch_.RLock();
  }
  
  Page* page = GetLeafPage(key, transaction, Operation::Insert);
  leaf_page = reinterpret_cast<LeafPage*>(page->GetData());
  
  // 2. a node with the intended key to insert already exists,
  //    return false
  for (int i = 0; i < leaf_page->GetSize(); ++i) {
    if (comparator_(leaf_page->KeyAt(i), key) == 0) { 
      ReleaseWLatches(transaction);
      buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
      return false;
    }
  }
  
  leaf_page->Insert(key, value, comparator_);
  if (leaf_page->GetSize < leaf_max_size_) {
    ReleaseWLatches(transaction); // 可以去掉, 能到这步说明该是安全的, 祖先节点也不持有其他锁
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
    return true;
  }

  // 3. leaf node reaches max capacity, split it
  // 分裂时新建的页还未加到树中, 不会被其他事务访问, 无需加锁, 只需用完unpin
  page_id_t new_leaf_id;
  Page* new_page = buffer_pool_manager_->NewPage(&new_leaf_id);
  auto new_leaf_page = reinterpret_cast<LeafPage*>(new_page->GetData());
  new_leaf_page->Init(new_leaf_id, leaf_page->GetParentPageId(), leaf_max_size_);
  new_leaf_page->SetNextPageId(leaf_page->GetNextPageId());
  leaf_page->SetNextPageId(new_leaf_id);
  leaf_page->MoveSplitedData(new_leaf_page); // move half of leaf_page's data to new_leaf_page

  BPlusTreePage* old_node = leaf_page;
  BPlusTreePage* new_node = new_leaf_page;
  KeyType split = new_node->KeyAt(0);
  while (true) {
    // 3.1 old page is root, create a new parent node
    if (old_node->IsRootPage()) {
      root_latch_.RUnlock();
      root_latch_.WLock();
      Page* new_root_page = buffer_pool_manager_->NewPage(&root_page_id_);
      auto new_root_node = reinterpret_cast<InternalPage*>(new_root_page->GetData());
      new_root_node->Init(root_page_id_, INVALID_PAGE_ID, internal_max_size_);
      new_root_node->SetKV(0, split, old_node->GetPageId());
      new_root_node->SetKV(1, split, new_node->GetPageId());
      new_root_node->IncreaseSize(2);
      old_node->SetParentPageId(root_page_id_);
      new_node->SetParentPageId(root_page_id_);
      UpdateRootPageId();
      ReleaseWLatches(transaction); // 释放节点锁 和 root_latch_
      buffer_pool_manager_->UnpinPage(root_page_id_, true);
      break;
    }
    
    // 3.2 old page is not root, add the splited page to old page's parent as well
    page_id_t parent_page_id = old_node->GetParentPageId();
    // Page* parent_page = buffer_pool_manager_->FetchPage(parent_page_id);
    Page* parent_page = GetPageFromTrx(parent_page_id, transaction);
    auto parent_node = reinterpret_cast<InternalPage*>(parent_page->GetData());
    parent_node->Insert(split, new_node->GetPageId(), comparator_);
    new_node->SetParentPageId(parent_node->GetPageId());
    if (parent_node->GetSize() <= internal_max_size_) {
      ReleaseWLatches(transaction);
      buffer_pool_manager_->UnpinPage(new_leaf_id, true);
      buffer_pool_manager_->UnpinPage(parent_page_id, true);
      break;
    }

    // parent has m + 1 children after insertion, split needed
    page_id_t parent_sibling_page_id;
    Page* parent_sibling_page = buffer_pool_manager_->NewPage(&parent_sibling_page_id);
    auto parent_sibling_node = reinterpret_cast<InternalPage*>(parent_sibling_page->GetData());
    parent_sibling_node->Init(parent_sibling_page_id, parent_node->GetParentPageId(), internal_max_size_);
    // int new_sibling_size = internal_max_size_ / 2; // sibling of the splited parent
    size_t offset = (parent_node->GetSize() + 1) / 2;
    for (int i = offset; i < parent_node.GetSize(); ++i) {
      parent_sibling_node->SetKV(i - offset, parent_node->KeyAt(i), parent_node->ValueAt(i));
      Page* pg = buffer_pool_manager_->FetchPage(parent_node->ValueAt(i));
      auto node = reinterpret_cast<BPlusTreePage*>(pg->GetData());
      node->SetParentPageId(parent_sibling_page_id);
      buffer_pool_manager_->UnpinPage(node->GetPageId(), true);
    }
    parent_sibling_node->SetSize(internal_max_size_ - offset);
    parent_node->SetSize(offset);

    buffer_pool_manager_->UnpinPage(old_node->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);
    old_node = parent_node;
    new_node = parent_sibling_node;
    split = parent_sibling_node.KeyAt(0);
  }
  ReleaseWLatches(transaction);
  // buffer_pool_manager_->UnpinPage(old_node->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);

  return true;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  root_latch_.RLock();
  if (IsEmpty()) {
    root_latch_.RUnlock();
    return;
  }
  Page* page = GetLeafPage(key, transaction, Operation::Remove);
  auto leaf_page = reinterpret_cast<LeafPage*>(page->GetData());
  leaf_page->Remove(key, comparator_);

  if (leaf_page->GetSieze() < leaf_page->GetMinSize()) {
    HandleUnderflow(leaf_page, transaction);
  }
  ReleaseWLatches(transaction);
  auto deleted_pages = transaction->GetDeletedPageSet();
  for (auto& pg_id : *deleted_pages) {
    buffer_pool_manager_->DeletePage(pg_id);
  }
  deleted_pages->clear();
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::HandleUnderflow(BPlusTreePage* page, Transaction *transaction) {
  // 1. root underflow
  if (page->IsRootPage()) {
    if (page->GetSize() > 1 || (page->IsLeafPage() && page->GetSize() == 1)) {
      return;
    }
    if (page->IsLeafPage()) { // root is leaf, size == 0
      transaction->AddIntoDeletedPageSet(page->GetPageId());
      root_latch_.RUnlock();
      root_latch_.WLock();
      root_page_id_ = INVALID_PAGE_ID;
    } else { // root is normal(internal), size == 1
      root_latch_.RUnlock();
      root_latch_.WLock();
      auto old_root_page = static_cast<InternalPage*>(page);
      root_page_id_ = old_root_page->ValueAt(0);
      auto new_root_page = reinterpret_cast<InternalPage*>(buffer_pool_manager_->FetchPage(root_page_id_)->GetData());
      new_root_page->SetParentPageId(INVALID_PAGE_ID);
      buffer_pool_manager_->UnpinPage(root_page_id_, true);
    }
    // root_latch_.RUnlock();
    // root_latch_.WLock();
    // auto old_root_page = static_cast<InternalPage*>(page);
    // root_page_id_ = old_root_page->ValueAt(0);
    // auto new_root_page = reinterpret_cast<InternalPage*>(buffer_pool_manager_->FetchPage(root_page_id_)->GetData());
    // new_root_page->SetParentPageId(INVALID_PAGE_ID);
    // buffer_pool_manager_->UnpinPage(root_page_id_, true);
    UpdateRootPageId();
    return;
  }

  page_id_t left_page_id;
  page_id_t right_page_id;
  GetSiblings(page, left_page_id, right_page_id, transaction);
  if (left_page_id == INVALID_PAGE_ID && right_page_id == INVALID_PAGE_ID) {
    throw std::logic_error("non-root page" + std::to_string(page->GetPageId()) + "has no siblig");
  }

  BPlusTreePage* left_page = nullptr;
  BPlusTreePage* right_page = nullptr;
  Page* left = nullptr;
  Page* right = nullptr;
  if (left_page_id != INVALID_PAGE_ID) {
    left = buffer_pool_manager_->FetchPage(left_page_id);
    left->WLatch();
    left_page = reinterpret_cast<InternalPage*>(left->GetData());
  }
  if (right_page_id != INVALID_PAGE_ID) {
    right = buffer_pool_manager_->FetchPage(right_page_id);
    right->WLatch();
    right_page = reinterpret_cast<InternalPage*>(right->GetData());
  }
  auto parent_page = reinterpret_cast<InternalPage*>(GetPageFromTrx(page->GetParentPageId(), trx)->GetData());

  // 2. steal from either sibling
  if (TryBorrow(page, left_page, parent_page, true) || TryBorrow(page, right_page, parent_page, false)) {
    if (left_page_id != INVALID_PAGE_ID) {
      left->WUnlatch();
      buffer_pool_manager_->UnpinPage(left_page_id, true);
    }
    if (right_page_id != INVALID_PAGE_ID) {
      right->WUnlatch();
      buffer_pool_manager_->UnpinPage(right_page_id, true);
    }
    // ReleaseWLatches(transaction);
    // buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
    return;
  }
  // 3. merge with either sibling
  if (left_page != nullptr) {
    // left->WLatch();
    MergePage(left_page, page, parent_page, transaction);
    left->WUnlatch();
    buffer_pool_manager_->UnpinPage(left_page_id, true);
  } else {
    // right->WLatch();
    MergePage(page, right_page, parent_page, transaction);
    right->WUnlatch();
    buffer_pool_manager_->UnpinPage(right_page_id, true);
  }
  // if (left_page_id != INVALID_PAGE_ID) {
  // }
  // if (right_page_id != INVALID_PAGE_ID) {
  // }
  if (parent_page->GetSize() < parent_page->GetMinSize()) {
    HandleUnderflow(parent_page, transaction);
  }
  // ReleaseWLatches(transaction);
  // buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::TryBorrow(BPlusTreePage* page, BPlusTreePage* sibling_page, InternalPage* parent_page, bool is_left_sibling) -> bool {
  if (sibling_page == nullptr || sibling_page->GetSize() <= sibling_page->GetMinSize()) {
    return false;
  }
  int sibling_array_id = is_left_sibling ? sibling_page->GetSize() - 1 : (page->IsLeafPage() ? 0 : 1);
  int parent_array_id = parent_page->ArrayIndex(page->GetPageId()) + is_left_sibling ? 0 : 1;

  KeyType updated;

  if (page->IsLeafPage()) {
    auto leaf_page = static_cast<LeafPage*>(page);
    auto leaf_sibling_page = static_cast<LeafPage*>(sibling_page);
    leaf_page->Insert(leaf_sibling_page->KeyAt(sibling_array_id), leaf_sibling_page->ValueAt(sibling_array_id), comparator_);
    leaf_sibling_page->Remove(leaf_sibling_page->KeyAt(sibling_array_id));
    updated = is_left_sibling ? leaf_page->KeyAt(0) : leaf_sibling_page->KeyAt(0);
  } else {
    auto internal_page = static_cast<InternalPage*>(page);
    auto internal_sibling_page = static_cast<InternalPage*>(sibling_page);
    updated = internal_sibling_page->KeyAt(sibling_array_id);
    /*
      borrow from left sibling:
        updated key = last key of array_ of left sibling
      borrow from right sibling:
        updated key =  first key of array_ of right sibling
    */
    page_id_t child_id;
    if (is_left_sibling) {
      internal_page->Insert(parent_page->KeyAt(parent_array_id), internal_page->ValueAt(0), comparator_);
      internal_page->SetValueAt(0, internal_sibling_page->ValueAt(sibling_array_id));
      child_id = internal_page->ValueAt(0);
    } else {
      internal_page->SetKV(internal_page->GetSize(), parent_page->KeyAt(parent_array_id), internal_sibling_page->ValueAt(0));
      internal_page->IncreaseSize(1);
      internal_sibling_page->SetKV(0, internal_sibling_page->KeyAt(0), internal_sibling_page->ValueAt(1));
      child_id = internal_page->ValueAt(internal_page->GetSize() - 1);
    }
    internal_sibling_page->RemoveAt(sibling_array_id);
    Page* page = buffer_pool_manager_->FetchPage(child_id);
    auto child_page = reinterpret_cast<BPlusTreePage*>(page->GetData());
    child_page->SetParentPageId(internal_page->GetPageId());
    buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
  } 
  parent_page->SetKeyAt(parent_array_id, updated);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::MergePage(BPlusTreePage* left_page, BPlusTreePage* right_page, BPlusTreePage* parent_page, Transaction* trx) {
  if (left_page->IsLeafPage()) {
    auto left_leaf_page = static_cast<LeafPage*>(left_page);
    auto right_leaf_page = static_cast<LeafPage*>(right_page);
    for (int i = 0; i < right_leaf_page->GetSize(); ++i) {
      left_leaf_page->Insert(right_leaf_page->KeyAt(i), right_leaf_page->ValueAt(i), comparator_);
    }
    left_leaf_page->SetNextPageId(right_leaf_page->GetNextPageId());
    auto removed_page_id = right_page->GetPageId();
    // parent_page->RemoveAt(parent_page->ArrayIndex(right_page->GetPageId()));
    parent_page->RemoveAt(parent_page->ArrayIndex(removed_page_id));
    trx->AddIntoDeletedPageSet(removed_page_id);
  } else {
    auto left_internal_page = static_cast<InternalPage*>(left_page);
    auto right_internal_page = static_cast<InternalPage*>(right_page);
    left_internal_page->Insert(parent_page->KeyAt(ArrayIndex(right_page->GetPageId())), right_internal_page->ValueAt(0), comparator_);
    SetPageParentId(right_internal_page->ValueAt(0), left_internal_page->GetPageId());
    auto removed_page_id = right_page->GetPageId();
    // parent_page->RemoveAt(parent_page->ArrayIndex(right_page->GetPageId()));
    parent_page->RemoveAt(parent_page->ArrayIndex(removed_page_id));
    trx->AddIntoDeletedPageSet(removed_page_id);
    for (int i = 1; i < right_internal_page->GetSize(); ++i) {
      left_internal_page->Insert(right_internal_page->KeyAt(i), right_internal_page->ValueAt(i), comparator_);
      SetPageParentId(right_internal_page->ValueAt(i), left_internal_page->GetPageId());
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::SetPageParentId(page_id_t child, page_id_t parent) {
  auto page = buffer_pool_manager_->FetchPage(child);
  auto child_node_page = reinterpret_cast<BPlusTreePage*>(page);
  child_node_page->SetParentPageId(parent);
  buffer_pool_manager_->UnpinPage(child, false);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::GetSiblings(BPlusTreePage* page, page_id_t& left, page_id_t& right, Transaction* trx) {
  if (page->IsRootPage) {
    throw std::invalid_argument("tring to get siblings of the root node");
  }
  // auto parent_page = reinterpret_cast<InternalPage*>(buffer_pool_manager_->FetchPage(page->GetParentPageId())->GetData());
  auto parent_page = reinterpret_cast<InternalPage*>(GetPageFromTrx(page->GetParentPageId(), trx)->GetData());
  auto idx = parent_page->ArrayIndex(page->GetPageId());
  if (idx == -1) {
    throw std::logic_error("tree error");
  }
  left = right = INVALID_PAGE_ID;
  if (idx != 0) {
    left = parent_page->ValueAt(idx - 1);
  }
  if (idx != parent_page->GetSize() - 1) {
    right = parent_page->ValueAt(idx + 1);
  }
  // 释放锁时会同时unpin
  // buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), false);
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE { 
  root_latch_.RLock();
  if (IsEmpty()) {
    root_latch_.RUnlock();
    return End();
  }
  page_id_t next_page_id = root_page_id_;
  while (true) {
    Page* pg = buffer_pool_manager_->FetchPage(next_page_id);
    auto tree_page = reinterpret_cast<BPlusTree*>(page->GetData());
    if (tree_page->IsLeafPage()) {
      return INDEXITERATOR_TYPE(tree_page->GetPageId(), 0, buffer_pool_manager_);
    }
    auto internal_page = static_cast<InternalPage*>(tree_page);
    if (internal_page == nullptr) {
      throw std::bad_cast();
    }
    next_page_id = internal_page->ValueAt(0);
    buffer_pool_manager_->UnpinPage(internal_page->GetPageId(), false);
  }
  return INDEXITERATOR_TYPE(INVALID_PAGE_ID, 0, nullptr); 
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE { 
  
  Page* page = GetLeafPage(key);
  auto leaf_page = reinterpret_cast<LeafPage*>(page->GetData());
  return INDEXITERATOR_TYPE(page->GetPageId(), leaf_page->Lowerbound(key, comparator_), buffer_pool_manager_);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { 
  return INDEXITERATOR_TYPE(INVALID_PAGE_ID, 0, nullptr); 
}

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { return 0; }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  auto *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Draw an empty tree");
    return;
  }
  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  ToGraph(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm, out);
  out << "}" << std::endl;
  out.flush();
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  if (IsEmpty()) {
    LOG_WARN("Print an empty tree");
    return;
  }
  ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm);
}

/**
 * This method is used for debug only, You don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
