#include "primer/trie.h"
#include <string_view>
#include "common/exception.h"

namespace bustub {

  template <class T>
  auto Trie::Get(std::string_view key) const -> const T* {
    throw NotImplementedException("Trie::Get is not implemented.");

    // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
    // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
    // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
    // Otherwise, return the value.

    if (key.empty()) {
      return nullptr;
    }
    //根节点
    auto c_root = this->root_;

    for (auto c : key) {
      auto iter = c_root->children_.find(c);
      if (iter != c_root->children_.end()) {
        c_root = iter->second;
      } else {
        return nullptr;
      }
    }

    if (c_root == nullptr || !c_root->is_value_node_) {
      return nullptr;
    }
    //找到了
    auto c_root_with_value = dynamic_cast<const TrieNodeWithValue<T> *>(c_root.get());
    if (c_root_with_value) {
      return c_root_with_value->value_.get();
    } else {
      return nullptr;
    }
  }

  template <class T>
  auto Trie::Put(std::string_view key, T value) const -> Trie {
    // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.
    throw NotImplementedException("Trie::Put is not implemented.");

    // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
    // exists, you should create a new `TrieNodeWithValue`.
    if (key.empty()) {
      return Trie(nullptr);
    }

    //只要有新的值需要插入 就一定会生成一个新的root
    auto new_root = this->root_ ? std::shared_ptr<TrieNode>(this->root_->Clone()) : std::make_shared<TrieNode>();

    auto c_root = new_root;
    for (int i = 0;i < key.size() - 1;i++) {
      char c = key[i];
      auto iter = c_root->children_.find(c);
      //节点存在
      if (auto iter = c_root->children_.find(c); iter != c_root->children_.end()) {
        c_root->children_[c] = iter->second->Clone();
      } else {
        c_root->children_[c] = std::make_shared<TrieNode>();//
      }
      //指针下移
      c_root = std::const_pointer_cast<TrieNode>(c_root->children_[c]);
    }
    //最后一个节点变成一个值节点
    char cc = key[key.size() - 1];
    if (auto iter = c_root->children_.find(key[cc]); iter == c_root->children_.end()) {
      //最后一个节点不存在
      c_root->children_[cc] = std::make_shared<TrieNodeWithValue<T>>(std::make_shared<T>(std::move(value)));
    } else {
      //最后一个节点原本存在
      c_root->children_[cc] = std::make_shared<TrieNodeWithValue<T>>(c_root->children_[cc]->children_, std::make_shared<T>(std::move(value)));
    }
    return Trie(new_root);
  }

  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.
  auto Trie::Remove(std::string_view key) const -> Trie {
    throw NotImplementedException("Trie::Remove is not implemented.");
    if (key.empty()) {
      return Trie(nullptr);
    }
    //只要有值删除 就一定会生成一个新的root
    auto new_root = this->root_ ? std::shared_ptr<TrieNode>(this->root_->Clone()) : std::make_shared<TrieNode>();
    auto c_root = new_root;

    for (int i = 0;i < key.size() - 1;i++) {
      char c = key[i];
      auto iter = c_root->children_.find(c);
      //节点存在
      if (auto iter = c_root->children_.find(c); iter != c_root->children_.end()) {
        c_root->children_[c] = iter->second->Clone();
      } else {
        //节点不存在 删除了一个不存在的 key
        std::cerr << "Trie::Remove() : can not find key :" << key << std::endl;
        return Trie(new_root);
      }
      //指针下移
      c_root = std::const_pointer_cast<TrieNode>(c_root->children_[c]);
    }
    //遍历到了最后一个节点
    char c = key[key.size() - 1];
    if (auto iter = c_root->children_.find(key[c]); iter == c_root->children_.end()) {
      //最后一个节点不存在
      return Trie(new_root);
    } else {
      //最后一个节点存在
      if (!iter->second->children_.empty()) {
        //最后一个节点有子节点 把他变成一个非值的节点
        c_root->children_[c] = std::make_shared<TrieNode>(c_root->children_[c]->children_);
      } else {
        //最后一个节点没有子节点
        c_root->children_.erase(c);
      }
    }
    return Trie(new_root);
  }

  // Below are explicit instantiation of template functions.
  //
  // Generally people would write the implementation of template classes and functions in the header file. However, we
  // separate the implementation into a .cpp file to make things clearer. In order to make the compiler know the
  // implementation of the template functions, we need to explicitly instantiate them here, so that they can be picked up by the linker.

  template auto Trie::Put(std::string_view key, uint32_t value) const->Trie;
  template auto Trie::Get(std::string_view key) const -> const uint32_t*;

  template auto Trie::Put(std::string_view key, uint64_t value) const->Trie;
  template auto Trie::Get(std::string_view key) const -> const uint64_t*;

  template auto Trie::Put(std::string_view key, std::string value) const->Trie;
  template auto Trie::Get(std::string_view key) const -> const std::string*;

  // If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

  using Integer = std::unique_ptr<uint32_t>;

  template auto Trie::Put(std::string_view key, Integer value) const->Trie;
  template auto Trie::Get(std::string_view key) const -> const Integer*;

  template auto Trie::Put(std::string_view key, MoveBlocked value) const->Trie;
  template auto Trie::Get(std::string_view key) const -> const MoveBlocked*;

}  // namespace bustub