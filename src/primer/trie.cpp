#include "primer/trie.h"
#include <string_view>
#include "common/exception.h"
#include <stack>
namespace bustub {

  template <class T>
  auto Trie::Get(std::string_view key) const -> const T* {


    // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
    // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
    // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
    // Otherwise, return the value.

    //根节点
    auto c_root = this->root_;

    for (auto& c : key) {
      if (c_root == nullptr) {
        std::cout << "get key: " << key << " failure" << std::endl;
        return nullptr;
      }
      auto iter = c_root->children_.find(c);
      if (iter != c_root->children_.end()) {
        c_root = std::const_pointer_cast<TrieNode>(iter->second);
      } else {
        std::cout << "get key: " << key << " failure" << std::endl;
        return nullptr;
      }
    }

    if (c_root == nullptr || !c_root->is_value_node_) {
      std::cout << "get key: " << key << " failure" << std::endl;
      return nullptr;
    }
    //找到了
    auto c_root_with_value = dynamic_cast<const TrieNodeWithValue<T> *>(c_root.get());
    if (c_root_with_value) {
      std::cout << "get key: " << key << std::endl;
      return c_root_with_value->value_.get();
    } else {
      std::cout << "get key: " << key << " failure" << std::endl;
      return nullptr;
    }
  }

  template <class T>
  auto Trie::Put(std::string_view key, T value) const -> Trie {
    // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.


    // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
    // exists, you should create a new `TrieNodeWithValue`.


    //只要调用此接口 就一定会生成一个新的root

    //key为空就是直接取代根节点中
    if (key.empty()) {
      auto new_root = root_ ? std::make_shared<TrieNodeWithValue<T>>(root_->children_, std::make_shared<T>(std::move
      (value))) : std::make_shared<TrieNodeWithValue<T>>(std::make_shared<T>(std::move
      (value)));
      std::cout << "Put:" << " key:" << key << std::endl;
      return Trie(new_root);
    }

    auto new_root = this->root_ ? std::shared_ptr<TrieNode>(this->root_->Clone()) : std::make_shared<TrieNode>();
    auto c_root = new_root;
    for (unsigned int i = 0;i < key.size() - 1;i++) {
      char c = key[i];

      if (auto iter = c_root->children_.find(c); iter != c_root->children_.end()) {
        //节点存在
        c_root->children_[c] = std::shared_ptr<TrieNode>(iter->second->Clone());
      } else {
        c_root->children_[c] = std::make_shared<TrieNode>();
      }
      //指针下移
      c_root = std::const_pointer_cast<TrieNode>(c_root->children_[c]);
    }
    //最后一个节点变成一个值节点
    char cc = key[key.size() - 1];


    if (auto iter = c_root->children_.find(cc); iter != c_root->children_.end()) {
      //最后一个节点原本存在
      c_root->children_[cc] = std::make_shared<TrieNodeWithValue<T>>(c_root->children_[cc]->children_, std::make_shared<T>(std::move(value)));
      std::cout << "Put:" << "replace key:" << key << std::endl;
    } else {
      //最后一个节点不存在
      c_root->children_[cc] = std::make_shared<TrieNodeWithValue<T>>(std::make_shared<T>(std::move(value)));
      std::cout << "Put:" << "create key:" << key << std::endl;
    }
    return Trie(new_root);
  }

  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.
  auto Trie::Remove(std::string_view key) const -> Trie {


    if (key.empty()) {
      if (this->root_) {
        auto new_root = std::make_shared<TrieNode>(root_->children_);
        return Trie(new_root);
      } else {
        return Trie(std::make_shared<TrieNode>());
      }
    }
    //只要有值删除 就一定会生成一个新的root
    auto new_root = this->root_ ? std::shared_ptr<TrieNode>(this->root_->Clone()) : std::make_shared<TrieNode>();
    auto c_root = new_root;
    //把路径上所有经过的节点入栈
    auto node_stack = std::make_unique<std::stack<std::shared_ptr<TrieNode>>>();
    node_stack->push(c_root);
    for (unsigned int i = 0;i < key.size() - 1;i++) {

      char c = key[i];
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
      node_stack->push(c_root);
    }
    //遍历到了最后一个节点
    char c = key[key.size() - 1];
    if (auto iter = c_root->children_.find(c); iter == c_root->children_.end()) {
      //最后一个节点不存在
      std::cerr << "Trie::Remove() : can not find key :" << key << std::endl;
      return Trie(new_root);
    } else {
      //最后一个节点存在
      if (!iter->second->children_.empty()) {
        //最后一个节点有子节点 把他变成一个非值的节点
        c_root->children_[c] = std::make_shared<TrieNode>(c_root->children_[c]->children_);
      } else {
        //最后一个节点没有子节点
        c_root->children_.erase(c);
        std::cout << "Remove:" << "delete key:" << key << std::endl;
      }
    }
    //遍历栈中的所有节点查看哪一个非值节点没有子节点 找到就把这个节点也删除 
    for (int i = key.size() - 2;i >= 0;i--) {
      auto c_node = node_stack->top();
      node_stack->pop();
      if (!c_node->is_value_node_ && c_node->children_.empty()) {
        auto father_node = node_stack->top();
        father_node->children_.erase(key[i]);
      } else {
        break;
      }
    }
    //最后判断根节点是不是空
    if (new_root->children_.empty()) {
      std::cout<<"Remove:"<<"delete root"<<std::endl;
      return Trie(nullptr);
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