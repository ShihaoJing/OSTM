#include <unordered_set>
#include <cstddef>
#include <cstdint>
#include <random>
#include <iostream>

#define hashsize(n) (1<<(n))
#define hashmask(n) (n-1)

static size_t prob(size_t hv, int iter) {
  return hv + iter * iter;
}

enum class node_state {
  FREE,
  IN_USE,
  ERASED
};

template<typename T>
struct node {
  T key;
  node_state state = node_state::FREE;
};


template <typename T, typename Hash = std::hash<T>>
class seq_map {
  node<T> *nodes;
  size_t cap;
  size_t count;
  Hash h;

  void validate() {
    std::unordered_set<T> validation;
    for (int i = 0; i < cap; ++i) {
      if (nodes[i].state == node_state::IN_USE) {
        if (validation.insert(nodes[i].key).second == false)
          throw std::logic_error("Unexpected case in validation");
      }
    }
  }

  void resize() {
    std::cout << "resizing\n";
    count = 0;

    size_t tmp_cap = cap;
    auto tmp_nodes = nodes;

    cap = (cap << 1);
    nodes = new node<T>[cap];

    for (int i = 0; i < tmp_cap; ++i) {
      if (tmp_nodes[i].state == node_state::IN_USE) {
        add(tmp_nodes[i].key);
      }
    }

    delete [] tmp_nodes;
  }

public:
  explicit seq_map(size_t capacity): nodes(new node<T>[hashsize(capacity)]),
                                      cap(hashsize(capacity)), count(0) { }
  ~seq_map() { delete [] nodes; }

  bool contains(T key) const {
    size_t hv = h(key);

    for (int i = 1; i <= cap; ++i) {
      size_t idx = hv & hashmask(cap);
      if (nodes[idx].state == node_state::FREE) {
        return false;
      }
      if (nodes[idx].state == node_state::IN_USE
          && nodes[idx].key == key) {
        return true;
      }
      //  Quadratic prob
      hv = prob(hv, i);
    }

    return false;
  }

  bool add(T key) {
    if ((count << 1) > cap)
      resize();

    if (contains(key))
      return false;

    size_t hv = h(key);

    for (int i = 1; i <= cap; ++i) {
      size_t idx = hv & hashmask(cap);
      if (nodes[idx].state == node_state::FREE || nodes[idx].state == node_state::ERASED) {
        ++count;
        nodes[idx].key = std::move(key);
        nodes[idx].state = node_state::IN_USE;
        return true;
      }

      hv = prob(hv, i);
    }

    throw std::logic_error("Unexpected case in add");
  }

  bool remove(T key) {
    if (!contains(key))
      return false;

    size_t hv = h(key);

    for (int i = 1; i <= cap; ++i) {
      size_t idx = hv & hashmask(cap);
      if (nodes[idx].state == node_state::IN_USE
          && nodes[idx].key == key) {
        --count;
        nodes[idx].state = node_state::ERASED;
        return true;
      }

      hv = prob(hv, i);
    }

    throw std::logic_error("Unexpected case in remove");
  }

  void populate() {
    int i = 1024;
    while (i) {
      if (add(rand()))
        --i;
    }
  }

  size_t size() {
    std::cout << "validating size" << std::endl;
    validate();


    size_t ret = 0;

    for (int i = 0; i < cap; ++i) {
      if (nodes[i].state == node_state::IN_USE) {
        ++ret;
      }
    }

    return ret;
  }

};
