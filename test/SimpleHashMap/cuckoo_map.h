#ifndef PARALLEL_CONCURRENCY_TRANSACTION_CUCKOO_MAP_H
#define PARALLEL_CONCURRENCY_TRANSACTION_CUCKOO_MAP_H

#include <iostream>
#include "../tm.h"

#define hashsize(n) (1<<(n))
#define hashmask(n) (n-1)
#define LIMIT 10

#define FREE -1

using namespace std;

template <typename T = int, typename Hash = std::hash<T>>
class cuckoo_map {
private:
  int cap;
  size_t count;
  T** table1;
  T** table2;
  Hash h;

  size_t hash0(T y) {
    return h(y);
  }

  size_t hash1(T y) {
    return h(y) + 1295871160;
  }


  void resize(int old_cap) {
	  OTM_BEGIN()
      T **old_table1;
	  T **old_table2;
	  //__transaction_atomic {

	  if (old_cap != OTM_SHARED_READ_I(cap)) {
		  return;
	  }

	  old_table1 = OTM_SHARED_READ_P(table1);
	  old_table2 = OTM_SHARED_READ_P(table2);

	  OTM_SHARED_WRITE_I(cap, cap << 1);
	  OTM_SHARED_WRITE_P(table1, new T*[OTM_SHARED_READ_I(cap)]);
	  OTM_SHARED_WRITE_P(table2, new T*[OTM_SHARED_READ_I(cap)]);

	  for (int i = 0; i < cap; ++i) {
		  OTM_SHARED_WRITE_P(table1[i], new int(-1));
		  OTM_SHARED_WRITE_P(table2[i], new int(-1));
	  }

	  for (int i = 0; i < old_cap; ++i) {
		  if (OTM_SHARED_READ_I(*old_table1[i]) != FREE) {
			  add(OTM_SHARED_READ_I(*old_table1[i]));
		  }
		  if (OTM_SHARED_READ_I(*old_table2[i]) != FREE) {
			  add(OTM_SHARED_READ_I(*old_table2[i]));
		  }
	  }

  }

public:
  explicit cuckoo_map(size_t hashpower) : table1(new T*[hashsize(hashpower)]),
  	  	  	  	  	  	  	  	  	  	  table2(new T*[hashsize(hashpower)]),
                                          cap(hashsize(hashpower)), count(0)
  {
      for (int i = 0; i < cap; ++i) {
		  table1[i] = new int(-1);
		  table2[i] = new int(-1);
	  }
  }


  bool contains(T key) {
	  OTM_BEGIN();
	  uint32_t hv0 = hash0(key);
	  uint32_t hv1 = hash1(key);

	  //__transaction_atomic {

	  if (OTM_SHARED_READ_I(*table1[hv0 & hashmask(cap)]) == key) {
		  return true;
	  }
	  else if (OTM_SHARED_READ_I(*table2[hv1 & hashmask(cap)]) == key) {
		  return true;
	  }

	  return false;

    // } _transaction_atomic
  }

  bool add(T key) {
	OTM_BEGIN();
    if (contains(key))
      return false;

    T x = key;

    for (int i = 0; i < LIMIT; ++i) {
      //__transaction_atomic {

      uint32_t pos0 = hash0(x) & hashmask(cap);
      T tmp = OTM_SHARED_READ_I(*table1[pos0]);
      OTM_SHARED_WRITE_I(*table1[pos0], x);
      x = tmp;
      if (x == FREE)
        return true;

      uint32_t pos1 = hash1(x) & hashmask(cap);;
      tmp = OTM_SHARED_READ_I(*table2[pos1]);
      OTM_SHARED_WRITE_I(*table2[pos1], x);
      x = tmp;
      if (x == FREE)
        return true;

      //} _transaction_atomic
    }

    /*
    // to avoid memory leak
    key = *x;
    delete x;
    */

    resize(cap);

    return add(key);
  }

  bool remove(T key) {
	OTM_BEGIN();
    uint32_t hv0 = hash0(key);
    uint32_t hv1 = hash1(key);
    uint32_t pos0 = hash0(key) & hashmask(cap);
    uint32_t pos1 = hash1(key) & hashmask(cap);


    if (OTM_SHARED_READ_I(*table1[pos0]) == key) {
    	OTM_SHARED_WRITE_I(*table1[pos0], FREE);
    	return true;
    }
    if (OTM_SHARED_READ_I(*table2[pos0]) == key) {
    	OTM_SHARED_WRITE_I(*table2[pos1], FREE);
    	return true;
    }

    return false;

    //} _transaction_atomic
  }

  size_t size() {
	  size_t ret = 0;
	  for (int i = 0; i < cap; ++i) {
		  if (*table1[i] != FREE) {
			  ++ret;
		  }
		  if (*table2[i] != FREE) {
			  ++ret;
		  }
	  }
      return ret;
  }

  int capacity() {
	  return cap;
  }
};

#endif //PARALLEL_CONCURRENCY_CUCKOO_MAP_H
