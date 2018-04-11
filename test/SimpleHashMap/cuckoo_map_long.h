/*
 * cuckoo_map_long.h
 *
 *  Created on: Apr 11, 2018
 *      Author: root
 */

#ifndef TEST_SIMPLEHASHMAP_CUCKOO_MAP_LONG_H_
#define TEST_SIMPLEHASHMAP_CUCKOO_MAP_LONG_H_

#include <iostream>
#include "../tm.h"

#define hashsize(n) (1<<(n))
#define hashmask(n) (n-1)
#define READ_I(n) OTM_SHARED_READ_I(n)
#define READ_L(n) OTM_SHARED_READ_L(n)
#define LIMIT 10

#define FREE -1

using namespace std;

template <typename T = int, typename Hash = std::hash<T>>
class cuckoo_map {
private:
  int cap;
  size_t count;
  T* table0;
  T* table1;
  Hash h;

  size_t hash0(T y) {
    return h(y);
  }

  size_t hash1(T y) {
    return h(y) + 1295871160;
  }


  void resize(int old_cap) {
	  OTM_BEGIN()
      T *old_table0;
	  T *old_table1;
	  //__transaction_atomic {

	  if (old_cap != READ_I(cap)) {
		  return;
	  }

	  old_table0 = OTM_SHARED_READ_P(table0);
	  old_table1 = OTM_SHARED_READ_P(table1);

	  OTM_SHARED_WRITE_I(cap, cap << 1);
	  OTM_SHARED_WRITE_P(table0, new T[READ_I(cap)]);
	  OTM_SHARED_WRITE_P(table1, new T[READ_I(cap)]);

	  for (int i = 0; i < READ_I(cap); ++i) {
		  OTM_SHARED_WRITE_L(table0[i], FREE);
		  OTM_SHARED_WRITE_L(table1[i], FREE);
	  }

	  for (int i = 0; i < old_cap; ++i) {
		  if (OTM_SHARED_READ_L(old_table0[i]) != FREE) {
			  add(OTM_SHARED_READ_L(old_table0[i]));
		  }
		  if (OTM_SHARED_READ_L(old_table1[i]) != FREE) {
			  add(OTM_SHARED_READ_L(old_table1[i]));
		  }
	  }

  }

public:
  explicit cuckoo_map(size_t hashpower) : table0(new T[hashsize(hashpower)]),
  	  	  	  	  	  	  	  	  	  	  table1(new T[hashsize(hashpower)]),
                                          cap(hashsize(hashpower)), count(0)
  {
      for (int i = 0; i < cap; ++i) {
		  table0[i] = FREE;
		  table1[i] = FREE;
	  }
  }


  bool contains(T key) {
	  OTM_BEGIN();
	  uint32_t p0 = hash0(key) & hashmask(READ_I(cap));
	  uint32_t p1 = hash1(key) & hashmask(READ_I(cap));

	  //__transaction_atomic {

	  if (OTM_SHARED_READ_L(table0[p0]) == key) {
		  return true;
	  }
	  else if (OTM_SHARED_READ_L(table1[p1]) == key) {
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

      uint32_t p0 = hash0(x) & hashmask(READ_I(cap));
      T tmp = OTM_SHARED_READ_L(table0[p0]);
      OTM_SHARED_WRITE_L(table0[p0], x);
      x = tmp;
      if (x == FREE)
        return true;

      uint32_t p1 = hash1(x) & hashmask(READ_I(cap));;
      tmp = OTM_SHARED_READ_L(table1[p1]);
      OTM_SHARED_WRITE_L(table1[p1], x);
      x = tmp;
      if (x == FREE)
        return true;

      //} _transaction_atomic
    }

    resize(cap);

    return add(key);
  }

  bool remove(T key) {
	OTM_BEGIN();
    uint32_t p0 = hash0(key) & hashmask(READ_I(cap));
    uint32_t p1 = hash1(key) & hashmask(READ_I(cap));


    if (OTM_SHARED_READ_L(table0[p0]) == key) {
    	OTM_SHARED_WRITE_L(table0[p0], FREE);
    	return true;
    }
    if (OTM_SHARED_READ_L(table1[p1]) == key) {
    	OTM_SHARED_WRITE_L(table1[1], FREE);
    	return true;
    }

    return false;

    //} _transaction_atomic
  }

  size_t size() {
	  size_t ret = 0;
	  for (int i = 0; i < cap; ++i) {
		  if (table0[i] != FREE) {
			  ++ret;
		  }
		  if (table1[i] != FREE) {
			  ++ret;
		  }
	  }
      return ret;
  }

  int capacity() {
	  return cap;
  }
};





#endif /* TEST_SIMPLEHASHMAP_CUCKOO_MAP_LONG_H_ */
