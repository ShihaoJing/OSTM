//============================================================================
// Author      : Mohamed Saad
// Copyright   : 30 May, 2015
//============================================================================

#ifndef _UNDOLOG_
#define _UNDOLOG_

#include <setjmp.h>

#include <iostream>
using namespace std;

#include <sstream>

#include "library_inst.hpp"
#include "commons.hpp"
using namespace commons;

#define TIMEOUT_VALUE		1000

namespace undolog_invis {

class Transaction;

class Lock {
public:
	Lock() :
			lock(0) {
	}
	int lock;
	Transaction* writer;
};

#define WRITE	0x10000

extern Lock locks[LOCK_SIZE];
TM_INLINE inline Lock* getLock(void** so) {
	long index = ((long) so) & LOCK_MASK;
	//		cout << so << " locks " << index);
	return &locks[index];
}

class Transaction : public AbstractTransaction{
	ReversedSet<void*> writeset;
	UnsafeList<Lock*> readset;
	bool aborted;
	int age;
	jmp_buf* buff;

private:

public:
	Transaction(int a) : age(a), buff(NULL), aborted(false) {};

	void start(jmp_buf* buff){
		LOG("Start " << age);
		this->buff = buff;
	}
#ifdef DEBUG_UNDOLOG_INV
	void* rv;
	void* wv;
#endif
	void* read(void** so) {
		Lock* lock = getLock(so);
		LOG("r " << age << " " << lock);
		if(lock->writer == this)
			return *so;	// exists in the current writeset
		int retries = 0;
		if(!readset.exists(lock)){
read_again:
			int l = lock->lock;
			if(l & WRITE){
				if(retries > TIMEOUT_VALUE)
					suicide(0);
				retries++;
				goto read_again;
			}
			if(!__sync_bool_compare_and_swap(&(lock->lock), l, l+1))
				 goto read_again;
			LOG("read " << age << " " << lock << " Atomic: " << l << " " << l+1);
			readset.add(lock);
		}
		void* val = *so;
#ifdef DEBUG_UNDOLOG_INV
		rv = val;
		LOG(age << " Read " << (long)val);
#endif
		return val;
	}

	void write(void** so, void* value) {
		Lock* lock = getLock(so);
		LOG("w " << age << " " << lock);
		if(lock->writer != this){	// not exists in the current writeset
			int retries = 0;
again:
			Transaction *writer = lock->writer;
			int l = lock->lock;
			if(l & WRITE){
				if(writer!=NULL && writer->age > this->age){
					writer->abort();
					goto again;	// wait till it is aborted
				}else
					suicide(0);
			}
			if(l > 1 || (l == 1 && !readset.exists(lock))){
				if(retries > TIMEOUT_VALUE)
					suicide(0);	// another concurrent reader
				retries++;
				goto again;
			}
			if(!__sync_bool_compare_and_swap(&(lock->lock), l, l|WRITE))
				 goto again;
			LOG("write " << age << " " << lock << " Atomic: " << l << " " << (int)(l|WRITE));
		}
		lock->writer = this;
		writeset.add(so, *so);
#ifdef DEBUG_UNDOLOG_INV
		wv = value;
		LOG(age << " Write " << (long)wv);
#endif
		*so = value;
	}

	void restart(){
		if(buff)
			longjmp (*buff, 1);
	}

	void suicide(int reason) {
		LOG("Suicide " << age);
		rollback();
		restart();
	}

	bool rollback() {
		LOG("Rollback " << age);
		ListNode<Lock*>* itr1 = readset.header;
		while (itr1 != NULL) {
			Lock* lock = itr1->value;
unlock_again:
			int l = lock->lock;
			if(!__sync_bool_compare_and_swap(&(lock->lock), l, l-1))	// decrement readers
				goto unlock_again;
			LOG("rollback " << age << " " << lock << " Atomic: " << l << " " << l-1);
			itr1 = itr1->next;
		}

		ListNode<Entry<void**, void*>*>* itr = writeset.elements.header;
		while (itr != NULL) {
			LOG("Revert " << age << " " << (long)*itr->value->object << " to " << (long)itr->value->data);
			*itr->value->object = itr->value->data;	// revert value
			Lock* lock = getLock(itr->value->object);
			LOG("unlock " << age << " Atomic: " << lock->lock << " = 0");
			lock->lock = 0;	// unlock
			itr = itr->next;
		}
//		writeset.reset();
		return true;
	}

	bool abort(){
		return __sync_bool_compare_and_swap(&aborted, false, true);
	}

	bool commit() {
		if(__sync_bool_compare_and_swap(&aborted, true, false)){
			suicide(0);
			return false;
		}
		ListNode<Lock*>* itr1 = readset.header;
		while (itr1 != NULL) {
			Lock* lock = itr1->value;
unlock_again:
			int l = lock->lock;
			if(!__sync_bool_compare_and_swap(&(lock->lock), l, l-1))	// decrement readers
				goto unlock_again;
			itr1 = itr1->next;
		}

		ListNode<Entry<void**, void*>*>* itr = writeset.elements.header;
		while (itr != NULL) {
			Lock* lock = getLock(itr->value->object);
			lock->lock = 0;	// unlock
			itr = itr->next;
		}
#ifdef DEBUG_UNDOLOG_INV
		LOG("Committed " << age << " " << (long)rv << "\t" << (long)wv);
#endif
		return true;
	}
};

}

#endif
