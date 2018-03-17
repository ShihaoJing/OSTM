//============================================================================
// Author      : Mohamed Saad
// Copyright   : 30 May, 2015
//============================================================================

#ifndef _OUL_KILL
#define _OUL_KILL

#include <setjmp.h>

#include <iostream>
using namespace std;

#include <sstream>

#include "library_inst.hpp"
#include "commons.hpp"
using namespace commons;

namespace oul_kill {

static volatile int last_completed;
static volatile int validation;

class Transaction;

class Lock {
public:
	Lock() :
			writer(NULL) {
	}
	Transaction* writer;
	List<Transaction*> readers;
};

extern int global_version;
extern Lock locks[LOCK_SIZE];
TM_INLINE inline Lock* getLock(void** so) {
	long index = ((long) so) & LOCK_MASK;
	//		cout << so << " locks " << index);
	return &locks[index];
}

#define ABORTED		1
#define ACTIVE		0
#define UNKNOWN		2
#define COMMITTED	3

class Transaction : public AbstractTransaction{
//	UnsafeList<long*> readset;
	int status;
	Set<void*> writeset;
	jmp_buf* buff;
	int age;

private:

public:
	Transaction(int a) : age(a), buff(NULL), status(ACTIVE) {};

	void start(jmp_buf* buff){
		this->buff = buff;
		LOG("Start Tx " << age);
	}


	void* read(void** so) {
		if(status == ABORTED){
			LOG("Already Aborted4 " << age);
			restart();
		}
		Lock* lock = getLock(so);
read_again:
		Transaction* w = (Transaction*)lock->writer;
		if(w != NULL && (long)w != -1){
			LOG("Another writer " << w->age);
			if (this->age < w->age) {
				if(!w->abort())
					goto read_again;
			} else {
				LOG("Writer1 " << w->age);
				status = ACTIVE;
				this->suicide();
			}
		}
		LOG("Read done " << age);
		return *so;
	}

	void write(void** so, void* value) {
		LOG("Write " << age);
		if(status == ABORTED){
			LOG("Already Aborted3 " << age);
			restart();
		}
		Lock* lock = getLock(so);
again:
		Transaction* w = (Transaction*)lock->writer;
		if (w != NULL && (long)w != -1) {
			LOG("Another writer " << w->age);
			if (this->age < w->age) {
				if(!w->abort())
					goto again;
			} else {
				status = ACTIVE;
				LOG("Writer2 " << w->age);
				this->suicide();
			}
		}
		if(!__sync_bool_compare_and_swap(&(lock->writer), NULL, this))
			goto again;
		LOG("Write done " << age);
		void* oldVal = *so;
		writeset.add(so, oldVal);
		*so = value;
		if(status != ACTIVE){
			if(__sync_bool_compare_and_swap(&(lock->writer), this, -1)){
				*so = oldVal;
				lock->writer = NULL;
			}
			return;
		}
	}

	void restart(){
		if(buff)
			longjmp (*buff, 1);
	}

	void suicide() {
		LOG("Suicide " << age);
		abort();
		restart();
	}

	bool abort() {
		LOG("Try " << age);
		if (status==ABORTED){
			// already aborted
			LOG("Already aborted " << age);
			return true;
		}
		if (status==COMMITTED){
			// already completed
			LOG("Already committed " << age);
			return false;
		}
		while(!__sync_bool_compare_and_swap(&status, ACTIVE, UNKNOWN)){
			// currently aborting
			while(status == UNKNOWN);
			if (status==ABORTED){
				// already aborted
				LOG("Already aborted2 " << age);
				return true;
			}
			if (status==COMMITTED){
				// already completed
				LOG("Already committed2 " << age);
				return false;
			}
		}
		LOG("Revert & Release locks " << age);
		ListNode<Entry<void**, void*>*>* itr = writeset.elements.header;
		while (itr != NULL) {
			Lock* lock = getLock(itr->value->object);
			if(__sync_bool_compare_and_swap(&(lock->writer), this, -1)){
				*itr->value->object = itr->value->data;	// revert value
				lock->writer = NULL;	// unlock
			}
			itr = itr->next;
		}
//		writeset.reset();
		status = ABORTED;
		LOG("Aborted " << age);
		return true;
	}

	bool commit() {
		return true;
	}

	bool complete() {
		if (status==ABORTED){	// already aborted
			restart();
			return false;
		}
		if(!__sync_bool_compare_and_swap(&status, ACTIVE, UNKNOWN)){
			// currently aborting
			while(status == UNKNOWN);
			if (status==ABORTED){	// complete aborting
				restart();
				return false;
			}else{
				LOG("Something wrong is happening!");
			}
		}
		ListNode<Entry<void**, void*>*>* itr = writeset.elements.header;
		LOG("Release locks " << age);
		while (itr != NULL) {
			Lock* lock = getLock(itr->value->object);
			lock->writer = NULL;	// unlock
			itr = itr->next;
		}
		status = COMMITTED;
		LOG("Completed " << age);
		return true;
	}

	void clean(){
		writeset.reset();
	}

	void noRetry(){
		buff = NULL;
	}

	int getAge() {
		return age;
	}

	int getStatus() {
		return status;
	}

};

}

#endif
