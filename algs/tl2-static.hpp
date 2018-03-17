//============================================================================
// Author      : Mohamed Saad
// Copyright   : 30 May, 2015
//============================================================================

#ifndef _TL2_
#define _TL2_

#include <setjmp.h>

#include <iostream>
using namespace std;

#include <sstream>

#include "commons.hpp"
using namespace commons;

#define MAX_WRITESET	10

namespace tl2 {

class Transaction;

class Lock {
public:
	Lock() :
			writer(NULL), version(1) {
	}
	;
	Transaction* writer;
	int version;
};

static int global_version;
static Lock locks[LOCK_SIZE];

class Transaction : public AbstractTransaction{
	UnsafeList<long*> readset;
	Entry<long*, long>* writeset;
	int writeSetSize;

	jmp_buf* buff;
	int read_version;

	Lock* getLock(long* so) {
		long index = (((long) so) & 0xFFFFFFFF) % LOCK_SIZE;
		//		cout << so << " locks " << index);
		return &locks[index];
	}

private:

public:

	Transaction(){
		writeset = new Entry<long*, long>[MAX_WRITESET];
		writeSetSize = 0;
	}

	void start(jmp_buf* buff){
		this->buff = buff;
		read_version = global_version;
	}

	long read(long* so) {
		readset.add(so);
		bool found = false;
		long v;
		for(int i=0; i<writeSetSize; i++){
			if(writeset[i].object == so){	// TODO better lookup values instead O(n)
				v = writeset[i].data;
				found = true;
				break;
			}
		}
		if(!found)
			v = *so;
		return v;
	}

	void write(long* so, long value) {
		writeset[writeSetSize].object = so;
		writeset[writeSetSize].data = value;	//TODO don't allow repeatition
		writeSetSize++;	}

	bool abort() {
		LOG("Abort");
//		writeset.reset();
		readset.clear();
		if(buff)
			longjmp (*buff, 1);
		return true;
	}

	bool commit() {
		{
			for(int i=0; i<writeSetSize; i++){
				Lock* lock = getLock(writeset[i].object);
				if(!__sync_bool_compare_and_swap(&(lock->writer), NULL, this)){
					for(int j=0; j<i; j++){
						Lock* lock = getLock(writeset[j].object);
						lock->writer = NULL;	// unlock
					}
					abort();
					return false;
				}
			}
		}
		int write_version = __sync_add_and_fetch(&global_version, 1);
		if(write_version != read_version + 1){
			ListNode<long*>* itr = readset.header;
			bool invalid = false;
			while (itr != NULL) {
				Lock* lock = getLock(itr->value);
				if(lock->version > read_version){
					invalid = true;
					break;
				}
				itr = itr->next;
			}
			if(invalid){
				for(int i=0; i<writeSetSize; i++){
					Lock* lock = getLock(writeset[i].object);
					lock->writer = NULL;	// unlock
				}
				abort();
				return false;
			}
		}
		{
			for(int i=0; i<writeSetSize; i++){
				Lock* lock = getLock(writeset[i].object);
				lock->version = write_version;
				*writeset[i].object = writeset[i].data;
				lock->writer = NULL;	// unlock
			}
		}
//		writeset.reset();
		readset.clear();
		LOG("Commit");
		return true;
	}
};

}

#endif
