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

//#define OPTIMIZE_TL2
//#define DEBUG_TL2

#include "library_inst.hpp"
#include "commons.hpp"
using namespace commons;

namespace tl2 {

class Transaction;

class Lock {
public:
	Lock() :
			writer(NULL), version(1) {
	}
	Transaction* writer;
	int version;
};

static void sys_init() {}
static void sys_shutdown(){}

extern int global_version;

extern Lock locks[LOCK_SIZE];
TM_INLINE inline Lock* getLock(void** so) {
	long index = ((long) so) & LOCK_MASK;
	//		cout << so << " locks " << index);
	return &locks[index];
}

class Transaction : public commons::AbstractTransaction{
	jmp_buf* buff;
	int read_version;
#ifdef DEBUG_TL2
	int aborted;
	int write_version;
#endif

private:

	void release_writeset(){
		ListNode<Entry<void**, void*>*>* itr = writeset.elements.header;
		while (itr != NULL) {
			Entry<void**, void*>* bucket = itr->value;
#ifdef OPTIMIZE_TL2
			int bucket_size = writeset.getBucketSize(itr->next == NULL);
			for(int i=0; i<bucket_size; i++){
				Lock* lock = getLock(bucket[i].object);
				lock->writer = NULL;	// unlock
			}
#else
			Lock* lock = getLock(bucket->object);
			lock->writer = NULL;	// unlock
#endif
			itr = itr->next;
		}
	}

public:
#ifdef OPTIMIZE_TL2
	BucketList<void**> readset;
	BloomBucketSet<void**, void*, Entry<void**, void*>> writeset;
#else
	UnsafeList<void**> readset;
	Set<void*> writeset;
#endif

	int age;
#ifdef STAT
	int starts;
	int aborts;
#ifdef STAT2
	int reads;
	int writes;
#endif
#endif

	Transaction(int id) : age(id) {
#ifdef DEBUG_TL2
		aborted = 0;
#endif
	}
	Transaction() : age(-1) {
#ifdef DEBUG_TL2
		aborted = 0;
#endif
	}

	void start(jmp_buf* buff){
#ifdef STAT
		starts++;
#endif
		this->buff = buff;
		read_version = global_version;
	}

	void* read(void** so) {
#ifdef STAT2
		reads++;
#endif
		DEBUG(age << " read");
		Lock* lock = getLock(so);
		readset.add(so);
		bool found;
		void* v = writeset.find(so, found);
		if(!found){
			int v1 = lock->version;
			if(lock->writer != NULL){
				DEBUG("------5------")
				abort();
			}
			v = *so;
			int v2 = lock->version;
			if(v1 > read_version || v1 != v2){
				DEBUG("------6------")
				abort();
			}
		}
		DEBUG(age << " readed");
		return v;
	}

	void write(void** so, void* value) {
#ifdef STAT2
		writes++;
#endif
		DEBUG(age << " write");
		writeset.add(so, value);	//TODO don't repeat
		DEBUG(age << " written");
	}

	void suicide(int reason) {
		abort();
	}

	bool abort() {
#ifdef STAT
		aborts++;
#endif
		DEBUG(age << " Abort");
		reset();
		if(buff)
			longjmp (*buff, 1);
		return true;
	}

#ifdef DEBUG_TL2
	void dump(){
		std::stringstream ss;
		ss << pthread_self() << ": ";
		if(aborted){
			ss << "*(" << aborted << ")";
		}
		ss << "Tx" << age << " " << read_version << "/" << write_version;
		{
			ss << " ws:[";
			ListNode<Entry<void**, void*>*>* itr = writeset.elements.header;
			while (itr != NULL) {
				ss << itr->value->object << "(" << (int)itr->value->data << "), ";
				itr = itr->next;
			}
			ss << "]";
		}
		{
			ss << " rs:[";
			ListNode<void**>* itr = readset.header;
			while (itr != NULL) {
				ss << itr->value << ", ";
				itr = itr->next;
			}
			ss << "]";
		}
		std::cout << ss.str() << std::endl;
	}
#endif

	bool commit() {
		DEBUG(age << " Commit");
		{
			ListNode<Entry<void**, void*>*>* itr = writeset.elements.header;
#ifdef OPTIMIZE_TL2
			while (itr != NULL) {
				Entry<void**, void*>* bucket = itr->value;
				int bucket_size = writeset.getBucketSize(itr->next == NULL);
				for(int i=0; i<bucket_size; i++){
					Lock* lock = getLock(bucket[i].object);
					if(!__sync_bool_compare_and_swap(&(lock->writer), NULL, this) && lock->writer != this){
						ListNode<Entry<void**, void*>*>* itr2 = writeset.elements.header;
						while (itr2 != itr) {
							Entry<void**, void*>* bucket2 = itr2->value;
							int bucket_size2 = writeset.getBucketSize(itr2->next == NULL);
							for(int i2=0; i2<bucket_size2; i2++){
								Lock* lock2 = getLock(bucket2[i2].object);
								lock2->writer = NULL;	// unlock
							}
							itr2 = itr2->next;
						}
						for(int i3=0; i3<i-1; i3++){
							Lock* lock3 = getLock(bucket[i].object);
							lock3->writer = NULL;	// unlock
						}
	#ifdef DEBUG_TL2
						aborted = 1;
	#endif
						DEBUG("------1------")
						abort();
						return false;
					}
				}
				itr = itr->next;
			}
#else
			while (itr != NULL) {
				Entry<void**, void*>* bucket = itr->value;
				Lock* lock = getLock(bucket->object);
				if(!__sync_bool_compare_and_swap(&(lock->writer), NULL, this) && lock->writer != this){
					ListNode<Entry<void**, void*>*>* itr2 = writeset.elements.header;
					while (itr2 != itr) {
						Entry<void**, void*>* bucket2 = itr2->value;
						Lock* lock2 = getLock(bucket2->object);
						lock2->writer = NULL;	// unlock
						itr2 = itr2->next;
					}
#ifdef DEBUG_TL2
					aborted = 1;
#endif
					DEBUG("------1------")
					abort();
					return false;
				}
				itr = itr->next;
			}
#endif
		}

		int write_version = 1 + __sync_fetch_and_add(&global_version, 1);
#ifdef DEBUG_TL2
		this->write_version = write_version;
#endif

		if(write_version != read_version + 1){
			bool invalid = false;
			{
#ifdef OPTIMIZE_TL2
				ListNode<void***>* itr = readset.elements.header;
				while (itr != NULL) {
					void*** bucket = itr->value;
					int bucket_size = readset.getBucketSize(itr->next == NULL);
					for(int i=0; i<bucket_size; i++){
						Lock* lock = getLock(bucket[i]);
						if(lock->version > read_version){
							invalid = true;
							break;
						}
					}
					if(invalid) break;
					itr = itr->next;
				}
#else
				ListNode<void**>* itr = readset.header;
				while (itr != NULL) {
					void** bucket = itr->value;
					Lock* lock = getLock(bucket);
					if(lock->version > read_version){
						invalid = true;
						break;
					}
					itr = itr->next;
				}
#endif
			}

			if(invalid){
				release_writeset();
#ifdef DEBUG_TL2
				aborted = 2;
#endif
				DEBUG("------4------")
				abort();
				return false;
			}
		}
		{
			ListNode<Entry<void**, void*>*>* itr = writeset.elements.header;
			while (itr != NULL) {
				Entry<void**, void*>* bucket = itr->value;
#ifdef OPTIMIZE_TL2
				int bucket_size = writeset.getBucketSize(itr->next == NULL);
				for(int i=0; i<bucket_size; i++){
					Lock* lock = getLock(bucket[i].object);
					*bucket[i].object = bucket[i].data;
					lock->version = write_version;
					lock->writer = NULL;	// unlock
				}
#else
				Lock* lock = getLock(bucket->object);
				*bucket->object = bucket->data;
				lock->version = write_version;
				lock->writer = NULL;	// unlock
#endif
				itr = itr->next;
			}
		}
		DEBUG(age << " Committed");
		reset();
		return true;
	}

	void reset(){
		writeset.reset();
		readset.reset();
	}
};

}

#endif
