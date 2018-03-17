//============================================================================
// Author      : Mohamed Saad
// Copyright   : 30 May, 2015
//============================================================================

#ifndef _NORED_
#define _NORED_

#include <setjmp.h>

#include <iostream>
using namespace std;

#include <sstream>

#include "commons.hpp"
using namespace commons;

//#define OPTIMIZE_NOREC
//#define HASH_SET

namespace norec {

static void sys_init() {}
static void sys_shutdown(){}

static volatile int global_time = 0;

class Transaction : public commons::AbstractTransaction{
	jmp_buf* buff;
	int start_time;

private:

	int validate(){
		int time;
		while(true){
//			DEBUG(age << " validate " << time << " != " << global_time);
			time = global_time;
			if((time & 1) != 0)
				continue;
			{
				ListNode<Entry<void**, void*>*>* itr = readset.elements.header;
				while (itr != NULL) {
					Entry<void**, void*>* bucket = itr->value;
#ifdef OPTIMIZE_NOREC
					int bucket_size = readset.getBucketSize(itr->next == NULL);
					for(int i=0; i<bucket_size; i++){
						if (*bucket[i].object != bucket[i].data){
							return -1;
						}
					}
#else
					if (*bucket->object != bucket->data){
						return -1;
					}
#endif
					itr = itr->next;
				}
			}
			if(time == global_time)
				return time;
		}
		return -1;
	}
public:
#ifdef OPTIMIZE_NOREC
	BucketSet<void*> readset;
#ifdef HASH_SET
	HashSet<void*> writeset;
#else
	BloomBucketSet<void**, void*, Entry<void**, void*>> writeset;
#endif
#else
	Set<void*> readset;
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

	void start(jmp_buf* buff){
#ifdef STAT
		starts++;
#endif
		this->buff = buff;
		start_time = global_time & ~(1L);
	}

	TM_INLINE inline void* read(void** so) {
#ifdef STAT2
		reads++;
#endif
//		return *so;
//		DEBUG(age << " read");
		bool found;
		void* v = writeset.find(so, found);
		if(!found){
			v = *so;
			while(start_time != global_time){
				start_time = validate();
				if(start_time < 0)
					abort();
				v = *so;
			}
			readset.add(so, v);
		}
//		DEBUG(age << " readed");
		return v;
	}

	TM_INLINE inline void write(void** so, void* value) {
#ifdef STAT2
		writes++;
#endif
//		*so = value;
//		DEBUG(age << " write");
		writeset.add(so, value);	//TODO don't repeat
//		DEBUG(age << " written");
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
		if(buff){
			DEBUG(age << " retry");
			longjmp (*buff, 1);
		}
		return true;
	}

	void reset(){
		writeset.reset();
		readset.reset();
	}

	bool commit() {
		DEBUG(age << " committing");
		while(!__sync_bool_compare_and_swap(&(global_time), start_time, start_time+1)){
			DEBUG(age << " wait");
			start_time = validate();
			DEBUG(age << " start time=" << start_time);
			if(start_time < 0)
				abort();
		}
		DEBUG(age << " writeback");

#ifdef HASH_SET
		for(int i=0; i<writeset.capacity; i++){
			if(writeset.full[i])
				*writeset.elements[i].object = writeset.elements[i].data;
		}
#else
		{
			ListNode<Entry<void**, void*>*>* itr = writeset.elements.header;
			while (itr != NULL) {
				Entry<void**, void*>* bucket = itr->value;
#ifdef OPTIMIZE_NOREC
				int bucket_size = writeset.getBucketSize(itr->next == NULL);
				for(int i=0; i<bucket_size; i++){
					*bucket[i].object = bucket[i].data;
				}
#else
				*bucket->object = bucket->data;
#endif
				itr = itr->next;
			}
		}
#endif
		global_time = start_time + 2;
		DEBUG(age << " committed");
		reset();
		return true;
	}
};

}

#endif
