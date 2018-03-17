//============================================================================
// Author      : Mohamed Saad
// Copyright   : 30 May, 2015
//============================================================================

#ifndef _OUL_SIMPLE
#define _OUL_SIMPLE

#include <setjmp.h>

#include <iostream>
using namespace std;

#include <sstream>

#include "library_inst.hpp"
#include "commons.hpp"
using namespace commons;

//#define DEBUG_UNDOLOG_VIS

#define TIMEOUT_VALUE		1000

namespace undolog_vis {

static int abort_reason[7];
#define RAW_1		0
#define RAW_2		1
#define WAW_1		2
#define WAW_2		3
#define KILLED		4
#define CASCADE		5
#define TIMEOUT		6

static void init_counters(){
	for(int i=0; i<6; i++){
		abort_reason[i] = 0;
	}
}

static void print_counters(){
	std::cout << "Aborts:" << endl;
	std::cout << "\tRAW_1\t" << abort_reason[RAW_1] << endl;
	std::cout << "\tRAW_2\t" << abort_reason[RAW_2] << endl;
	std::cout << "\tWAW_1\t" << abort_reason[WAW_1] << endl;
	std::cout << "\tWAW_2\t" << abort_reason[WAW_2] << endl;
	std::cout << "\tKILLED\t" << abort_reason[KILLED] << endl;
	std::cout << "\tCASCADE\t" << abort_reason[CASCADE] << endl;
	std::cout << "\tTIMEOUT\t" << abort_reason[TIMEOUT] << endl;
}

#define MAX_READERS	40

#define ACTIVE		1
#define INACTIVE	0
#define TRANSIENT	2
#define PENDING	3

static volatile int last_completed;
static volatile int validation;

class Transaction;

class Lock {
public:
	Lock() :
			writer(NULL) {
	}
	Transaction* writer;
	Transaction* readers[MAX_READERS];
};

extern Lock locks[LOCK_SIZE];
TM_INLINE inline Lock* getLock(void** so) {
	long index = ((long) so) & LOCK_MASK;
	//		cout << so << " locks " << index);
	return &locks[index];
}

struct Transaction{
	long status;
	ReversedSet<void*> writeset;
	jmp_buf* buff;
	int age;
#ifdef DEBUG_UNDOLOG_VIS
	int aborted;
#endif
private:

public:
	Transaction(int a) : status(ACTIVE), buff(NULL), age(a), reason(-1) {
#ifdef DEBUG_UNDOLOG_VIS
		aborted = -1;
#endif
	};
	Transaction() : status(INACTIVE), buff(NULL), age(-1) {
#ifdef DEBUG_UNDOLOG_VIS
		aborted = -1;
#endif
	};

	void start(jmp_buf* buff){
		this->buff = buff;
		LOG("Start Tx " << age);
	}

	int reason;
#ifdef DEBUG_UNDOLOG_VIS
	void* rv;
	void* wv;
#endif
  	TM_INLINE inline long read_l(long* so){
  		long r = DISPATCH<long, sizeof(long), Transaction>::read(so, this);
  		return r;
	}
	TM_INLINE inline int read_i(int* so) {
		return DISPATCH<int, sizeof(int), Transaction>::read(so, this);
	}
	TM_INLINE inline float read_f(float* so) {
		return DISPATCH<float, sizeof(float), Transaction>::read(so, this);
	}
    template <typename T>
    TM_INLINE inline T read_t(T* addr) {
	  return DISPATCH<T, sizeof(T), Transaction>::read(addr, this);
    }
	TM_INLINE inline void* read(void** so) {
		LOG("Read " << age);
		Lock* lock = getLock(so);
		int retries = 0;
read_again:
		if(status == TRANSIENT)
			suicide(KILLED);
		Transaction* writer = lock->writer;
		if(*((long*)writer) != INACTIVE && writer != this) { // && writer->age > this->age){	// abort only speculative writer
			if(retries > TIMEOUT_VALUE)
				suicide(TIMEOUT);
			retries++;
			goto read_again;	// wait till it is aborted
		}
		void* val = *so;
		bool registered = false;
		for(int i=0; i<MAX_READERS; i++){
			Transaction* reader = lock->readers[i];
			if(reader==this || ((*(long*)reader) != ACTIVE) && ((*(long*)reader) != PENDING) && __sync_bool_compare_and_swap(&(lock->readers[i]), reader, this)){
				registered = true;
				break;
			}
		}
		if(!registered)	// too many readers
			goto read_again;
		if(lock->writer != writer)	// writer got changed
			goto read_again;
#ifdef DEBUG_UNDOLOG_VIS
		rv = val;
#endif
		return val;
	}

	TM_INLINE inline void write_l(long* so, long value){
		DISPATCH<long, sizeof(long), Transaction>::write(so, value, this);
	}
	TM_INLINE inline void write_i(int* so, int value){
		DISPATCH<int, sizeof(int), Transaction>::write(so, value, this);
	}
	TM_INLINE inline void write_f(float* so, float value){
		DISPATCH<float, sizeof(float), Transaction>::write(so, value, this);
	}
    template <typename T>
    TM_INLINE inline void write_t(T* addr, T val) {
	  DISPATCH<T, sizeof(T), Transaction>::write(addr, val, this);
    }
	TM_INLINE inline void write(void** so, void* value) {
		if(status == TRANSIENT)
			suicide(KILLED);
		Lock* lock = getLock(so);
write_again:
		Transaction* writer = lock->writer;
		if(writer != this){
			if(*((long*)writer) != INACTIVE){
				if(writer->age > this->age){
					writer->abort(WAW_1);
					goto write_again;	// wait till it is aborted
				}else
					suicide(WAW_2);		// TODO lock steal
			}
			if(!__sync_bool_compare_and_swap(&(lock->writer), writer, this))
				goto write_again;
		}
		for(int i=0; i<MAX_READERS; i++){
			Transaction* reader = lock->readers[i];
			if((*((long*)reader) != INACTIVE) && this != reader)
				reader->abort(RAW_2);
		}
		writeset.add(so, *so);
		LOG("Write " << age << " " << so << " " << value);
		*so = value;
#ifdef DEBUG_UNDOLOG_VIS
		wv = value;
#endif
	}

#ifdef DEBUG_UNDOLOG_VIS
	void dump(){
		std::stringstream ss;
		ss << pthread_self() << ": ";
		if(aborted >= 0){
			ss << "*(" << aborted << ")";
		}
		ss << "Tx" << age << " "; // << read_version << "/" << write_version;
		{
			ss << " ws:[";
			ListNode<Entry<void*, void*>*>* itr = writeset.elements.header;
			while (itr != NULL) {
//				if(itr->next==NULL)
					ss << itr->value->object << "(" << (((int)itr->value->data) + 1) << "), ";
				itr = itr->next;
			}
			ss << "]";
		}
		std::cout << ss.str() << std::endl;
	}
#endif

	void restart(){
		if(buff)
			longjmp (*buff, 1);
	}

	void suicide(int reason) {
		LOG("Suicide " << age);
		rollback(reason);
		restart();
	}

	void rollback(int reason){
#ifdef DEBUG_UNDOLOG_VIS
		aborted = reason;
#endif
		LOG("Rollback " << age);
		abort_reason[reason]++;
		if(reason==KILLED)
			abort_reason[this->reason]++;
		ListNode<Entry<void**, void*>*>* itr = writeset.elements.header;
		while (itr != NULL) {
			*itr->value->object = itr->value->data;	// revert value
			LOG(age << " Restore: " << itr->value->object << " " << itr->value->data);
			itr = itr->next;
		}
		LOG("Aborted " << age);
		status = INACTIVE;
	}

	bool abort(int reason) {
		if(status == INACTIVE){
			LOG("Already Aborted " << age)
			return true;
		}
		if(__sync_bool_compare_and_swap(&status, PENDING, TRANSIENT)){
			rollback(reason);
			return true;
		}
		if( __sync_bool_compare_and_swap(&status, ACTIVE, TRANSIENT)){
			this->reason = reason;
			return true;
		}
		return false;
	}

	bool commit() {
		if(__sync_bool_compare_and_swap(&status, ACTIVE, PENDING)){
			LOG("Committed " << age)
			return true;
		}
		suicide(KILLED);
		return false;
	}

	bool complete() {
		if(__sync_bool_compare_and_swap(&status, PENDING, INACTIVE)){
#ifdef DEBUG_UNDOLOG_VIS
			INFO("Completed " << age << " " << rv << "\t" << wv);
#endif
			return true;
		}
		while(status == TRANSIENT);
		restart();
		return false;
	}

	void clean(){
//		writeset.reset();
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

static void init(){
	undolog_vis::Transaction* dummy = new undolog_vis::Transaction();
	for(int i=0; i<LOCK_SIZE; i++){
		undolog_vis::locks[i].writer = dummy;
		for(int j=0; j<MAX_READERS; j++){
			undolog_vis::locks[i].readers[j] = dummy;
		}
	}
}


}

#endif
