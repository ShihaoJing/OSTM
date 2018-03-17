//============================================================================
// Author      : Mohamed Saad
// Copyright   : 30 May, 2015
//============================================================================

#ifndef _OUL_SPECULATIVE
#define _OUL_SPECULATIVE

#include <setjmp.h>

#include <iostream>
using namespace std;

#include <sstream>

#include "library_inst.hpp"
#include "commons.hpp"
using namespace commons;

//#define DEBUG_OUL_SPECULATIVE
//#define PROFILE_OUL_SPECULATIVE

namespace oul_speculative {

#ifdef PROFILE_OUL_SPECULATIVE
static pthread_key_t created_tx_key;
#endif

static int abort_reason[6];
#define RAW_1		0
#define RAW_2		1
#define WAW_1		2
#define WAW_2		3
#define KILLED		4
#define CASCADE		5

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
#ifdef PROFILE_OUL_SPECULATIVE
	long long time_exec_start;
	long long time_exec_end;
	long long time_abort_start;
	long long time_abort_end;
	long long time_end;
#endif
private:

public:
	Transaction(int a) : status(ACTIVE), buff(NULL), age(a), reason(-1) {
#ifdef PROFILE_OUL_SPECULATIVE
		time_exec_start = time_exec_end = time_abort_start = time_abort_end = time_end = 0;
		((UnsafeList<Transaction*> *)pthread_getspecific(created_tx_key))->add(this);
#endif
	};
	Transaction() : status(INACTIVE), buff(NULL), age(-1) {
#ifdef PROFILE_OUL_SPECULATIVE
//		time_exec_start = time_exec_end = time_abort_start = time_abort_end = time_end = 0;
//		((UnsafeList<Transaction*> *)pthread_getspecific(created_tx_key))->add(this);
#endif
	};

	void start(jmp_buf* buff){
		this->buff = buff;
		LOG("Start Tx " << age);
#ifdef PROFILE_OUL_SPECULATIVE
		time_exec_start = getElapsedTime();
#endif
	}

	int reason;
#ifdef DEBUG_OUL_SPECULATIVE
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
		int registeredAt = -1;
read_again:
		if(status == TRANSIENT)
			suicide(KILLED);
		Transaction* writer = (Transaction*)((long)lock->writer & ~0x1L);
		if(*((long*)writer) != INACTIVE && writer->age > this->age){	// abort only speculative writer
			writer->abort(RAW_1);
			goto read_again;	// wait till it is aborted
		}

		while(registeredAt < 0)	// too many readers
			for(int i=0; i<MAX_READERS; i++){
				Transaction* reader = lock->readers[i];
				if(reader==this || ((*(long*)reader) != ACTIVE) && ((*(long*)reader) != PENDING) && __sync_bool_compare_and_swap(&(lock->readers[i]), reader, this)){
					registeredAt = i;
					break;
				}
			}

		if((lock->writer != writer) || ((long)lock->writer & 0x1L))	// writer got changed
			goto read_again;

		void* val = *so;
#ifdef DEBUG_OUL_SPECULATIVE
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
		LOG("Write " << age);
		Lock* lock = getLock(so);
write_again:
		if(status == TRANSIENT)
			suicide(KILLED);
		Transaction* writer = (Transaction*)((long)lock->writer & ~0x1L);
		if(writer != this){
			if(*((long*)writer) != INACTIVE){
				if(writer->age > this->age){
					writer->abort(WAW_1);
					goto write_again;	// wait till it is aborted
				}else
					suicide(WAW_2);		// TODO lock steal
			}
			if(!__sync_bool_compare_and_swap(&(lock->writer), writer, (long)this | 0x1L))
				goto write_again;
			writeset.add(so, *so);
		}
		for(int i=0; i<MAX_READERS; i++){
			Transaction* reader = lock->readers[i];
			if((*((long*)reader) != INACTIVE) && this->age < reader->age)	// abort only speculative readers
				reader->abort(RAW_2);
		}
		*so = value;
#ifdef DEBUG_OUL_SPECULATIVE
		wv = value;
#endif
		lock->writer = (Transaction*)((long)lock->writer & ~0x1L);
	}

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
#ifdef PROFILE_OUL_SPECULATIVE
		time_abort_start = getElapsedTime();
#endif
		LOG("Rollback " << age);
		abort_reason[reason]++;
		if(reason==KILLED)
			abort_reason[this->reason]++;
		ListNode<Entry<void**, void*>*>* itr = writeset.elements.header;
		while (itr != NULL) {
			*itr->value->object = itr->value->data;	// revert value
			Lock* lock = getLock(itr->value->object);
			for(int i=0; i<MAX_READERS; i++){
				Transaction* reader = lock->readers[i];
				if((*((long*)reader) != INACTIVE) && this->age < reader->age)	// abort only speculative readers
					reader->abort(CASCADE);
			}
			itr = itr->next;
		}
		LOG("Aborted " << age);
		status = INACTIVE;
#ifdef PROFILE_OUL_SPECULATIVE
		time_abort_end = getElapsedTime();
#endif
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
#ifdef PROFILE_OUL_SPECULATIVE
		time_exec_end = getElapsedTime();
#endif
		if(__sync_bool_compare_and_swap(&status, ACTIVE, PENDING)){
			LOG("Committed " << age)
			return true;
		}
		suicide(KILLED);
		return false;
	}

	bool complete() {
		if(__sync_bool_compare_and_swap(&status, PENDING, INACTIVE)){
#ifdef DEBUG_OUL_SPECULATIVE
			LOG("Completed " << age << " " << rv << "\t" << wv);
#endif
#ifdef PROFILE_OUL_SPECULATIVE
			time_end = getElapsedTime();
#endif
			return true;
		}
		while(status == TRANSIENT);
		restart();
		return false;
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

static void init(){
	oul_speculative::Transaction* dummy = new oul_speculative::Transaction();
	for(int i=0; i<LOCK_SIZE; i++){
		oul_speculative::locks[i].writer = dummy;
		for(int j=0; j<MAX_READERS; j++){
			oul_speculative::locks[i].readers[j] = dummy;
		}
	}

#ifdef PROFILE_OUL_SPECULATIVE
	pthread_key_create(&created_tx_key, NULL);
#endif
}

static void init_thread(){
#ifdef PROFILE_OUL_SPECULATIVE
	UnsafeList<Transaction*> *list = new UnsafeList<Transaction*>();
	pthread_setspecific(created_tx_key, (void*)list);
#endif
}

static void destory_thread(){
#ifdef PROFILE_OUL_SPECULATIVE
	ListNode<Transaction*>* itr = ((UnsafeList<Transaction*> *)pthread_getspecific(created_tx_key))->header;
	pthread_t me = pthread_self();
	while (itr != NULL) {
		Transaction* tx = itr->value;
		INFO(me << ";" <<
				tx->age << ";" <<
				tx->time_exec_start << ";" << tx->time_exec_end << ";" <<
				tx->time_abort_start << ";" << tx->time_abort_end << ";" <<
				tx->time_end
		);
		itr = itr->next;
	}
#endif
}


}

#endif
