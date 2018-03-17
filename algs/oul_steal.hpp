//============================================================================
// Author      : Mohamed Saad
// Copyright   : 30 May, 2015
//============================================================================

#ifndef _OUL_STEAL
#define _OUL_STEAL

#include <setjmp.h>

#include <iostream>
using namespace std;

#include <sstream>

#include "library_inst.hpp"
#include "commons.hpp"
using namespace commons;

//#define DEBUG_OUL_STEAL
//#define PROFILE_OUL_STEAL

namespace oul_steal {

#ifdef PROFILE_OUL_STEAL
static pthread_key_t created_tx_key;
#endif

static int abort_reason[8];
#define RAW_1		0
#define RAW_2		1
#define WAW_1		2
#define WAW_2		3
#define KILLED		4
#define CASCADE		5
#define CLEAN		6
#define LOOP		7

static void init_counters(){
	for(int i=0; i<8; i++){
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
	std::cout << "\tCLEAN\t" << abort_reason[CLEAN] << endl;
	std::cout << "\tLOOP\t" << abort_reason[LOOP] << endl;
}

#define MAX_READERS	40

#define ACTIVE		1
#define INACTIVE	0
#define TRANSIENT	2
#define PENDING	3

static volatile int last_completed;
static volatile int validation;

struct Transaction;

extern struct Transaction* dummy;

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

class WriteSetEntry{
public:
	void** address;
	void* value;
	Transaction* owner;
	WriteSetEntry(void** a, void* v, Transaction* o) : address(a), value(v), owner(o) {}
};

struct Transaction{
	long status;
	UnsafeReversedList<WriteSetEntry*> writeset;
	jmp_buf* buff;
	int age;
	bool aborted;
#ifdef DEBUG_OUL_STEAL
	int aborted_reason;
#endif
#ifdef PROFILE_OUL_STEAL
	long long time_exec_start;
	long long time_exec_end;
	long long time_abort_start;
	long long time_abort_end;
	long long time_end;
#endif
private:

public:
	Transaction(int a) : status(ACTIVE), buff(NULL), age(a), aborted(false), reason(-1) {
#ifdef DEBUG_OUL_STEAL
		aborted_reason = -1;
#endif
#ifdef PROFILE_OUL_STEAL
		time_exec_start = time_exec_end = time_abort_start = time_abort_end = time_end = 0;
		((UnsafeList<Transaction*> *)pthread_getspecific(created_tx_key))->add(this);
#endif
	};
	Transaction() : status(INACTIVE), buff(NULL), aborted(false), age(-1) {
#ifdef DEBUG_OUL_STEAL
		aborted_reason = -1;
#endif
//#ifdef PROFILE_OUL_STEAL
//		time_exec_start = time_exec_end = time_abort_start = time_abort_end = time_end = 0;
//		((UnsafeList<Transaction*> *)pthread_getspecific(created_tx_key))->add(this);
//#endif
	};

	void start(jmp_buf* buff){
		this->buff = buff;
		LOG("Start Tx " << age);
#ifdef PROFILE_OUL_STEAL
		time_exec_start = getElapsedTime();
#endif
	}

	int reason;
#ifdef DEBUG_OUL_STEAL
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
#ifdef DEBUG_OUL_STEAL
		rv = val;
		LOG("Read " << age << "(" << rv << ")");
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
			bool steal = false;
			if(*((long*)writer) != INACTIVE){
				if(writer->age > this->age){
					writer->abort(WAW_1);
					goto write_again;	// wait till it is aborted
				}else{
					LOG("Steal lock from " << writer->age);
					steal = true;
				}
			}
			if(!__sync_bool_compare_and_swap(&(lock->writer), writer, (long)this | 0x1L))
				goto write_again;
			writeset.add(new WriteSetEntry(so, *so, steal ? writer : NULL));
		}
		for(int i=0; i<MAX_READERS; i++){
			Transaction* reader = lock->readers[i];
			if((*((long*)reader) != INACTIVE) && this->age < reader->age)	// abort only speculative readers
				reader->abort(RAW_2);
		}
		*so = value;
#ifdef DEBUG_OUL_STEAL
		wv = value;
		LOG("Write " << age << "(" << wv << ")");
#endif
		lock->writer = (Transaction*)((long)lock->writer & ~0x1L);
		if(status == TRANSIENT)
			suicide(LOOP);
	}

#ifdef DEBUG_OUL_STEAL
	void dump(){
		std::stringstream ss;
		ss << pthread_self() << ": ";
		if(aborted_reason >= 0){
			ss << "*(" << aborted_reason << ")";
		}
		ss << "Tx" << age << " "; // << read_version << "/" << write_version;
		{
			ss << " ws:[";
			ListNode<WriteSetEntry*>* itr = writeset.header;
			while (itr != NULL) {
				if(itr->next==NULL)
					ss << itr->value->address << "(" << (((long)itr->value->value) + 1) << "), ";
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
#ifdef DEBUG_OUL_STEAL
		aborted_reason = reason;
#endif
#ifdef PROFILE_OUL_STEAL
		time_abort_start = getElapsedTime();
#endif
		LOG("Rollback " << age);
		if(reason!=CLEAN)
			abort_reason[reason]++;
		if(reason==KILLED)
			abort_reason[this->reason]++;
		aborted = true;
		ListNode<WriteSetEntry*>* itr = writeset.header;
		while (itr != NULL) {
			Lock* lock = getLock(itr->value->address);
			if(__sync_bool_compare_and_swap(&(lock->writer), this, (long)this | 0x1L)){	// I'm still the owner
				WriteSetEntry* entry = itr->value;
				LOG("Revert " << entry->address << " from " <<  *entry->address << " to " << entry->value);
				*entry->address = entry->value;	// revert value
				if(entry->owner != NULL){
					WriteSetEntry* stolenEntry = entry;
					bool found = true;
					do{
						if(!stolenEntry->owner->aborted){
							lock->writer = stolenEntry->owner;	// return stolen lock	// FIXME race between condition and here
							LOG("Return lock to owner: " << stolenEntry->owner->age);
							break;	// leave it
						}
						ListNode<WriteSetEntry*>* itrSt = stolenEntry->owner->writeset.header;
						found = false;
						while (itrSt != NULL) {
							if(itrSt->value->address == entry->address){
								// found the entry at the old owner write-set
								LOG("Owner " << stolenEntry->owner->age << " Revert " << entry->address << " from " <<  *entry->address << " to " << itrSt->value->value);
								*entry->address = itrSt->value->value;
								stolenEntry = itrSt->value;
								found = true;
								break;
							}
							itrSt = itrSt->next;
						}
					}while((stolenEntry->owner != NULL) && found);
					if(stolenEntry->owner == NULL || stolenEntry->owner->aborted || !found){
						LOG("Unlock Address " << itr->value->address);
						lock->writer = dummy;	// unlock
					}
//					lock->writer = entry->owner;	// return stolen lock
//					if(entry->owner->aborted){
//						entry->owner->rollback(CLEAN);
//					}
				}else{
					lock->writer = dummy;	// unlock
				}
			}else{
				LOG("Stolen Address " << itr->value->address);
			}
			for(int i=0; i<MAX_READERS; i++){
				Transaction* reader = lock->readers[i];
				if((*((long*)reader) != INACTIVE) && this->age < reader->age)	// abort only speculative readers
					reader->abort(CASCADE);
			}
			itr = itr->next;
		}

		LOG("Rollback Done!" << age);
//		if(!stolen){
//			LOG("I got stolen " << age);
//			status = PENDING;				// we can't do that because non-stolen locks won't be released when the Tx is in Pending state
//		}else{
//			LOG("Aborted " << age);
			status = INACTIVE;
//		}
#ifdef PROFILE_OUL_STEAL
		time_abort_end = getElapsedTime();
#endif
	}

	bool abort(int reason) {
		if(status == INACTIVE){
			LOG("Already Aborted " << age);
			return true;
		}
		if(__sync_bool_compare_and_swap(&status, PENDING, TRANSIENT)){
			rollback(reason);
			return true;
		}
		if( __sync_bool_compare_and_swap(&status, ACTIVE, TRANSIENT)){
			LOG("Abort " << age);
			this->reason = reason;
			return true;
		}
		LOG("Failed Abort " << age);
		return false;
	}

	bool commit() {
#ifdef PROFILE_OUL_STEAL
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
#ifdef DEBUG_OUL_STEAL
			LOG("Completed " << age << " " << rv << "\t" << wv);
#endif
#ifdef PROFILE_OUL_STEAL
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
	dummy = new Transaction();
	for(int i=0; i<LOCK_SIZE; i++){
		oul_steal::locks[i].writer = dummy;
		for(int j=0; j<MAX_READERS; j++){
			oul_steal::locks[i].readers[j] = dummy;
		}
	}

#ifdef PROFILE_OUL_STEAL
	pthread_key_create(&created_tx_key, NULL);
#endif
}

static void init_thread(){
#ifdef PROFILE_OUL_STEAL
	UnsafeList<Transaction*> *list = new UnsafeList<Transaction*>();
	pthread_setspecific(created_tx_key, (void*)list);
#endif
}

static void destory_thread(){
#ifdef PROFILE_OUL_STEAL
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
