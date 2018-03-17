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

#include "commons.hpp"
using namespace commons;

namespace oul_steal {

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

struct Transaction;

static struct Transaction* dummy;

class Lock {
public:
	Lock() :
			writer(NULL) {
	}
	Transaction* writer;
	Transaction* readers[MAX_READERS];
};

static Lock locks[LOCK_SIZE];

class WriteSetEntry{
public:
	long* address;
	long value;
	Transaction* owner;
	WriteSetEntry(long* a, long v, Transaction* o) : address(a), value(v), owner(o) {}
};

struct Transaction{
	long status;
	UnsafeList<WriteSetEntry*> writeset;
	jmp_buf* buff;
	int age;

	Lock* getLock(long* so) {
		long index = (((long) so) & 0xFFFFFFFF) % LOCK_SIZE;
		//		cout << so << " locks " << index);
		return &locks[index];
	}

private:

public:
	Transaction(int a) : status(ACTIVE), buff(NULL), age(a), reason(-1) {};
	Transaction() : status(INACTIVE), buff(NULL), age(-1) {};

	void start(jmp_buf* buff){
		this->buff = buff;
		LOG("Start Tx " << age);
	}

	int reason;
	long rv,wv;

	long read(long* so) {
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
				if(reader==this || ((*(long*)reader) != ACTIVE) && __sync_bool_compare_and_swap(&(lock->readers[i]), reader, this)){
					registeredAt = i;
					break;
				}
			}

		if((lock->writer != writer) || ((long)lock->writer & 0x1L))	// writer got changed
			goto read_again;

		long val = *so;
		rv = val;
		LOG("Read " << age << "(" << rv << ")");
		return val;
	}

	void write(long* so, long value) {
		if(status == TRANSIENT)
			suicide(KILLED);
		LOG("Write " << age);
		Lock* lock = getLock(so);
write_again:
		Transaction* writer = (Transaction*)((long)lock->writer & ~0x1L);
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
		for(int i=0; i<MAX_READERS; i++){
			Transaction* reader = lock->readers[i];
			if((*((long*)reader) != INACTIVE) && this->age < reader->age)	// abort only speculative readers
				reader->abort(RAW_2);
		}
		writeset.add(new WriteSetEntry(so, *so, steal ? writer : NULL));
		*so = value;
		wv = value;
		LOG("Write " << age << "(" << wv << ")");
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
		LOG("Rollback " << age);
		abort_reason[reason]++;
		if(reason==KILLED)
			abort_reason[this->reason]++;
		bool stolen = false;
		ListNode<WriteSetEntry*>* itr = writeset.header;
		while (itr != NULL) {
			Lock* lock = getLock(itr->value->address);
			if(__sync_bool_compare_and_swap(&(lock->writer), this, (long)this | 0x1L)){	// I'm still the owner
				WriteSetEntry* entry = itr->value;
				*entry->address = entry->value;	// revert value
				if(entry->owner != NULL){
					lock->writer = entry->owner;	// return stolen lock
				}else{
					lock->writer = dummy;	// unlock
				}
				for(int i=0; i<MAX_READERS; i++){
					Transaction* reader = lock->readers[i];
					if((*((long*)reader) != INACTIVE) && this->age < reader->age)	// abort only speculative readers
						reader->abort(CASCADE);
				}
				// unlock
			}else
				stolen = true;
			itr = itr->next;
		}

		if(stolen){
			LOG("I got stolen " << age);
			status = PENDING;
		}else{
			LOG("Aborted " << age);
			status = INACTIVE;
		}
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
		if(__sync_bool_compare_and_swap(&status, ACTIVE, PENDING)){
			LOG("Committed " << age)
			return true;
		}
		suicide(KILLED);
		return false;
	}

	bool complete() {
		if(__sync_bool_compare_and_swap(&status, PENDING, INACTIVE)){
			LOG("Completed " << age << " " << rv << "\t" << wv);
			return true;
		}
		while(status == TRANSIENT);
		restart();
		return false;
	}

	void clean(){
		writeset.clear();
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
