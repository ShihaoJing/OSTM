//============================================================================
// Author      : Mohamed Saad
// Copyright   : 30 May, 2015
//============================================================================

#ifndef _OWB_
#define _OWB_

#include <setjmp.h>

#include <iostream>
using namespace std;

#include <sstream>

#include "library_inst.hpp"
#include "commons.hpp"
using namespace commons;

namespace owb {

static volatile int last_completed;
static volatile int validation;

static int abort_reason[7];
#define RAW			0
#define CASCADE_1	1
#define CASCADE_2	2
#define VALIDATE_1	3
#define VALIDATE_2	4
#define LOCKED_1	5
#define LOCKED_2	6

static void init_counters(){
	for(int i=0; i<7; i++){
		abort_reason[i] = 0;
	}
}

static void print_counters(){
	std::cout << "Aborts:" << endl;
	std::cout << "\tRAW\t" << abort_reason[RAW] << endl;
	std::cout << "\tCASCADE_1\t" << abort_reason[CASCADE_1] << endl;
	std::cout << "\tCASCADE_2\t" << abort_reason[CASCADE_2] << endl;
	std::cout << "\tVALIDATE_1\t" << abort_reason[VALIDATE_1] << endl;
	std::cout << "\tVALIDATE_2\t" << abort_reason[VALIDATE_2] << endl;
	std::cout << "\tLOCKED_1\t" << abort_reason[LOCKED_1] << endl;
	std::cout << "\tLOCKED_2\t" << abort_reason[LOCKED_2] << endl;
}

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

extern Lock locks[LOCK_SIZE];
TM_INLINE inline Lock* getLock(void** so) {
	long index = ((long) so) & LOCK_MASK;
	//		cout << so << " locks " << index);
	return &locks[index];
}

#define ABORTED		1
#define ACTIVE		0
#define UNKNOWN		2
#define COMPLETED	3

class Transaction : public AbstractTransaction{
	BucketList<Entry<Lock*, int>*> readset;
	BloomBucketSet<void**, void*, Entry<void**, void*>> writeset;
	SafeList<Transaction*> dependencies;
	int age;
	volatile int status;
	jmp_buf* buff;
	volatile bool cleaned;
	int initial_last_completed;

private:
	// make sure no body has completed since the transaction started execution
	bool validateReadSet() {
            int temp = last_completed;
		if(temp == initial_last_completed) return true;
		ListNode<Entry<Lock*, int>**>* itr = readset.elements.header;
		while (itr != NULL) {
			Entry<Lock*, int>** bucket = itr->value;
			int bucket_size = readset.getBucketSize(itr->next == NULL);
			for(int i=0; i<bucket_size; i++){
				Entry<Lock*, int>* entry = bucket[i];
				Lock* lock = entry->object;
				int v = lock->version;
				LOG("Commit Validate " << age << " [" << v << ", " << entry->data << "]");
				if (v != entry->data && lock->writer != this){
					LOG("Read Wrong Value");
					status = ACTIVE;
					this->suicide(VALIDATE_2);
					return false;
				}
			}
			itr = itr->next;
		}
        initial_last_completed = temp;
		LOG("Commit Valid "  << age);
		return true;
	}

	// make sure no body has completed since the transaction started commit
	bool validateWriteSet() {
		ListNode<Entry<Lock*, int>**>* itr = readset.elements.header;
		while (itr != NULL) {
			Entry<Lock*, int>** bucket = itr->value;
			int bucket_size = readset.getBucketSize(itr->next == NULL);
			for(int i=0; i<bucket_size; i++){
				Entry<Lock*, int>* entry = bucket[i];
				Lock* lock = entry->object;
				int v = lock->version;
				LOG("Post Commit Validate "  << age << " [" << v << ", " << entry->data << "]");
				if(lock->writer == this && v != 1 + entry->data){
					LOG("Concurrently Changed");
					status = ACTIVE;
					this->suicide(VALIDATE_2);
					return false;
				}
			}
			itr = itr->next;
		}
		LOG("Post Commit Valid "  << age);
		return true;
	}

	void restart() {
		LOG("Restarting by tx itself");
		if(buff)
			longjmp (*buff, 1);
	}
public:

	Transaction(int a) :
			age(a), status(ACTIVE), buff(NULL), cleaned(false) {
		initial_last_completed = last_completed;
	}

	void noRetry(){
		buff = NULL;
	}

	void suicide(int reason) {
		LOG("Suicide " << age);
		abort(reason);
		restart();
	}

	void start(jmp_buf* buff){
		this->buff = buff;
	}

	inline void* read(void** so) {
		LOG("Age " << age << " Start Reading");
		Lock* lock = getLock(so);
		Entry<Lock*, int>* e = new Entry<Lock*, int>();
		e->object = lock;
		bool found = false;
		void* v = writeset.find(so, found);
		if (found){
			LOG("Read " << age << " <" << v << ">");
			e->data = lock->version;
			readset.add(e);
			return v;
		}
readAgain:
		int dep = 0;
		v = *so;
		Transaction* w = (Transaction*)lock->writer;
		int ver = lock->version;
		e->data = ver;
		if (w != NULL) {
			if (w->age > this->age){
				w->abort(RAW);
				goto readAgain;
			}else{
				dep = w->age;
				w->dependencies.add(this);
				if(w->status != ACTIVE){	// needed because writer may be aborted before I register as a dependency
					// TODO double check this case
					this->suicide(CASCADE_1);
					return 0;
				}
				LOG("Add Dependency " << age << " to " << dep);
			}
		}
		if(v != *so || ver != lock->version)
			goto readAgain;
		validateReadSet();
		readset.add(e);
		LOG("Read " << age << " <" << v << ", v" << ver << ", d" << dep << ">");
//		if(readSetSize > MAX_READSET) LOG("READ OVERFLOW!!");
		return v;
	}

	inline void write(void** so, void* value) {
		LOG("Write " << age << " " << so << " <" << value << ">");
		writeset.add(so, value);	//TODO don't allow repeatition
//		if(writeSetSize > MAX_WRITESET) INFO("WRITE OVERFLOW!!");
	}

	bool abort(int reason) {
		LOG("Aborting " << age << " Reason " << reason);
		abort_reason[reason]++;
		if (status==ABORTED){
			// already aborted
			LOG("Already aborted " << age);
			return true;
		}
		if (status==COMPLETED){
			// already completed
			LOG("Already completed " << age);
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
			if (status==COMPLETED){
				// already completed
				LOG("Already completed2 " << age);
				return false;
			}
		}
		LOG("Abort " << age);
		cleaned = true;
		{
			ListNode<Transaction*>* itr = dependencies.header;
			while (itr != NULL) {
				LOG("Abort Dependency " << itr->value->age);
				itr->value->abort(CASCADE_2);
				itr = itr->next;
			}
			dependencies.reset();
			LOG("Dependencies Done");
		}
		{
			ListNode<Entry<void**, void*>*>* itr = writeset.elements.header;
			while (itr != NULL) {
				Entry<void**, void*>* bucket = itr->value;
				int bucket_size = writeset.getBucketSize(itr->next == NULL);
				for(int i=0; i<bucket_size; i++){
					Lock* lock = getLock(bucket[i].object);
					if (lock->writer == this) {
						if(reason != LOCKED_2){
							LOG("Restore " << bucket[i].object << " " << bucket[i].data);
							*bucket[i].object = bucket[i].data;
						}
	//					lock->version--;
						lock->writer = NULL;
					}else{
						LOG("Can't Restore " << bucket[i].object << " " << bucket[i].data);
					}
				}
				itr = itr->next;
			}
//			delete writeset;	//TODO clean
//			delete readset;	//TODO clean
		}
		LOG("Aborted " << age);
		status = ABORTED;
		return true;
	}

	bool commit() {
		return commit(false);
	}

	bool commit(bool complete) {
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
		if (validation==1 && !validateReadSet())
			return false;
		LOG("Commit " << age);
		{
			ListNode<Entry<void**, void*>*>* itr = writeset.elements.header;
			while (itr != NULL) {
				Entry<void**, void*>* bucket = itr->value;
				int bucket_size = writeset.getBucketSize(itr->next == NULL);
				for(int i=0; i<bucket_size; i++){
					Lock* lock = getLock(bucket[i].object);
again:
					Transaction* w = (Transaction*)lock->writer;
					if (w != NULL) {
						if(w == this)
							continue;
						if (this->age < w->age) {
							if(!w->abort(LOCKED_1))
								goto again;
						} else {
							status = ACTIVE;
							LOG(age << " Active Writer " << w->age);
							this->suicide(LOCKED_2);
							return false;
						}
					}
					if(!__sync_bool_compare_and_swap(&(lock->writer), NULL, this))
						goto again;
					LOG("Writer= " << lock->writer);
				}
				itr = itr->next;
			}

		}
		{
			ListNode<Entry<void**, void*>*>* itr = writeset.elements.header;
			while (itr != NULL) {
				Entry<void**, void*>* bucket = itr->value;
				int bucket_size = writeset.getBucketSize(itr->next == NULL);
				for(int i=0; i<bucket_size; i++){
					Lock* lock = getLock(bucket[i].object);
					lock->version++;
					void* temp = bucket[i].data;
					bucket[i].data = *bucket[i].object;
					*bucket[i].object = temp;
					Transaction* tx = (Transaction*)lock->writer;
					LOG("Expose '" << temp << "'->'" << temp << "' v" << lock->version << " w" << (tx==NULL ? 0 : tx->age));
				}
				itr = itr->next;
			}


		}
		if(complete){
			ListNode<Entry<void**, void*>*>* itr = writeset.elements.header;
			while (itr != NULL) {
				Entry<void**, void*>* bucket = itr->value;
				int bucket_size = writeset.getBucketSize(itr->next == NULL);
				for(int i=0; i<bucket_size; i++){
					getLock(bucket[i].object)->writer = NULL;
				}
				itr = itr->next;
			}

			LOG("CommittedAndCompleted " << age);
			status = COMPLETED;
		}else{
			if((validation==1 || last_completed > initial_last_completed) && !validateWriteSet())
				return false;
			LOG("Committed " << age);
			status = ACTIVE;
		}
		return true;
	}

	bool complete() {
		LOG("Completing " << age);
		if (status==ABORTED){	// already aborted
			LOG("Already Aborted");
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
		if (!validateReadSet())
			return false;
		ListNode<Entry<void**, void*>*>* itr = writeset.elements.header;
		while (itr != NULL) {
			Entry<void**, void*>* bucket = itr->value;
			int bucket_size = writeset.getBucketSize(itr->next == NULL);
			for(int i=0; i<bucket_size; i++){
				getLock(bucket[i].object)->writer = NULL;
			}
			itr = itr->next;
		}

		LOG("Completed " << age);
		status = COMPLETED;
		return true;
	}

	void clean(){
		if(cleaned)
			 return;
		LOG("Clean " << age << " ws=" << writeset.size << " rs="<< readset.size << " dep=" << dependencies.size());
		cleaned = true;
		LOG("Clean WriteSet " << age);
		writeset.reset();
		LOG("Clean ReadSet " << age);
		readset.reset();
		LOG("Clean Dependencies " << age);
		dependencies.reset();
		LOG("Clean Done " << age);
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
