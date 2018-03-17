/*
 * TCM.hpp
 *
 *  Created on: Jul 21, 2015
 *      Author: root
 */

#ifndef TCM_HPP_
#define TCM_HPP_

#include <setjmp.h>
#include <stdint.h>
#include <iostream>
#include <functional>
#include <string>

#include "library_inst.hpp"
#include "commons.hpp"
using namespace commons;

namespace stmlite{

static int operations = 0;

#define EMPTY	100000000

#define SIGNATURE_LENGTH 	16

#define MAX_WRITESET	260

static volatile int global_time = 0;
static volatile int writerThread = -1;

class Transaction : public commons::AbstractTransaction{
public:
	int age;
#ifdef STAT
	int starts;
	int aborts;
#ifdef STAT2
	int reads;
	int writes;
#endif
#endif
	int start_version;
	volatile int commit_version;
	BloomBucketSet<void**, void*, Entry<void**, void*>> writeset;
	Signature<void**> readSignature;
	volatile bool abort_flag;
	volatile bool commit_flag;
	jmp_buf* buff;
	Transaction() : age(-1), abort_flag(false), commit_flag(false), commit_version(-1) {
	};
	Transaction(int a) : age(a), abort_flag(false), commit_flag(false), commit_version(-1) {
	};

	void reset(){
		DEBUG("Reset " << age);
		writeset.reset();
		readSignature.reset();
		commit_flag = false;
		abort_flag = false;
		commit_version = -1;
	}

	void start(jmp_buf* buff){
#ifdef STAT
		starts++;
#endif
		DEBUG("Start " << age);
		this->buff = buff;
		start_version = global_time; //__sync_fetch_and_add(&global_time, 1);
	}
#ifdef DEBUG_STMLITE
	void* rv;
#endif
	void* read(void** so) {
		DEBUG("Read " << age);
#ifdef STAT2
		reads++;
#endif
		readSignature.insert(so);
		bool found;
		void* v = writeset.find(so, found);
		if(!found){
			v = *so;
		}
#ifdef DEBUG_STMLITE
		rv = v;
#endif
		DEBUG("Read Done " << age);
		return v;
	}

	void write(void** so, void* value) {
		DEBUG("Write " << age);
#ifdef STAT2
		writes++;
#endif
		writeset.add(so, value);
		DEBUG("Write Done" << age);
	}

	void commit(){
		DEBUG("Commit " << age);
		ListNode<Entry<void**, void*>*>* itr = writeset.elements.header;
		while (itr != NULL) {
			Entry<void**, void*>* bucket = itr->value;
			int bucket_size = writeset.getBucketSize(itr->next == NULL);
			for(int i=0; i<bucket_size; i++){
				*bucket[i].object = bucket[i].data;
			}
			itr = itr->next;
		}

		DEBUG("T" << age << " Commit done\n");

		commit_version = __sync_fetch_and_add(&global_time, 1);
	}

	void suicide(int reason){
		abort();
	}

	bool abort() {
#ifdef STAT
		aborts++;
#endif
		DEBUG("Abort" << age);
		longjmp (*buff, 1);
		return true;
	}
};

class PreCommitLogEntry{
public:
	Transaction* tx;
	Signature<void**> writeSignature;
	Signature<void**> readSignature;
	volatile bool ready;
	bool fresh;

	PreCommitLogEntry() : tx(NULL), ready(false), fresh(true) {}

	void setPreCommitLogEntry(Transaction* transaction){
		ready = false;
		tx = transaction;
		readSignature.copy(&tx->readSignature);
		writeSignature.copy(&tx->writeset.signature);
		ready = true;
	}
};

class CommitLogEntry{
public:
	int commit_version;
	Signature<void**> writeSignature;
	bool valid;
//	int validation;
//	Transaction* tx;

	CommitLogEntry() : commit_version(-1), valid(false) {};

	void reset(){
		commit_version = -1;
		valid = false;
	}

	void setCommitLogEntry(PreCommitLogEntry* entry, int commitVersion){
//		tx =  entry->tx;
		commit_version = commitVersion;
		writeSignature.copy(&entry->writeSignature);
//		validation = 0;
	}
};

class TCM{
	int preCommitLogLength;
	PreCommitLogEntry* preCommitLog;
	int commitLogLength;
	CommitLogEntry* commitLog;
	int* aliveTx;
	int nextToCommit;

	bool conflictCheck(PreCommitLogEntry* preCommitEntry, int index){
		DEBUG("Check " << preCommitEntry->tx->age);
		int mask = 1 << index;
//		bool force = preCommitEntry->fresh;
//		preCommitEntry->fresh = false;
		for(int i=0; i<commitLogLength; i++){
			if(commitLog[i].valid){
//				if(!force && (mask & commitLog[i].validation))
//					continue;
//				commitLog[i].validation |= mask;
				DEBUG("Compare " << preCommitEntry->tx->age << " " << commitLog[i].commit_version);
				if(preCommitEntry->tx->start_version < commitLog[i].commit_version){
					DEBUG("SigCompare");
					if(commitLog[i].writeSignature.intersec(&preCommitEntry->readSignature)){
						DEBUG("\tFail");
						return false;
					}else{
						DEBUG("\tPass");
					}
				}else{
					DEBUG("Skip");
				}
			}
		}
		DEBUG("Okay " << preCommitEntry->tx->age);
		return true;
	}

public:
	TCM(int w) {
		preCommitLogLength = w;
		commitLogLength = w*5;
		preCommitLog = new PreCommitLogEntry[w];
		aliveTx = new int[w];
		commitLog = new CommitLogEntry[commitLogLength];
		clean();
	};

	void clean(){
		for(int i=0; i<preCommitLogLength; i++)
			aliveTx[i] = EMPTY;
		for(int i=0; i<commitLogLength; i++)
			commitLog[i].reset();
		nextToCommit = 0;
	}

	void cleanCommitedLog(){
		int minSV = aliveTx[0];
		for(int i=1; i<preCommitLogLength; i++)
			if(aliveTx[i] < minSV)
				minSV = aliveTx[i];
		DEBUG("MinSV=" << minSV);
		for(int i=0; i<commitLogLength; i++){
			if(commitLog[i].valid && commitLog[i].commit_version < minSV){
				DEBUG("Clean " << commitLog[i].commit_version);
				commitLog[i].valid = false;
			}
		}
	}

	void validatePreCommitted(){
		for(int i=0; i<preCommitLogLength; i++){
			if(preCommitLog[i].ready){
				if(conflictCheck(&preCommitLog[i], i)){
					if(nextToCommit == preCommitLog[i].tx->age){
						nextToCommit++;
						int commit_version = global_time + 1;
						preCommitLog[i].tx->commit_flag = true;
						bool inserted = false;
						while(!inserted){
							DEBUG("Insert " << preCommitLog[i].tx->age);
							for(int j=0; j<commitLogLength; j++){
								if(!commitLog[j].valid){
									DEBUG("Add to commit log " << preCommitLog[i].tx->age);
									commitLog[j].setCommitLogEntry(&preCommitLog[i], commit_version);
									preCommitLog[i].ready = false;
									commitLog[j].valid = true;
									inserted = true;
									break;
								}
							}
							cleanCommitedLog();
						}
					}else{
						DEBUG("Waiting for=" << nextToCommit);
					}
				}else{
					preCommitLog[i].ready = false;
					preCommitLog[i].tx->abort_flag = true;
				}
			}
		}
	}

	void started(Transaction *tx, int threadId){
		DEBUG("Started=" << tx->age << " tid=" << threadId);
		aliveTx[threadId] = tx->start_version;
	}

	void finished(Transaction *tx, int threadId){
		DEBUG("Finished=" << tx->age << " tid=" << threadId);
		while(preCommitLog[threadId].ready);
		DEBUG("Precommit=" << tx->age << " tid=" << threadId);
		preCommitLog[threadId].setPreCommitLogEntry(tx);
	}

	void committed(Transaction *tx, int threadId){
		DEBUG("Committed=" << tx->age << " tid=" << threadId);
		DEBUG(tx->age << " [" << tx->start_version << ":" << tx->commit_version << "]");
		aliveTx[threadId] = EMPTY;
	}
};

extern TCM *manager;

static void* run_tcm(void* arg){
	stick_worker((int)((long)arg + 1));
	while(true){
		manager->validatePreCommitted();
		manager->cleanCommitedLog();
	}
	return 0;
}

static void sys_init(int workers) {
	manager = new TCM(workers);
	pthread_t tcm_worker;
	pthread_create(&tcm_worker, NULL, run_tcm, (void*)(long)workers);
}

static void sys_shutdown(){}
}


#endif /* TCM_HPP_ */
