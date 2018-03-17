/*
 * TCM.hpp
 *
 *  Created on: Jul 21, 2015
 *      Author: root
 */

#ifndef TCM_UNORDERED_HPP_
#define TCM_UNORDERED_HPP_

#include <stdint.h>
#include <iostream>
#include <functional>
#include <string>

namespace stmlite_unordered{

static int operations = 0;

#define EMPTY	100000000

#define SIGNATURE_LENGTH 	8

static volatile int global_time = 0;
static volatile int writerThread = -1;
static int SIG_MASK = SIGNATURE_LENGTH * 32 - 1;

class Signature{
	uint32_t signature[SIGNATURE_LENGTH];
	bool empty;
public:
	Signature() : empty(true){
		for(int i=0; i<SIGNATURE_LENGTH; i++){
			signature[i] = 0;
		}
	}
	int simple_hash(long* addr){
		return (long)addr;
	}
	std::hash<void**> std_hash;
	void insert(void** addr){
		int index = std_hash(addr) & SIG_MASK;
		int byte_index = index >> 5;
		int bit_index = index & 0x1F;
		signature[byte_index] |= (1 << bit_index);
		empty = false;
	}
	bool exists(void** addr){
		if(empty) return false;
		int index = std_hash(addr) & SIG_MASK;
		int byte_index = index >> 5;
		int bit_index = index & 0x1F;
		return signature[byte_index] & (1 << bit_index);
	}
	bool intersec(Signature* s){
		operations++;
		for(int i=0; i<SIGNATURE_LENGTH; i++){
			if(s->signature[i] & signature[i])
				return true;
		}
		return false;
	}
};

class Transaction : public AbstractTransaction{
public:
	int tx_id;
	int start_version;
	int commit_version;
	volatile bool abort_flag;
	volatile bool commit_flag;
	Set<void*> writeset;
	Signature readSignature;
	Signature writeSignature;
	jmp_buf* buff;
	Transaction(int id) : tx_id(id), abort_flag(false), commit_flag(false) {};

	void start(jmp_buf* buff){
		LOG("Start " << tx_id);
		this->buff = buff;
		start_version = __sync_fetch_and_add(&global_time, 1);
	}

//	long rv;
	void* read(void** so) {
		readSignature.insert(so);
		if(writeSignature.exists(so)){
			bool found;
			void* v = writeset.find(so, found);
			if(!found)
				v = *so;
			return v;
		}
//		rv = v;
		return *so;
	}

	void write(void** so, void* value) {
		writeset.add(so, value);
		writeSignature.insert(so);
	}

	void commit(){
		ListNode<Entry<void**, void*>*>* itr = writeset.elements.header;
//		LOG(tx_id << " read:" << rv << " write:" << *itr->value->object << "->" << itr->value->data);
		while (itr != NULL) {
			*itr->value->object = itr->value->data;
			itr = itr->next;
		}
		commit_version = __sync_fetch_and_add(&global_time, 1);
	}

	void suicide(int reason){
		abort();
	}

	void abort() {
		LOG("Abort" << tx_id);
		longjmp (*buff, 1);
	}
};

class PreCommitLogEntry{
public:
	Transaction* tx;
	Signature* writeSignature;
	Signature* readSignature;
	bool ready;
	bool fresh;

	PreCommitLogEntry() : tx(NULL), writeSignature(NULL), readSignature(NULL), ready(false), fresh(true) {}

	void setPreCommitLogEntry(Transaction* transaction){
		ready = false;
		tx = transaction;
		readSignature = &tx->readSignature;
		writeSignature = &tx->writeSignature;
		ready = true;
	}
};

class CommitLogEntry{
public:
	int commit_version;
	Signature* writeSignature;
	bool valid;
	int validation;

	CommitLogEntry() : commit_version(-1), writeSignature(NULL), valid(false) {};

	void setPreCommitLogEntry(PreCommitLogEntry* entry){
		commit_version =  entry->tx->commit_version;
		writeSignature = entry->writeSignature;
		validation = 0;
	}
};

class TCM{
	int preCommitLogLength;
	PreCommitLogEntry* preCommitLog;
	int commitLogLength;
	CommitLogEntry* commitLog;
	int* aliveTx;
//	volatile int nextToCommit;

	bool conflictCheck(PreCommitLogEntry* preCommitEntry, int index){
		LOG("Check " << preCommitEntry->tx->tx_id);
		int mask = 1 << index;
		bool force = preCommitEntry->fresh;
		preCommitEntry->fresh = false;
		for(int i=0; i<commitLogLength; i++){
			if(commitLog[i].valid){
				if(!force && (mask & commitLog[i].validation))
					continue;
				commitLog[i].validation |= mask;
				LOG("Compare " << preCommitEntry->tx->tx_id << " " << commitLog[i].commit_version);
				if(preCommitEntry->tx->start_version < commitLog[i].commit_version){
					LOG("SigCompare");
					if(commitLog[i].writeSignature->intersec(preCommitEntry->readSignature)){
						LOG("\tFail");
						return false;
					}else{
						LOG("\tPass");
					}
				}else{
					LOG("Skip");
				}
			}
		}
		LOG("Okay " << preCommitEntry->tx->tx_id);
		return true;
	}

public:
	TCM(int w) {
		preCommitLogLength = w;
		commitLogLength = w*20;
		preCommitLog = new PreCommitLogEntry[w];
		aliveTx = new int[w];
		for(int i=0; i<w; i++)
			aliveTx[i] = EMPTY;
		commitLog = new CommitLogEntry[commitLogLength];
//		nextToCommit = 0;
	};

	void cleanCommitedLog(){
		int minSV = aliveTx[0];
		for(int i=1; i<preCommitLogLength; i++)
			if(aliveTx[i] < minSV)
				minSV = aliveTx[i];
		LOG("MinSV=" << minSV);
		for(int i=0; i<commitLogLength; i++){
			if(commitLog[i].valid && commitLog[i].commit_version < minSV){
				LOG("Clean " << commitLog[i].commit_version);
				commitLog[i].valid = false;
			}
		}
	}

	void validatePreCommitted(){
		for(int i=0; i<preCommitLogLength; i++){
			if(preCommitLog[i].ready){
//				if(nextToCommit == preCommitLog[i].tx->tx_id){
					if(conflictCheck(&preCommitLog[i], i)){
//						int temp = nextToCommit;
						preCommitLog[i].ready = false;
						preCommitLog[i].tx->commit_flag = true;
						// Wait for commit till finish
//						cleanCommitedLog();
//						while(nextToCommit == temp);
					}else{
						preCommitLog[i].ready = false;
						preCommitLog[i].tx->abort_flag = true;
					}
//				}
			}
		}
	}

	void started(Transaction *tx, int threadId){
		aliveTx[threadId] = tx->start_version;
	}

	void finished(Transaction *tx, int threadId){
		preCommitLog[threadId].setPreCommitLogEntry(tx);
	}

	void committed(Transaction *tx, int threadId){
		bool inserted = false;
		LOG(tx->tx_id << " [" << tx->start_version << ":" << tx->commit_version << "]");
		while(!inserted){
			for(int i=0; i<commitLogLength; i++){
				if(!commitLog[i].valid && __sync_bool_compare_and_swap(&commitLog[i].valid, false, true)){
					commitLog[i].setPreCommitLogEntry(&preCommitLog[threadId]);
					inserted = true;
					break;
				}
			}
		}
		aliveTx[threadId] = EMPTY;
//		nextToCommit = tx->tx_id + 1;
	}
};

}

#endif /* TCM_HPP_ */
