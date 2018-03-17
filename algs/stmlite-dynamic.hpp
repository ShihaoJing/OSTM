/*
 * TCM.hpp
 *
 *  Created on: Jul 21, 2015
 *      Author: root
 */

#ifndef TCM_HPP_
#define TCM_HPP_

#include <stdint.h>
#include <iostream>
#include <functional>
#include <string>

#include "commons.hpp"

namespace stmlite{

int operations = 0;

#define EMPTY	100000000

#define SIGNATURE_LENGTH 	8

volatile int global_time = 0;
volatile int writerThread = -1;
int SIG_MASK = SIGNATURE_LENGTH * 32 - 1;

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
	std::hash<long*> std_hash;
	void insert(long* addr){
		int index = std_hash(addr) & SIG_MASK;
		int byte_index = index >> 5;
		int bit_index = index & 0x1F;
		signature[byte_index] |= (1 << bit_index);
		empty = false;
	}
	bool exists(long* addr){
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

class Transaction : public commons::AbstractTransaction{
public:
	int tx_id;
	int start_version;
	volatile int commit_version;
	Set<long> writeset;
	Signature readSignature;
	Signature writeSignature;
	volatile bool abort_flag;
	volatile bool commit_flag;
	jmp_buf* buff;
	Transaction(int id) : tx_id(id), abort_flag(false), commit_flag(false), commit_version(-1) {};

	void start(jmp_buf* buff){
		LOG("Start " << tx_id);
		this->buff = buff;
		start_version = __sync_fetch_and_add(&global_time, 1);
	}

//	long rv;
	long read(long* so) {
		readSignature.insert(so);
		if(writeSignature.exists(so)){
			long v = writeset.find(so);
			if((long)v < 0)	// not found in write set (FIXME find better handling for not-found)
				v = *so;
			return v;
		}
//		rv = v;
		return *so;
	}

	void write(long* so, long value) {
		writeset.add(so, value);
		writeSignature.insert(so);
	}

	void commit(){
		ListNode<Entry<long*, long>*>* itr = writeset.elements.header;
//		LOG(tx_id << " read:" << rv << " write:" << *itr->value->object << "->" << itr->value->data);
		while (itr != NULL) {
			*itr->value->object = itr->value->data;
			itr = itr->next;
		}
		commit_version = __sync_fetch_and_add(&global_time, 1);
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
	volatile bool ready;
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
	Transaction* tx;

	CommitLogEntry() : commit_version(-1), writeSignature(NULL), valid(false) {};

	void setPreCommitLogEntry(PreCommitLogEntry* entry){
		while(entry->tx->commit_version < 0)
			entry->tx->commit_flag = true;
		tx =  entry->tx;
		commit_version = entry->tx->commit_version;
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
	int nextToCommit;

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
		commitLogLength = w;
		preCommitLog = new PreCommitLogEntry[w];
		aliveTx = new int[w];
		for(int i=0; i<w; i++)
			aliveTx[i] = EMPTY;
		commitLog = new CommitLogEntry[commitLogLength];
		nextToCommit = 0;
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
				if(nextToCommit == preCommitLog[i].tx->tx_id){
					if(conflictCheck(&preCommitLog[i], i)){
						nextToCommit++;
						preCommitLog[i].tx->commit_flag = true;
						bool inserted = false;
						while(!inserted){
							for(int j=0; j<commitLogLength; j++){
								if(!commitLog[j].valid){
									commitLog[j].setPreCommitLogEntry(&preCommitLog[i]);
									preCommitLog[i].ready = false;
									commitLog[j].valid = true;
									inserted = true;
									break;
								}
							}
							cleanCommitedLog();
						}
					}else{
						preCommitLog[i].ready = false;
						preCommitLog[i].tx->abort_flag = true;
					}
				}
			}
		}
	}

	void started(Transaction *tx, int threadId){
		aliveTx[threadId] = tx->start_version;
	}

	void finished(Transaction *tx, int threadId){
		while(preCommitLog[threadId].ready);
		preCommitLog[threadId].setPreCommitLogEntry(tx);
	}

	void committed(Transaction *tx, int threadId){
		LOG(tx->tx_id << " [" << tx->start_version << ":" << tx->commit_version << "]");
		aliveTx[threadId] = EMPTY;
	}
};

}

#endif /* TCM_HPP_ */
