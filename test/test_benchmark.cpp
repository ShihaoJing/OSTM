#include <time.h>
#include <stdlib.h>
#include <setjmp.h>
#include <errno.h>

#include "../conf.h"

#include <iostream>
using namespace std;

#include "../algs/commons.hpp"

#include "../algs/dummy.hpp"
#include "../algs/owb.hpp"
#include "../algs/owbrh.hpp"
#include "../algs/tl2.hpp"
#include "../algs/norec.hpp"
#include "../algs/undolog_invis.hpp"
#include "../algs/undolog_vis.hpp"
#include "../algs/oul_speculative.hpp"
#include "../algs/oul_steal.hpp"
#include "../algs/stmlite.hpp"
#include "../algs/stmlite-unordered.hpp"

#include "../bench/microbench.hpp"

#include "../conf.h"

//#define CALC_AGE	(int) (tid*TX_PER_THREAD)+i
#define CALC_AGE	(int) (i*workers)+tid

//#define DEFAULT_WORKER_LOAD 		5000
#define DEFAULT_WORKER_LOAD 		1
#define JOBS_SHARED_QUEUE_LENGTH	1024
#define MAX_PENDING_PER_THREAD		2

#define MAX_ABORT_RATIO		30		// ++Algorithms [ratio of commits:aborts is 1:30]

static int workload;
static int workers;
static int cleaners;


// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Dummy ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ //
namespace dummy_runner{

void* run(void* threadid){
	INIT_THREAD_LOG
	long tid = (long) threadid;
	stick_worker((int) tid);
	INFO("Thread " << tid);
	long retries = 0;
	int worker_load = workload / workers;
	for(int i=0; i<worker_load; i++){
		int age = CALC_AGE;
		BENCH::benchmark_start(age, tid);
		jmp_buf _jmpbuf;
		int abort_flags = setjmp(_jmpbuf);
		retries++;
		dummy::Transaction* tx = new dummy::Transaction();
		BENCH::benchmark(tx, age, tid);
	}
	DUMP_LOG
	pthread_exit((void*)retries);
}

}

// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ OWB-v1 ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ //
namespace owb_v1_runner{

static volatile int last_committed = 0;

long restartTx(int age, long tid){
	long retries = 0;
	BENCH::benchmark_start(age, tid);
	jmp_buf _jmpbuf;
	int abort_flags = setjmp(_jmpbuf);
	retries++;
	owb::Transaction* tx = new owb::Transaction(age);
	LOG("restart " << age);
	tx->start(&_jmpbuf);
	BENCH::benchmark(tx, age, tid);
	tx->commit();
	if(tx->complete())
		tx->clean();
	LOG("restarted " << age);
	return retries;
}

void* run(void* threadid){
	INIT_THREAD_LOG
	long tid = (long) threadid;
	stick_worker((int) tid);
	UnsafeQueue<owb::Transaction*> pending;
	INFO("Thread " << tid);
	long retries = 0;
	int min_pending = -1;
	int worker_load = workload/workers;
	for(int i=0; i<worker_load; i++){
		int age = CALC_AGE;
		BENCH::benchmark_start(age, tid);
		jmp_buf _jmpbuf;
		int abort_flags = setjmp(_jmpbuf);
		retries++;
		LOG("exec " << age);
		owb::Transaction* tx = new owb::Transaction(age);
		tx->start(&_jmpbuf);
		do{
			owb::Transaction* ptx = pending.peek();
			if(ptx!=NULL && last_committed == ptx->getAge()){
				LOG("complete [" << ptx->getAge() << "] last completed " << last_committed);
				if(!ptx->complete())
					retries += restartTx(ptx->getAge(), tid);
				ptx->clean();
				last_committed++;
				pending.remove();
				continue;
			}
			break;	// nothing pending
		}while(true);
		BENCH::benchmark(tx, age, tid);
		tx->commit();
		if(last_committed == age){
			LOG("complete " << age << " last completed " << last_committed);
			if(!tx->complete())
				retries += restartTx(tx->getAge(), tid);
			tx->clean();
			last_committed++;
		}else{
			LOG("enqueue " << age << " [pending: " << (pending.peek()==NULL ? -1 : pending.peek()->getAge() ) << "] last committed " << last_committed);
			tx->noRetry();
			pending.enqueue(tx);
		}
	}
	do{
		owb::Transaction* ptx = pending.peek();
		if(ptx==NULL) break;
		if(last_committed == ptx->getAge()){
			LOG("complete [" << ptx->getAge() << "] last committed " << last_committed);
			if(!ptx->complete())
				retries += restartTx(ptx->getAge(), tid);
			ptx->clean();
			last_committed++;
			pending.remove();
		}
	}while(true);
	DUMP_LOG
	pthread_exit((void*)retries);
}

}

// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~ OWB-v2 ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ //
namespace owb_v2_runner{

owb::Transaction** committedTxs;

void init(){
	owb::last_completed = -1;
	committedTxs = new owb::Transaction*[workload];
	for(int i=0; i<workload; i++)
		committedTxs[i] = NULL;
}

void* run(void* threadid){
	INIT_THREAD_LOG
	long tid = (long) threadid;
	stick_worker((int) tid);
	long retries = 0;
	if(tid == workers){
		INFO("Monitor Thread " << tid);
		int last = workload - 1;
		bool check = true;
		long long startTime = getElapsedTime();
		long long endTime;
		while(owb::last_completed < last){
			int temp = owb::last_completed;
			int missing = -1;
			for(int i=owb::last_completed+1; i<= last; i++){
				if(committedTxs[i] == NULL){
					missing = i;
					break;
				}
				LOG("+++++++++ " << i << " +++++++++ ");
				if(committedTxs[i]->complete()){
					if(cleaners == 0)
						committedTxs[i]->clean();
				}else
					retries += owb_v1_runner::restartTx(committedTxs[i]->getAge(), tid);
				owb::last_completed++;
				if(check){
					bool finished = true;
					for(int w=0; w<workers; w++){
						if(committedTxs[last-w] == NULL){
							finished = false;
							break;
						}
					}
					if(finished){
						endTime = getElapsedTime();
						check = false;
						INFO("((" << owb::last_completed << "/" << last << "))");
					}
				}
			}

			int nulls = 0;
			int last_committed = owb::last_completed;
			for(int i=owb::last_completed+1; i<= last && nulls<workers; i++){
				if(committedTxs[i] == NULL)
					nulls++;
				else{
					last_committed = i;
					nulls = 0;
				}
			}
			INFO("<<" << temp << ":" << owb::last_completed << "/" << last_committed << ">> missing:" << missing);
		}
		long long end2Time = getElapsedTime();
		INFO("Monitor (" << (end2Time - endTime) << "/" << (end2Time - startTime) << "))");
	}else if(tid > workers){
		INFO("Cleaner Thread " << tid);
		int last_cleaned = tid - workers - 1 - cleaners;
		int last = workload - (cleaners - (tid - workers - 1));
		LOG(tid << " [" << last_cleaned + cleaners << ":" << last << "]");
		long long startTime = getElapsedTime();
		while(last_cleaned < last){
			for(int i=last_cleaned + cleaners; i<=owb::last_completed; i+=cleaners){
				if(committedTxs[i] == NULL)
					break;
				LOG("--------- " << i << " --------- ");
				committedTxs[i]->clean();
				last_cleaned = i;
			}
		}
		long long endTime = getElapsedTime();
		INFO("Cleaner ((" << (endTime - startTime) << "))");
	}else{
		INFO("Thread " << tid);
		int worker_load = workload / workers;
		for(int i=0; i<worker_load; i++){
			int age = CALC_AGE;
			BENCH::benchmark_start(age, tid);
			jmp_buf _jmpbuf;
			int abort_flags = setjmp(_jmpbuf);
			retries++;
			LOG("exec " << age);
			owb::Transaction* tx = new owb::Transaction(age);
			tx->start(&_jmpbuf);
			BENCH::benchmark(tx, age, tid);
			tx->commit();
			tx->noRetry();
			committedTxs[age] = tx;
		}
	}
	DUMP_LOG
	pthread_exit((void*)retries);
}
}

// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~ OWB-v3 ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ //
namespace owb_v3_runner{

int* jobs;
owb::Transaction** committedTxs;

void init(){
	owb::last_completed = -1;
	jobs = new int[workload];
	committedTxs = new owb::Transaction*[workload];
	for(int i=0; i<workload; i++){
		committedTxs[i] = NULL;
		jobs[i] = 1;
	}
}

void* run(void* threadid){
	INIT_THREAD_LOG
	long tid = (long) threadid;
	stick_worker((int) tid);
	long retries = 0;
	if(tid == workers){
		INFO("Monitor Thread " << tid);
		int last = workload - 1;
		bool check = true;
		long long startTime = getElapsedTime();
		long long endTime;
		while(owb::last_completed < last){
			int temp = owb::last_completed;
			int missing = -1;
			for(int i=owb::last_completed+1; i<= last; i++){
				if(committedTxs[i] == NULL){
					missing = i;
					break;
				}
				LOG("+++++++++ " << i << " +++++++++ ");
				if(committedTxs[i]->complete()){
					if(cleaners == 0)
						committedTxs[i]->clean();
				}else
					retries += owb_v1_runner::restartTx(committedTxs[i]->getAge(), tid);
				owb::last_completed++;
				if(check){
					bool finished = true;
					for(int w=0; w<workers; w++){
						if(committedTxs[last-w] == NULL){
							finished = false;
							break;
						}
					}
					if(finished){
						endTime = getElapsedTime();
						check = false;
						INFO("((" << owb::last_completed << "/" << last << "))");
					}
				}
			}

			int nulls = 0;
			int last_committed = owb::last_completed;
			for(int i=owb::last_completed+1; i<= last && nulls<workers; i++){
				if(committedTxs[i] == NULL)
					nulls++;
				else{
					last_committed = i;
					nulls = 0;
				}
			}
			LOG("<<" << temp << ":" << last_committed << ">> missing:" << missing);
		}
		long long end2Time = getElapsedTime();
		INFO("Monitor (" << (end2Time - endTime) << "/" << (end2Time - startTime) << "))");
	}else if(tid > workers){
		INFO("Cleaner Thread " << tid);
		int last_cleaned = tid - workers - 1 - cleaners;
		int last = workload - (cleaners - (tid - workers - 1));
		LOG(tid << " [" << last_cleaned + cleaners << ":" << last << "]");
		long long startTime = getElapsedTime();
		while(last_cleaned < last){
			for(int i=last_cleaned + cleaners; i<=owb::last_completed; i+=cleaners){
				if(committedTxs[i] == NULL)
					break;
				LOG("--------- " << i << " --------- ");
				committedTxs[i]->clean();
				last_cleaned = i;
			}
		}
		long long endTime = getElapsedTime();
		INFO("Cleaner ((" << (endTime - startTime) << "))");
	}else{
		INFO("Thread " << tid);
		for(int i=0; i<workload; i++){
			if(!__sync_bool_compare_and_swap(&(jobs[i]), 1, 0))	// acquired by another work
				continue;
			int age = i;
			BENCH::benchmark_start(age, tid);
			jmp_buf _jmpbuf;
			int abort_flags = setjmp(_jmpbuf);
			retries++;
			LOG("exec " << age);
			owb::Transaction* tx = new owb::Transaction(age);
			tx->start(&_jmpbuf);
			BENCH::benchmark(tx, age, tid);
			tx->commit();
			tx->noRetry();
			committedTxs[age] = tx;
		}
	}
	DUMP_LOG
	pthread_exit((void*)retries);
}
}

// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~ OWB-v4 ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ //
namespace owb_v4_runner{

int* jobs;
owb::Transaction** committedTxs;
//static volatile int last_validator;
int max_pending;

long restartTx(int age, long tid){
	long retries = 0;
	BENCH::benchmark_start(age, tid);
	jmp_buf _jmpbuf;
	int abort_flags = setjmp(_jmpbuf);
	retries++;
	owb::Transaction* tx = new owb::Transaction(age);
	LOG("restart " << age);
	tx->start(&_jmpbuf);
	BENCH::benchmark(tx, age, tid);
	if(tx->commit(true))
		if(cleaners == 0)
			tx->clean();
	LOG("restarted " << age);
	return retries;
}

void init(){
	owb::last_completed = -1;
//	last_validator = -1;
	owb::validation = 0;
	max_pending = workers * MAX_PENDING_PER_THREAD;
	jobs = new int[workload];
	committedTxs = new owb::Transaction*[workload];
	for(int i=0; i<workload; i++){
		committedTxs[i] = NULL;
		jobs[i] = 1;
	}
}

//long long validation_time = 0;

int try_validation(int tid){
	if(
//		last_validator!=tid &&
		owb::validation == 0 &&
		__sync_bool_compare_and_swap(&owb::validation, 0, 1)
	)
	{ // try to be the validator
//		long long startTime = getElapsedTime();
		int temp = owb::last_completed;
		int retries = 0;
		for(int i=owb::last_completed+1; i< workload; i++){
			if(committedTxs[i] == NULL){
				break;
			}
			LOG("+++++++++ " << i << " +++++++++ ");
			if(committedTxs[i]->complete()){
				if(cleaners == 0)
					committedTxs[i]->clean();
			}else
				retries += restartTx(committedTxs[i]->getAge(), tid);
			owb::last_completed++;
		}
	//	INFO("<" << temp << ":" << last_completed << ">");
//		last_validator = tid;
		owb::validation = 0;	// reset the flag
//		long long endTime = getElapsedTime();
//		validation_time += endTime - startTime;
		return retries;
	}
	return 0;
}

void* run(void* threadid){
	INIT_THREAD_LOG
	long tid = (long) threadid;
	stick_worker((int) tid);
	long retries = 0;
//	long long work_time = 0;
	for(int i=0; i<workload; i++){
		retries += try_validation(tid);
		if(jobs[i]==0 || !__sync_bool_compare_and_swap(&(jobs[i]), 1, 0))	// acquired by another work
			continue;
		while(i - owb::last_completed > max_pending);
		LOG(i << " start");
//		long long startTime = getElapsedTime();
		int age = i;
		BENCH::benchmark_start(age, tid);
		jmp_buf _jmpbuf;
		int abort_flags = setjmp(_jmpbuf);
		retries++;
		LOG("exec " << age);
		owb::Transaction* tx = new owb::Transaction(age);
		tx->start(&_jmpbuf);
		BENCH::benchmark(tx, age, tid);
		tx->commit();
		tx->noRetry();
		committedTxs[age] = tx;
		LOG(i << " end");
//		long long endTime = getElapsedTime();
//		work_time += endTime - startTime;
	}
	retries += try_validation(tid);//+1000);
//	INFO("Thread " << tid << " work=" << work_time);
	DUMP_LOG
	pthread_exit((void*)retries);
}
}

// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~ OWB-v5 ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ //
namespace owb_v5_runner{

int jobs[JOBS_SHARED_QUEUE_LENGTH];
int pending_queue_length;
int pending_mask;
owb::Transaction** committedTxs;
int epochs;
int max_pending;


long restartTx(int age, long tid){
	long retries = 0;
	BENCH::benchmark_start(age, tid);
	jmp_buf _jmpbuf;
	int abort_flags = setjmp(_jmpbuf);
	retries++;
	owb::Transaction* tx = new owb::Transaction(age);
	LOG("restart " << age);
	tx->start(&_jmpbuf);
	BENCH::benchmark(tx, age, tid);
	if(tx->commit(true))
		if(cleaners == 0)
			tx->clean();
	LOG("restarted " << age);
	return retries;
}

void init(){
	owb::last_completed = -1;
	owb::validation = 0;
	epochs = (workload + JOBS_SHARED_QUEUE_LENGTH - 1)/JOBS_SHARED_QUEUE_LENGTH;
	max_pending = workers * MAX_PENDING_PER_THREAD;

	for(int i=0; i<JOBS_SHARED_QUEUE_LENGTH; i++){
		jobs[i] = 0;
	}
	pending_queue_length = 1024;
	while(pending_queue_length < max_pending)
		pending_queue_length *= 2;
	committedTxs = new owb::Transaction*[pending_queue_length];
	pending_mask = pending_queue_length - 1;
	for(int i=0; i<pending_queue_length; i++){
		committedTxs[i] = NULL;
	}
}

//long long validation_time = 0;

int try_validation(int tid){
	if(
		owb::validation == 0 &&
		__sync_bool_compare_and_swap(&owb::validation, 0, 1)
	)
	{ // try to be the validator
//		long long startTime = getElapsedTime();
		int temp = owb::last_completed;
		int retries = 0;
		for(int j=owb::last_completed+1; j< workload; j++){
			int i = j & pending_mask;
			if(committedTxs[i] == NULL){
				break;
			}
			LOG("+++++++++ " << i << " +++++++++ ");
			if(committedTxs[i]->complete()){
				if(cleaners == 0)
					committedTxs[i]->clean();
			}else
				retries += restartTx(committedTxs[i]->getAge(), tid);
			committedTxs[i] = NULL;
			owb::last_completed++;
		}
	//	INFO("<" << temp << ":" << last_completed << ">");
		owb::validation = 0;	// reset the flag
//		long long endTime = getElapsedTime();
//		validation_time += endTime - startTime;
		return retries;
	}
	return 0;
}

void* run(void* threadid){
	INIT_THREAD_LOG
	long tid = (long) threadid;
	stick_worker((int) tid);
	long retries = 0;
//	long long work_time = 0;
	INFO("Thread " << tid);
	int jobId = -1;
	for(int j=0; j<epochs; j++){
		int marked = j+1;
		for(int i=0; i<JOBS_SHARED_QUEUE_LENGTH; i++){
			jobId++;
			retries += try_validation(tid);
			if(jobs[i]==marked || !__sync_bool_compare_and_swap(&(jobs[i]), j, marked))	// acquired by another work
				continue;
			if(jobId >= workload) break;
			while(jobId - owb::last_completed > max_pending);
			LOG(i << " start");
	//		long long startTime = getElapsedTime();
			int age = jobId;
			BENCH::benchmark_start(age, tid);
			jmp_buf _jmpbuf;
			int abort_flags = setjmp(_jmpbuf);
			retries++;
			LOG("exec " << age);
			owb::Transaction* tx = new owb::Transaction(age);
			tx->start(&_jmpbuf);
			BENCH::benchmark(tx, age, tid);
			tx->commit();
			tx->noRetry();
			committedTxs[age & pending_mask] = tx;
			LOG(i << " end");
	//		long long endTime = getElapsedTime();
	//		work_time += endTime - startTime;
		}
	}
	retries += try_validation(tid);
//	INFO("Thread " << tid << " work=" << work_time);
	DUMP_LOG
	pthread_exit((void*)retries);
}
}

// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~ OWB-v5+ ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ //
namespace owb_v5_plus_runner{

int jobs[JOBS_SHARED_QUEUE_LENGTH];
int pending_queue_length;
int pending_mask;
owb::Transaction** committedTxs;
int epochs;
int max_pending;
int min_pending;
int hold_on;

long restartTx(int age, long tid){
	long retries = 0;
	BENCH::benchmark_start(age, tid);
	jmp_buf _jmpbuf;
	int abort_flags = setjmp(_jmpbuf);
	retries++;
	owb::Transaction* tx = new owb::Transaction(age);
	LOG("restart " << age);
	tx->start(&_jmpbuf);
	BENCH::benchmark(tx, age, tid);
	if(tx->commit(true))
		if(cleaners == 0)
			tx->clean();
	LOG("restarted " << age);
	return retries;
}

void init(){
	owb::last_completed = -1;
	owb::validation = 0;
	epochs = (workload + JOBS_SHARED_QUEUE_LENGTH - 1)/JOBS_SHARED_QUEUE_LENGTH;
	max_pending = workers * MAX_PENDING_PER_THREAD;
	min_pending = workers;
	hold_on = 0;
	for(int i=0; i<JOBS_SHARED_QUEUE_LENGTH; i++){
		jobs[i] = 0;
	}
	pending_queue_length = 1024;
	while(pending_queue_length < max_pending)
		pending_queue_length *= 2;
	committedTxs = new owb::Transaction*[pending_queue_length];
	pending_mask = pending_queue_length - 1;
	for(int i=0; i<pending_queue_length; i++){
		committedTxs[i] = NULL;
	}
}

//long long validation_time = 0;

int try_validation(int tid){
	if(
		owb::validation == 0 &&
		__sync_bool_compare_and_swap(&owb::validation, 0, 1)
	)
	{ // try to be the validator
//		long long startTime = getElapsedTime();
		int temp = owb::last_completed;
		int retries = 0;
		for(int j=owb::last_completed+1; j< workload; j++){
			int i = j & pending_mask;
			if(committedTxs[i] == NULL){
				break;
			}
			LOG("+++++++++ " << i << " +++++++++ ");
			if(committedTxs[i]->complete()){
				if(cleaners == 0)
					committedTxs[i]->clean();
			}else
				retries += restartTx(committedTxs[i]->getAge(), tid);
			committedTxs[i] = NULL;
			owb::last_completed++;
		}
	//	INFO("<" << temp << ":" << last_completed << ">");
		owb::validation = 0;	// reset the flag
//		long long endTime = getElapsedTime();
//		validation_time += endTime - startTime;
		return retries;
	}
	return 0;
}

void* run(void* threadid){
	INIT_THREAD_LOG
	long tid = (long) threadid;
	stick_worker((int) tid);
	long retries = 0;
//	long long work_time = 0;
	INFO("Thread " << tid);
	int jobId = -1;
	for(int j=0; j<epochs; j++){
		int marked = j+1;
		for(int i=0; i<JOBS_SHARED_QUEUE_LENGTH; i++){
			jobId++;
			retries += try_validation(tid);
			if(jobs[i]==marked || !__sync_bool_compare_and_swap(&(jobs[i]), j, marked))	// acquired by another work
				continue;
			if(jobId >= workload) break;
			if(jobId - owb::last_completed > max_pending){
				hold_on++;
				while(jobId - owb::last_completed > min_pending);	// freeze execution
			}
			LOG(i << " start");
	//		long long startTime = getElapsedTime();
			int age = jobId;
			BENCH::benchmark_start(age, tid);
			jmp_buf _jmpbuf;
			int abort_flags = setjmp(_jmpbuf);
			retries++;
			LOG("exec " << age);
			owb::Transaction* tx = new owb::Transaction(age);
			tx->start(&_jmpbuf);
			BENCH::benchmark(tx, age, tid);
			tx->commit();
			tx->noRetry();
			committedTxs[age & pending_mask] = tx;
			LOG(i << " end");
	//		long long endTime = getElapsedTime();
	//		work_time += endTime - startTime;
		}
	}
	retries += try_validation(tid);
//	INFO("Thread " << tid << " work=" << work_time);
	DUMP_LOG
	pthread_exit((void*)retries);
}
}

// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~ OWB-v5++ ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ //
namespace owb_v5_plus_plus_runner{

int jobs[JOBS_SHARED_QUEUE_LENGTH];
int pending_queue_length;
int pending_mask;
owb::Transaction** committedTxs;
int epochs;
int min_pending;
int hold_on;
long aborts;

long restartTx(int age, long tid){
	long retries = 0;
	BENCH::benchmark_start(age, tid);
	jmp_buf _jmpbuf;
	int abort_flags = setjmp(_jmpbuf);
	retries++;
	owb::Transaction* tx = new owb::Transaction(age);
	LOG("restart " << age);
	tx->start(&_jmpbuf);
	BENCH::benchmark(tx, age, tid);
	if(tx->commit(true))
		if(cleaners == 0)
			tx->clean();
	LOG("restarted " << age);
	return retries;
}

void init(){
	owb::last_completed = -1;
	owb::validation = 0;
	epochs = (workload + JOBS_SHARED_QUEUE_LENGTH - 1)/JOBS_SHARED_QUEUE_LENGTH;
	min_pending = workers;
	aborts = 0;
	hold_on = 0;
	for(int i=0; i<JOBS_SHARED_QUEUE_LENGTH; i++){
		jobs[i] = 0;
	}
	pending_queue_length = 1024;
//	while(pending_queue_length < workers * MAX_PENDING_PER_THREAD)
//		pending_queue_length *= 2;
	committedTxs = new owb::Transaction*[pending_queue_length];
	pending_mask = pending_queue_length - 1;
	for(int i=0; i<pending_queue_length; i++){
		committedTxs[i] = NULL;
	}
}

//long long validation_time = 0;

int try_validation(int tid){
	if(
		owb::validation == 0 &&
		__sync_bool_compare_and_swap(&owb::validation, 0, 1)
	)
	{ // try to be the validator
//		long long startTime = getElapsedTime();
		int temp = owb::last_completed;
		int retries = 0;
		for(int j=owb::last_completed+1; j< workload; j++){
			int i = j & pending_mask;
			if(committedTxs[i] == NULL){
				break;
			}
			LOG("+++++++++ " << i << " +++++++++ ");
			if(committedTxs[i]->complete()){
				if(cleaners == 0)
					committedTxs[i]->clean();
			}else{
				long r = restartTx(committedTxs[i]->getAge(), tid);
				retries += r;
				aborts += r - 1;
			}
			committedTxs[i] = NULL;
			owb::last_completed++;
		}
	//	INFO("<" << temp << ":" << last_completed << ">");
		owb::validation = 0;	// reset the flag
//		long long endTime = getElapsedTime();
//		validation_time += endTime - startTime;
		return retries;
	}
	return 0;
}

void* run(void* threadid){
	INIT_THREAD_LOG
	long tid = (long) threadid;
	stick_worker((int) tid);
	long retries = 0;
//	long long work_time = 0;
	INFO("Thread " << tid);
	int jobId = -1;
	for(int j=0; j<epochs; j++){
		int marked = j+1;
		for(int i=0; i<JOBS_SHARED_QUEUE_LENGTH; i++){
			jobId++;
			retries += try_validation(tid);
			if(jobs[i]==marked || !__sync_bool_compare_and_swap(&(jobs[i]), j, marked))	// acquired by another work
				continue;
			if(jobId >= workload) break;
            if(aborts * MAX_ABORT_RATIO > jobId || jobId - owb::last_completed > pending_queue_length){
//         		INFO("Hold " << aborts << " " << jobId);
                hold_on++;
                while(jobId - owb::last_completed > min_pending) try_validation(tid); // freeze execution
            }
			LOG(i << " start");
	//		long long startTime = getElapsedTime();
			int age = jobId;
			long temp = retries;
			BENCH::benchmark_start(age, tid);
			jmp_buf _jmpbuf;
			int abort_flags = setjmp(_jmpbuf);
			retries++;
			LOG("exec " << age);
			owb::Transaction* tx = new owb::Transaction(age);
			tx->start(&_jmpbuf);
			BENCH::benchmark(tx, age, tid);
			tx->commit();
			tx->noRetry();
			committedTxs[age & pending_mask] = tx;
			aborts += retries - temp - 1;
			LOG(i << " end");
	//		long long endTime = getElapsedTime();
	//		work_time += endTime - startTime;
		}
	}
	retries += try_validation(tid);
//	INFO("Thread " << tid << " work=" << work_time);
	DUMP_LOG
	pthread_exit((void*)retries);
}
}


// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~ OWB-RH++ ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ //
namespace owbrh_plus_plus_runner{

int jobs[JOBS_SHARED_QUEUE_LENGTH];
int pending_queue_length;
int pending_mask;
owbrh::Transaction** committedTxs;
int epochs;
int min_pending;
int hold_on;
long aborts;

long restartTx(int age, long tid){
	long retries = 0;
	BENCH::benchmark_start(age, tid);
	jmp_buf _jmpbuf;
	int abort_flags = setjmp(_jmpbuf);
	retries++;
	owbrh::Transaction* tx = new owbrh::Transaction(age);
	LOG("restart " << age);
	tx->start(&_jmpbuf);
	BENCH::benchmark(tx, age, tid);
	if(tx->commit(true))
		if(cleaners == 0)
			tx->clean();
	LOG("restarted " << age);
	return retries;
}

void init(){
	owbrh::last_completed = -1;
	owbrh::validation = 0;
	epochs = (workload + JOBS_SHARED_QUEUE_LENGTH - 1)/JOBS_SHARED_QUEUE_LENGTH;
	min_pending = workers;
	aborts = 0;
	hold_on = 0;
	for(int i=0; i<JOBS_SHARED_QUEUE_LENGTH; i++){
		jobs[i] = 0;
	}
	pending_queue_length = 1024;
//	while(pending_queue_length < workers * MAX_PENDING_PER_THREAD)
//		pending_queue_length *= 2;
	committedTxs = new owbrh::Transaction*[pending_queue_length];
	pending_mask = pending_queue_length - 1;
	for(int i=0; i<pending_queue_length; i++){
		committedTxs[i] = NULL;
	}
}

//long long validation_time = 0;

int try_validation(int tid){
	if(
		owbrh::validation == 0 &&
		__sync_bool_compare_and_swap(&owbrh::validation, 0, 1)
	)
	{ // try to be the validator
//		long long startTime = getElapsedTime();
		int temp = owbrh::last_completed;
		int retries = 0;
		for(int j=owbrh::last_completed+1; j< workload; j++){
			int i = j & pending_mask;
			if(committedTxs[i] == NULL){
				break;
			}
			LOG("+++++++++ " << i << " +++++++++ ");
			if(committedTxs[i]->complete()){
				if(cleaners == 0)
					committedTxs[i]->clean();
			}else{
				long r = restartTx(committedTxs[i]->getAge(), tid);
				retries += r;
				aborts += r - 1;
			}
			committedTxs[i] = NULL;
			owbrh::last_completed++;
		}
	//	INFO("<" << temp << ":" << last_completed << ">");
		owbrh::validation = 0;	// reset the flag
//		long long endTime = getElapsedTime();
//		validation_time += endTime - startTime;
		return retries;
	}
	return 0;
}

void* run(void* threadid){
	INIT_THREAD_LOG
	long tid = (long) threadid;
	stick_worker((int) tid);
	long retries = 0;
//	long long work_time = 0;
	INFO("Thread " << tid);
	int jobId = -1;
	for(int j=0; j<epochs; j++){
		int marked = j+1;
		for(int i=0; i<JOBS_SHARED_QUEUE_LENGTH; i++){
			jobId++;
			retries += try_validation(tid);
			if(jobs[i]==marked || !__sync_bool_compare_and_swap(&(jobs[i]), j, marked))	// acquired by another work
				continue;
			if(jobId >= workload) break;
            if(aborts * MAX_ABORT_RATIO > jobId || jobId - owbrh::last_completed > pending_queue_length){
//         		INFO("Hold " << aborts << " " << jobId);
                hold_on++;
                while(jobId - owbrh::last_completed > min_pending) try_validation(tid); // freeze execution
            }
			LOG(i << " start");
	//		long long startTime = getElapsedTime();
			int age = jobId;
			long temp = retries;
			BENCH::benchmark_start(age, tid);
			jmp_buf _jmpbuf;
			int abort_flags = setjmp(_jmpbuf);
			retries++;
			LOG("exec " << age);
			owbrh::Transaction* tx = new owbrh::Transaction(age);
			tx->start(&_jmpbuf);
			BENCH::benchmark(tx, age, tid);
			tx->commit();
			tx->noRetry();
			committedTxs[age & pending_mask] = tx;
			aborts += retries - temp - 1;
			LOG(i << " end");
	//		long long endTime = getElapsedTime();
	//		work_time += endTime - startTime;
		}
	}
	retries += try_validation(tid);
//	INFO("Thread " << tid << " work=" << work_time);
	DUMP_LOG
	pthread_exit((void*)retries);
}
}

// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ TL2 ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ //
namespace tl2_runner{

void* run(void* threadid){
	INIT_THREAD_LOG
	long tid = (long) threadid;
	stick_worker((int) tid);
	INFO("Thread " << tid);
	long retries = 0;
	int worker_load = workload / workers;
	for(int i=0; i<worker_load; i++){
		int age = CALC_AGE;
		BENCH::benchmark_start(age, tid);
		jmp_buf _jmpbuf;
		int abort_flags = setjmp(_jmpbuf);
		retries++;
		tl2::Transaction* tx = new tl2::Transaction();
		tx->start(&_jmpbuf);
		BENCH::benchmark(tx, age, tid);
		tx->commit();
	}
	DUMP_LOG
	pthread_exit((void*)retries);
}

}

// ~~~~~~~~~~~~~~~~~~~~~~~~~~~ Ordered TL2 ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ //
namespace otl2_v1_runner{

static volatile int last_committed;

void init(){
	last_committed = 0;
}

void* run(void* threadid){
	INIT_THREAD_LOG
	long tid = (long) threadid;
	stick_worker((int) tid);
	INFO("Thread " << tid);
	long retries = 0;
	int worker_load = workload / workers;
	for(int i=0; i<worker_load; i++){
		int age = CALC_AGE;
		BENCH::benchmark_start(age, tid);
		jmp_buf _jmpbuf;
		int abort_flags = setjmp(_jmpbuf);
		retries++;
		tl2::Transaction* tx = new tl2::Transaction();
		tx->start(&_jmpbuf);
		BENCH::benchmark(tx, age, tid);
		while(last_committed != age);
		tx->commit();
		last_committed ++;
	}
	DUMP_LOG
	pthread_exit((void*)retries);
}

}

namespace otl2_v2_runner{

static volatile int last_committed;
int* jobs;

void init(){
	last_committed = 0;
	jobs = new int[workload];
	for(int i=0; i<workload; i++){
		jobs[i] = 1;
	}
}

void* run(void* threadid){
	INIT_THREAD_LOG
	long tid = (long) threadid;
	stick_worker((int) tid);
	INFO("Thread " << tid);
	long retries = 0;
	for(int i=0; i<workload; i++){
		if(!__sync_bool_compare_and_swap(&(jobs[i]), 1, 0))	// acquired by another work
			continue;
		int age = i;
		BENCH::benchmark_start(age, tid);
		jmp_buf _jmpbuf;
		int abort_flags = setjmp(_jmpbuf);
		retries++;
		tl2::Transaction* tx = new tl2::Transaction();
		tx->start(&_jmpbuf);
		BENCH::benchmark(tx, age, tid);
		while(last_committed != age);
		tx->commit();
		last_committed ++;
	}
	DUMP_LOG
	pthread_exit((void*)retries);
}

}

namespace otl2_v3_runner{

static volatile int last_committed;
int jobs[JOBS_SHARED_QUEUE_LENGTH];
int epochs;

void init(){
	last_committed = 0;
	epochs = (workload + JOBS_SHARED_QUEUE_LENGTH - 1)/JOBS_SHARED_QUEUE_LENGTH;
	for(int i=0; i<JOBS_SHARED_QUEUE_LENGTH; i++){
		jobs[i] = 0;
	}
}

void* run(void* threadid){
	INIT_THREAD_LOG
	long tid = (long) threadid;
	stick_worker((int) tid);
	INFO("Thread " << tid);
	long retries = 0;

	int jobId = -1;
	for(int j=0; j<epochs; j++){
		int marked = j+1;
		for(int i=0; i<JOBS_SHARED_QUEUE_LENGTH; i++){
			jobId++;
			if(jobs[i]==marked || !__sync_bool_compare_and_swap(&(jobs[i]), j, marked))	// acquired by another work
				continue;
			if(jobId >= workload) break;
			int age = jobId;
			BENCH::benchmark_start(age, tid);
			jmp_buf _jmpbuf;
			int abort_flags = setjmp(_jmpbuf);
			retries++;
			tl2::Transaction* tx = new tl2::Transaction();
			tx->start(&_jmpbuf);
			BENCH::benchmark(tx, age, tid);
			while(last_committed != age);
			tx->commit();
			last_committed ++;
		}
	}
	DUMP_LOG
	pthread_exit((void*)retries);
}

}

// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ NOrec ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ //
namespace norec_runner{

void* run(void* threadid){
	INIT_THREAD_LOG
	long tid = (long) threadid;
	stick_worker((int) tid);
	INFO("Thread " << tid);
	long retries = 0;
	int worker_load = workload / workers;
	for(int i=0; i<worker_load; i++){
		int age = CALC_AGE;
		BENCH::benchmark_start(age, tid);
		jmp_buf _jmpbuf;
		int abort_flags = setjmp(_jmpbuf);
		retries++;
		norec::Transaction* tx = new norec::Transaction();
		tx->start(&_jmpbuf);
		BENCH::benchmark(tx, age, tid);
		tx->commit();
	}
	DUMP_LOG
	pthread_exit((void*)retries);
}

}

// ~~~~~~~~~~~~~~~~~~~~~~~~~~~ Ordered NOrec ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ //
namespace onorec_v1_runner{

static volatile int last_committed;

void init(){
	last_committed = 0;
}

void* run(void* threadid){
	INIT_THREAD_LOG
	long tid = (long) threadid;
	stick_worker((int) tid);
	INFO("Thread " << tid);
	long retries = 0;
	int worker_load = workload / workers;
	for(int i=0; i<worker_load; i++){
		int age = CALC_AGE;
		BENCH::benchmark_start(age, tid);
		jmp_buf _jmpbuf;
		int abort_flags = setjmp(_jmpbuf);
		retries++;
		norec::Transaction* tx = new norec::Transaction();
		tx->start(&_jmpbuf);
		BENCH::benchmark(tx, age, tid);
		while(last_committed != age);
		tx->commit();
		last_committed ++;
	}
	DUMP_LOG
	pthread_exit((void*)retries);
}

}

namespace onorec_v2_runner{

static volatile int last_committed;
int* jobs;

void init(){
	last_committed = 0;
	jobs = new int[workload];
	for(int i=0; i<workload; i++){
		jobs[i] = 1;
	}
}

void* run(void* threadid){
	INIT_THREAD_LOG
	long tid = (long) threadid;
	stick_worker((int) tid);
	INFO("Thread " << tid);
	long retries = 0;
	for(int i=0; i<workload; i++){
		if(!__sync_bool_compare_and_swap(&(jobs[i]), 1, 0))	// acquired by another work
			continue;
		int age = i;
		BENCH::benchmark_start(age, tid);
		jmp_buf _jmpbuf;
		int abort_flags = setjmp(_jmpbuf);
		retries++;
		norec::Transaction* tx = new norec::Transaction();
		tx->start(&_jmpbuf);
		BENCH::benchmark(tx, age, tid);
		while(last_committed != age);
		tx->commit();
		last_committed ++;
	}
	DUMP_LOG
	pthread_exit((void*)retries);
}

}

namespace onorec_v3_runner{

static volatile int last_committed;
int jobs[JOBS_SHARED_QUEUE_LENGTH];
int epochs;

void init(){
	last_committed = 0;
	epochs = (workload + JOBS_SHARED_QUEUE_LENGTH - 1)/JOBS_SHARED_QUEUE_LENGTH;
	for(int i=0; i<JOBS_SHARED_QUEUE_LENGTH; i++){
		jobs[i] = 0;
	}
}

void* run(void* threadid){
	INIT_THREAD_LOG
	long tid = (long) threadid;
	stick_worker((int) tid);
	INFO("Thread " << tid);
	long retries = 0;

	int jobId = -1;
	for(int j=0; j<epochs; j++){
		int marked = j+1;
		for(int i=0; i<JOBS_SHARED_QUEUE_LENGTH; i++){
			jobId++;
			if(jobs[i]==marked || !__sync_bool_compare_and_swap(&(jobs[i]), j, marked))	// acquired by another work
				continue;
			if(jobId >= workload) break;
			int age = jobId;
			BENCH::benchmark_start(age, tid);
			jmp_buf _jmpbuf;
			int abort_flags = setjmp(_jmpbuf);
			retries++;
			norec::Transaction* tx = new norec::Transaction();
			tx->start(&_jmpbuf);
			BENCH::benchmark(tx, age, tid);
			while(last_committed != age);
			tx->commit();
			last_committed ++;
		}
	}
	DUMP_LOG
	pthread_exit((void*)retries);
}

}


// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ UndoLog Invisible ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ //
namespace undolog_invis_runner{

void* run(void* threadid){
	INIT_THREAD_LOG
	long tid = (long) threadid;
	stick_worker((int) tid);
	INFO("Thread " << tid);
	long retries = 0;
	int worker_load = workload / workers;
	for(int i=0; i<worker_load; i++){
		int age = CALC_AGE;
		BENCH::benchmark_start(age, tid);
		jmp_buf _jmpbuf;
		int abort_flags = setjmp(_jmpbuf);
		retries++;
		undolog_invis::Transaction* tx = new undolog_invis::Transaction(age);
		tx->start(&_jmpbuf);
		BENCH::benchmark(tx, age, tid);
		tx->commit();
	}
	DUMP_LOG
	pthread_exit((void*)retries);
}

}

namespace undolog_invis_ordered_runner{

static volatile int last_committed;
int jobs[JOBS_SHARED_QUEUE_LENGTH];
int epochs;

void init(){
	last_committed = 0;
	epochs = (workload + JOBS_SHARED_QUEUE_LENGTH - 1)/JOBS_SHARED_QUEUE_LENGTH;
	for(int i=0; i<JOBS_SHARED_QUEUE_LENGTH; i++){
		jobs[i] = 0;
	}
}

void* run(void* threadid){
	INIT_THREAD_LOG
	long tid = (long) threadid;
	stick_worker((int) tid);
	INFO("Thread " << tid);
	long retries = 0;

	int jobId = -1;
	for(int j=0; j<epochs; j++){
		int marked = j+1;
		for(int i=0; i<JOBS_SHARED_QUEUE_LENGTH; i++){
			jobId++;
			if(jobs[i]==marked || !__sync_bool_compare_and_swap(&(jobs[i]), j, marked))	// acquired by another work
				continue;
			if(jobId >= workload) break;
			int age = jobId;
			BENCH::benchmark_start(age, tid);
			jmp_buf _jmpbuf;
			int abort_flags = setjmp(_jmpbuf);
			retries++;
			undolog_invis::Transaction* tx = new undolog_invis::Transaction(age);
			tx->start(&_jmpbuf);
			BENCH::benchmark(tx, age, tid);
			bool timeout = true;
			for(int i=0; i<TIMEOUT_VALUE; i++)
				if(last_committed == age){
					tx->commit();
					last_committed++;
					timeout = false;
					break;
				}
			if(timeout)
				tx->suicide(0);
		}
	}
	DUMP_LOG
	pthread_exit((void*)retries);
}

}

// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ UndoLog Visible ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ //
namespace undolog_visible_runner{

void preInit(){
	undolog_vis::init();
}

void* run(void* threadid){
	INIT_THREAD_LOG
	long tid = (long) threadid;
	stick_worker((int) tid);
	INFO("Thread " << tid);
	long retries = 0;
	int worker_load = workload / workers;
	for(int i=0; i<worker_load; i++){
		int age = CALC_AGE;
		BENCH::benchmark_start(age, tid);
		jmp_buf _jmpbuf;
		int abort_flags = setjmp(_jmpbuf);
		retries++;
		undolog_vis::Transaction* tx = new undolog_vis::Transaction(age);
		tx->start(&_jmpbuf);
		BENCH::benchmark(tx, age, tid);
		tx->commit();
		tx->complete();
	}
	DUMP_LOG
	pthread_exit((void*)retries);
}

}

namespace ordered_undolog_visible_v1_runner{

static volatile int last_committed;
int jobs[JOBS_SHARED_QUEUE_LENGTH];
int epochs;

void preInit(){
	undolog_vis::init();
}

void init(){
	last_committed = 0;
	epochs = (workload + JOBS_SHARED_QUEUE_LENGTH - 1)/JOBS_SHARED_QUEUE_LENGTH;
	for(int i=0; i<JOBS_SHARED_QUEUE_LENGTH; i++){
		jobs[i] = 0;
	}
}

void* run(void* threadid){
	INIT_THREAD_LOG
	long tid = (long) threadid;
	stick_worker((int) tid);
	INFO("Thread " << tid);
	long retries = 0;

	int jobId = -1;
	for(int j=0; j<epochs; j++){
		int marked = j+1;
		for(int i=0; i<JOBS_SHARED_QUEUE_LENGTH; i++){
			jobId++;
			if(jobs[i]==marked || !__sync_bool_compare_and_swap(&(jobs[i]), j, marked))	// acquired by another work
				continue;
			if(jobId >= workload) break;
			int age = jobId;
			BENCH::benchmark_start(age, tid);
			jmp_buf _jmpbuf;
			int abort_flags = setjmp(_jmpbuf);
			retries++;
			undolog_vis::Transaction* tx = new undolog_vis::Transaction(age);
			tx->start(&_jmpbuf);
			BENCH::benchmark(tx, age, tid);
			tx->commit();
			bool timeout = true;
			for(int i=0; i<TIMEOUT_VALUE; i++)
				if(last_committed == age){
					tx->complete();
					last_committed++;
					timeout = false;
					break;
				}
			if(timeout)
				tx->suicide(TIMEOUT);
		}
	}
	DUMP_LOG
	pthread_exit((void*)retries);
}

}

namespace ordered_undolog_visible_v2_runner{

int jobs[JOBS_SHARED_QUEUE_LENGTH];
int pending_queue_length;
int pending_mask;
undolog_vis::Transaction** committedTxs;
int epochs;
int max_pending;


long restartTx(int age, long tid){
	long retries = 0;
	BENCH::benchmark_start(age, tid);
	jmp_buf _jmpbuf;
	int abort_flags = setjmp(_jmpbuf);
	retries++;
	undolog_vis::Transaction* tx = new undolog_vis::Transaction(age);
	LOG("restart " << age);
	tx->start(&_jmpbuf);
	BENCH::benchmark(tx, age, tid);
	tx->commit();
	if(tx->complete())
		if(cleaners == 0)
			tx->clean();
	LOG("restarted " << age);
	return retries;
}

void preInit(){
	undolog_vis::init();
}

void init(){
	undolog_vis::last_completed = -1;
	undolog_vis::validation = 0;
	epochs = (workload + JOBS_SHARED_QUEUE_LENGTH - 1)/JOBS_SHARED_QUEUE_LENGTH;
	max_pending = workers * MAX_PENDING_PER_THREAD;

	for(int i=0; i<JOBS_SHARED_QUEUE_LENGTH; i++){
		jobs[i] = 0;
	}
	pending_queue_length = 1024;
	while(pending_queue_length < max_pending)
		pending_queue_length *= 2;
	committedTxs = new undolog_vis::Transaction*[pending_queue_length];
	pending_mask = pending_queue_length - 1;
	for(int i=0; i<pending_queue_length; i++){
		committedTxs[i] = NULL;
	}
}

//long long validation_time = 0;

int try_validation(int tid){
	if(
		undolog_vis::validation == 0 &&
		__sync_bool_compare_and_swap(&undolog_vis::validation, 0, 1)
	)
	{ // try to be the validator
//		long long startTime = getElapsedTime();
		int temp = undolog_vis::last_completed;
		int retries = 0;
		for(int j=undolog_vis::last_completed+1; j< workload; j++){
			int i = j & pending_mask;
			if(committedTxs[i] == NULL){
				break;
			}
			LOG("+++++++++ " << j << " +++++++++ ");
			if(committedTxs[i]->complete()){
				if(cleaners == 0)
					committedTxs[i]->clean();
			}else
				retries += restartTx(committedTxs[i]->getAge(), tid);
			committedTxs[i] = NULL;
			undolog_vis::last_completed++;
		}
	//	INFO("<" << temp << ":" << last_completed << ">");
		undolog_vis::validation = 0;	// reset the flag
//		long long endTime = getElapsedTime();
//		validation_time += endTime - startTime;
		return retries;
	}
	return 0;
}

void* run(void* threadid){
	INIT_THREAD_LOG
	long tid = (long) threadid;
	stick_worker((int) tid);
	long retries = 0;
//	long long work_time = 0;
	INFO("Thread " << tid);
	int jobId = -1;
	for(int j=0; j<epochs; j++){
		int marked = j+1;
		for(int i=0; i<JOBS_SHARED_QUEUE_LENGTH; i++){
			jobId++;
			retries += try_validation(tid);
			if(jobs[i]==marked || !__sync_bool_compare_and_swap(&(jobs[i]), j, marked))	// acquired by another work
				continue;
			if(jobId >= workload) break;
			while(jobId - undolog_vis::last_completed > max_pending);
			LOG(i << " start");
	//		long long startTime = getElapsedTime();
			int age = jobId;
			BENCH::benchmark_start(age, tid);
			jmp_buf _jmpbuf;
			int abort_flags = setjmp(_jmpbuf);
			retries++;
			LOG("exec " << age);
			undolog_vis::Transaction* tx = new undolog_vis::Transaction(age);
			tx->start(&_jmpbuf);
			BENCH::benchmark(tx, age, tid);
			tx->commit();
			tx->noRetry();
			committedTxs[age & pending_mask] = tx;
			LOG(i << " end");
	//		long long endTime = getElapsedTime();
	//		work_time += endTime - startTime;
		}
	}
	retries += try_validation(tid);
//	INFO("Thread " << tid << " work=" << work_time);
	DUMP_LOG
	pthread_exit((void*)retries);
}
}

// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~ OUL-speculative ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ //
namespace oul_speculative_runner{

int jobs[JOBS_SHARED_QUEUE_LENGTH];
int pending_queue_length;
int pending_mask;
oul_speculative::Transaction** committedTxs;
int epochs;
int max_pending;

long restartTx(int age, long tid){
	long retries = 0;
	BENCH::benchmark_start(age, tid);
	jmp_buf _jmpbuf;
	int abort_flags = setjmp(_jmpbuf);
	retries++;
	oul_speculative::Transaction* tx = new oul_speculative::Transaction(age);
	LOG("restart " << age);
	tx->start(&_jmpbuf);
	BENCH::benchmark(tx, age, tid);
	tx->commit();
	if(tx->complete())
		if(cleaners == 0)
			tx->clean();
	LOG("restarted " << age);
	return retries;
}

void preInit(){
	oul_speculative::init();
}

//int timing;
//long long info[100][4];

void init(){
	oul_speculative::last_completed = -1;
	oul_speculative::validation = 0;
	epochs = (workload + JOBS_SHARED_QUEUE_LENGTH - 1)/JOBS_SHARED_QUEUE_LENGTH;
	max_pending = workers * MAX_PENDING_PER_THREAD;

	for(int i=0; i<JOBS_SHARED_QUEUE_LENGTH; i++){
		jobs[i] = 0;
	}
	pending_queue_length = 1024;
	while(pending_queue_length < max_pending)
		pending_queue_length *= 2;
	committedTxs = new oul_speculative::Transaction*[pending_queue_length];
	pending_mask = pending_queue_length - 1;
	for(int i=0; i<pending_queue_length; i++){
		committedTxs[i] = NULL;
	}
//	timing = 0;
//	for(int i=0; i<100; i++)
//		info[i][0] = info[i][1] = info[i][2] = info[i][3] = 0;
}

//long long validation_time = 0;

int try_validation(int tid){
	if(
		oul_speculative::validation == 0 &&
		__sync_bool_compare_and_swap(&oul_speculative::validation, 0, 1)
	)
	{ // try to be the validator
//		long long startTime = getElapsedTime();
		int temp = oul_speculative::last_completed;
		int retries = 0;
		for(int j=oul_speculative::last_completed+1; j< workload; j++){
			int i = j & pending_mask;
			if(committedTxs[i] == NULL){
				break;
			}
			LOG("+++++++++ " << j << " +++++++++ ");
			if(committedTxs[i]->complete()){
				if(cleaners == 0)
					committedTxs[i]->clean();
			}else{
				long r = restartTx(committedTxs[i]->getAge(), tid);
//				info[i][2] += r;
				retries += r;
			}
			committedTxs[i] = NULL;
//			info[i][1] = timing++;
			oul_speculative::last_completed++;
		}
	//	INFO("<" << temp << ":" << last_completed << ">");
		oul_speculative::validation = 0;	// reset the flag
//		long long endTime = getElapsedTime();
//		validation_time += endTime - startTime;
		return retries;
	}
	return 0;
}

void* run(void* threadid){
	INIT_THREAD_LOG
	long tid = (long) threadid;
	stick_worker((int) tid);
	oul_speculative::init_thread();
	long retries = 0;
//	long long work_time = 0;
	INFO("Thread " << tid);
	int jobId = -1;
	for(int j=0; j<epochs; j++){
		int marked = j+1;
		for(int i=0; i<JOBS_SHARED_QUEUE_LENGTH; i++){
			jobId++;
			retries += try_validation(tid);
			if(jobs[i]==marked || !__sync_bool_compare_and_swap(&(jobs[i]), j, marked))	// acquired by another work
				continue;
			if(jobId >= workload) break;
			while(jobId - oul_speculative::last_completed > max_pending);
			LOG(i << " start");
//			info[jobId][3] = tid;
//			info[jobId][0] = timing++;
	//		long long startTime = getElapsedTime();
//			long temp = retries;
			int age = jobId;
			BENCH::benchmark_start(age, tid);
			jmp_buf _jmpbuf;
			setjmp(_jmpbuf);
			retries++;
			LOG("exec " << age);
			oul_speculative::Transaction* tx = new oul_speculative::Transaction(age);
			tx->start(&_jmpbuf);
			BENCH::benchmark(tx, age, tid);
			tx->commit();
			tx->noRetry();
			committedTxs[age & pending_mask] = tx;
			LOG(i << " end");
//			info[jobId][2] += retries - temp;
	//		long long endTime = getElapsedTime();
	//		work_time += endTime - startTime;
		}
	}
	retries += try_validation(tid);
//	INFO("Thread " << tid << " work=" << work_time);
	DUMP_LOG
	oul_speculative::destory_thread();
	pthread_exit((void*)retries);
}
}

// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~ OUL-speculative+ ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ //
namespace oul_speculative_plus_runner{

int jobs[JOBS_SHARED_QUEUE_LENGTH];
int pending_queue_length;
int pending_mask;
oul_speculative::Transaction** committedTxs;
int epochs;
int max_pending;
int min_pending;
int hold_on;

long restartTx(int age, long tid){
	long retries = 0;
	BENCH::benchmark_start(age, tid);
	jmp_buf _jmpbuf;
	int abort_flags = setjmp(_jmpbuf);
	retries++;
	oul_speculative::Transaction* tx = new oul_speculative::Transaction(age);
	LOG("restart " << age);
	tx->start(&_jmpbuf);
	BENCH::benchmark(tx, age, tid);
	tx->commit();
	if(tx->complete())
		if(cleaners == 0)
			tx->clean();
	LOG("restarted " << age);
	return retries;
}

void preInit(){
	oul_speculative::init();
}

void init(){
	oul_speculative::last_completed = -1;
	oul_speculative::validation = 0;
	epochs = (workload + JOBS_SHARED_QUEUE_LENGTH - 1)/JOBS_SHARED_QUEUE_LENGTH;
	max_pending = workers * MAX_PENDING_PER_THREAD;
	min_pending = workers;
	hold_on = 0;
	for(int i=0; i<JOBS_SHARED_QUEUE_LENGTH; i++){
		jobs[i] = 0;
	}
	pending_queue_length = 1024;
	while(pending_queue_length < max_pending)
		pending_queue_length *= 2;
	committedTxs = new oul_speculative::Transaction*[pending_queue_length];
	pending_mask = pending_queue_length - 1;
	for(int i=0; i<pending_queue_length; i++){
		committedTxs[i] = NULL;
	}
}

//long long validation_time = 0;

int try_validation(int tid){
	if(
		oul_speculative::validation == 0 &&
		__sync_bool_compare_and_swap(&oul_speculative::validation, 0, 1)
	)
	{ // try to be the validator
//		long long startTime = getElapsedTime();
		int temp = oul_speculative::last_completed;
		int retries = 0;
		for(int j=oul_speculative::last_completed+1; j< workload; j++){
			int i = j & pending_mask;
			if(committedTxs[i] == NULL){
				break;
			}
			LOG("+++++++++ " << j << " +++++++++ ");
			if(committedTxs[i]->complete()){
				if(cleaners == 0)
					committedTxs[i]->clean();
			}else{
				long r = restartTx(committedTxs[i]->getAge(), tid);
				retries += r;
			}
			committedTxs[i] = NULL;
			oul_speculative::last_completed++;
		}
	//	INFO("<" << temp << ":" << last_completed << ">");
		oul_speculative::validation = 0;	// reset the flag
//		long long endTime = getElapsedTime();
//		validation_time += endTime - startTime;
		return retries;
	}
	return 0;
}

void* run(void* threadid){
	INIT_THREAD_LOG
	long tid = (long) threadid;
	stick_worker((int) tid);
	oul_speculative::init_thread();
	long retries = 0;
//	long long work_time = 0;
	INFO("Thread " << tid);
	int jobId = -1;
	for(int j=0; j<epochs; j++){
		int marked = j+1;
		for(int i=0; i<JOBS_SHARED_QUEUE_LENGTH; i++){
			jobId++;
			retries += try_validation(tid);
			if(jobs[i]==marked || !__sync_bool_compare_and_swap(&(jobs[i]), j, marked))	// acquired by another work
				continue;
			if(jobId >= workload) break;
			if(jobId - oul_speculative::last_completed > max_pending){
				hold_on++;
				while(jobId - oul_speculative::last_completed > min_pending);	// freeze execution
			}
			LOG(i << " start");
	//		long long startTime = getElapsedTime();
			int age = jobId;
			BENCH::benchmark_start(age, tid);
			jmp_buf _jmpbuf;
			setjmp(_jmpbuf);
			retries++;
			LOG("exec " << age);
			oul_speculative::Transaction* tx = new oul_speculative::Transaction(age);
			tx->start(&_jmpbuf);
			BENCH::benchmark(tx, age, tid);
			tx->commit();
			tx->noRetry();
			committedTxs[age & pending_mask] = tx;
			LOG(i << " end");
	//		long long endTime = getElapsedTime();
	//		work_time += endTime - startTime;
		}
	}
	retries += try_validation(tid);
//	INFO("Thread " << tid << " work=" << work_time);
	DUMP_LOG
	oul_speculative::destory_thread();
	pthread_exit((void*)retries);
}
}

// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~ OUL-speculative++ ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ //
namespace oul_speculative_plus_plus_runner{

int jobs[JOBS_SHARED_QUEUE_LENGTH];
int pending_queue_length;
int pending_mask;
oul_speculative::Transaction** committedTxs;
int epochs;
int min_pending;
int hold_on;
long aborts;

long restartTx(int age, long tid){
	long retries = 0;
	BENCH::benchmark_start(age, tid);
	jmp_buf _jmpbuf;
	int abort_flags = setjmp(_jmpbuf);
	retries++;
	oul_speculative::Transaction* tx = new oul_speculative::Transaction(age);
	LOG("restart " << age);
	tx->start(&_jmpbuf);
	BENCH::benchmark(tx, age, tid);
	tx->commit();
	if(tx->complete())
		if(cleaners == 0)
			tx->clean();
	LOG("restarted " << age);
	return retries;
}

void preInit(){
	oul_speculative::init();
}

void init(){
	oul_speculative::last_completed = -1;
	oul_speculative::validation = 0;
	epochs = (workload + JOBS_SHARED_QUEUE_LENGTH - 1)/JOBS_SHARED_QUEUE_LENGTH;
	min_pending = workers;
	aborts = 0;
	hold_on = 0;
	for(int i=0; i<JOBS_SHARED_QUEUE_LENGTH; i++){
		jobs[i] = 0;
	}
	pending_queue_length = 1024;
//	while(pending_queue_length < workers * MAX_PENDING_PER_THREAD)
//		pending_queue_length *= 2;
	committedTxs = new oul_speculative::Transaction*[pending_queue_length];
	pending_mask = pending_queue_length - 1;
	for(int i=0; i<pending_queue_length; i++){
		committedTxs[i] = NULL;
	}
}

//long long validation_time = 0;

int try_validation(int tid){
	if(
		oul_speculative::validation == 0 &&
		__sync_bool_compare_and_swap(&oul_speculative::validation, 0, 1)
	)
	{ // try to be the validator
//		long long startTime = getElapsedTime();
		int temp = oul_speculative::last_completed;
		int retries = 0;
		for(int j=oul_speculative::last_completed+1; j< workload; j++){
			int i = j & pending_mask;
			if(committedTxs[i] == NULL){
				break;
			}
			LOG("+++++++++ " << j << " +++++++++ ");
			if(committedTxs[i]->complete()){
				if(cleaners == 0)
					committedTxs[i]->clean();
			}else{
				long r = restartTx(committedTxs[i]->getAge(), tid);
				retries += r;
				aborts += r - 1;
			}
			committedTxs[i] = NULL;
			oul_speculative::last_completed++;
		}
	//	INFO("<" << temp << ":" << last_completed << ">");
		oul_speculative::validation = 0;	// reset the flag
//		long long endTime = getElapsedTime();
//		validation_time += endTime - startTime;
		return retries;
	}
	return 0;
}

void* run(void* threadid){
	INIT_THREAD_LOG
	long tid = (long) threadid;
	stick_worker((int) tid);
	oul_speculative::init_thread();
	long retries = 0;
//	long long work_time = 0;
	INFO("Thread " << tid);
	int jobId = -1;
	for(int j=0; j<epochs; j++){
		int marked = j+1;
		for(int i=0; i<JOBS_SHARED_QUEUE_LENGTH; i++){
			jobId++;
			retries += try_validation(tid);
			if(jobs[i]==marked || !__sync_bool_compare_and_swap(&(jobs[i]), j, marked))	// acquired by another work
				continue;
			if(jobId >= workload) break;
            if(aborts * MAX_ABORT_RATIO > jobId || jobId - oul_speculative::last_completed > pending_queue_length){
//         		INFO("Hold " << aborts << " " << jobId);
                hold_on++;
                while(jobId - oul_speculative::last_completed > min_pending) try_validation(tid); // freeze execution
            }
            LOG(i << " start");
	//		long long startTime = getElapsedTime();
			int age = jobId;
			long temp = retries;
			BENCH::benchmark_start(age, tid);
			jmp_buf _jmpbuf;
			setjmp(_jmpbuf);
			retries++;
			LOG("exec " << age);
			oul_speculative::Transaction* tx = new oul_speculative::Transaction(age);
			tx->start(&_jmpbuf);
			BENCH::benchmark(tx, age, tid);
			tx->commit();
			tx->noRetry();
			committedTxs[age & pending_mask] = tx;
			aborts += retries - temp - 1;
			LOG(i << " end");
	//		long long endTime = getElapsedTime();
	//		work_time += endTime - startTime;
		}
	}
	retries += try_validation(tid);
//	INFO("Thread " << tid << " work=" << work_time);
	DUMP_LOG
	oul_speculative::destory_thread();
	pthread_exit((void*)retries);
}
}

// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~ OUL-steal ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ //
namespace oul_steal_runner{

int jobs[JOBS_SHARED_QUEUE_LENGTH];
int pending_queue_length;
int pending_mask;
oul_steal::Transaction** committedTxs;
int epochs;
int max_pending;

long restartTx(int age, long tid){
	long retries = 0;
	BENCH::benchmark_start(age, tid);
	jmp_buf _jmpbuf;
	int abort_flags = setjmp(_jmpbuf);
	retries++;
	oul_steal::Transaction* tx = new oul_steal::Transaction(age);
	LOG("restart " << age);
	tx->start(&_jmpbuf);
	BENCH::benchmark(tx, age, tid);
	tx->commit();
	if(tx->complete())
		if(cleaners == 0)
			tx->clean();
	LOG("restarted " << age);
	return retries;
}

void preInit(){
	oul_steal::init();
}

//int timing;
//long long info[100][4];

void init(){
	oul_steal::last_completed = -1;
	oul_steal::validation = 0;
	epochs = (workload + JOBS_SHARED_QUEUE_LENGTH - 1)/JOBS_SHARED_QUEUE_LENGTH;
	max_pending = workers * MAX_PENDING_PER_THREAD;

	for(int i=0; i<JOBS_SHARED_QUEUE_LENGTH; i++){
		jobs[i] = 0;
	}
	pending_queue_length = 1024;
	while(pending_queue_length < max_pending)
		pending_queue_length *= 2;
	committedTxs = new oul_steal::Transaction*[pending_queue_length];
	pending_mask = pending_queue_length - 1;
	for(int i=0; i<pending_queue_length; i++){
		committedTxs[i] = NULL;
	}

//	timing = 0;
//	for(int i=0; i<100; i++)
//		info[i][0] = info[i][1] = info[i][2] = info[i][3] = 0;
}

//long long validation_time = 0;

int try_validation(int tid){
	if(
		oul_steal::validation == 0 &&
		__sync_bool_compare_and_swap(&oul_steal::validation, 0, 1)
	)
	{ // try to be the validator
//		long long startTime = getElapsedTime();
		int temp = oul_steal::last_completed;
		int retries = 0;
		for(int j=oul_steal::last_completed+1; j< workload; j++){
			int i = j & pending_mask;
			if(committedTxs[i] == NULL){
				break;
			}
			LOG("+++++++++ " << j << " +++++++++ ");
			if(committedTxs[i]->complete()){
				if(cleaners == 0)
					committedTxs[i]->clean();
			}else{
				long r = restartTx(committedTxs[i]->getAge(), tid);
//				info[i][2] += r;
				retries += r;
			}
			committedTxs[i] = NULL;
//			info[i][1] = timing++;
			oul_steal::last_completed++;
		}
	//	INFO("<" << temp << ":" << last_completed << ">");
		oul_steal::validation = 0;	// reset the flag
//		long long endTime = getElapsedTime();
//		validation_time += endTime - startTime;
		return retries;
	}
	return 0;
}

void* run(void* threadid){
	INIT_THREAD_LOG
	long tid = (long) threadid;
	stick_worker((int) tid);
	oul_steal::init_thread();
	long retries = 0;
//	long long work_time = 0;
	INFO("Thread " << tid);
	int jobId = -1;
	for(int j=0; j<epochs; j++){
		int marked = j+1;
		for(int i=0; i<JOBS_SHARED_QUEUE_LENGTH; i++){
			jobId++;
			retries += try_validation(tid);
			if(jobs[i]==marked || !__sync_bool_compare_and_swap(&(jobs[i]), j, marked))	// acquired by another work
				continue;
			if(jobId >= workload) break;
			while(jobId - oul_steal::last_completed > max_pending);
			LOG(i << " start");
//		long long startTime = getElapsedTime();
//			info[jobId][3] = tid;
//			info[jobId][0] = timing++;
//			long temp = retries;
			int age = jobId;
			BENCH::benchmark_start(age, tid);
			jmp_buf _jmpbuf;
			setjmp(_jmpbuf);
			retries++;
			LOG("exec " << age);
			oul_steal::Transaction* tx = new oul_steal::Transaction(age);
			tx->start(&_jmpbuf);
			BENCH::benchmark(tx, age, tid);
			tx->commit();
			tx->noRetry();
			committedTxs[age & pending_mask] = tx;
			LOG(i << " end");
//			info[jobId][2] += retries - temp;
//			long long endTime = getElapsedTime();
	//		work_time += endTime - startTime;
		}
	}
	retries += try_validation(tid);
//	INFO("Thread " << tid << " work=" << work_time);
	DUMP_LOG
	oul_steal::destory_thread();
	pthread_exit((void*)retries);
}
}

// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~ OUL-steal+ ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ //
namespace oul_steal_plus_runner{

int jobs[JOBS_SHARED_QUEUE_LENGTH];
int pending_queue_length;
int pending_mask;
oul_steal::Transaction** committedTxs;
int epochs;
int max_pending;
int min_pending;
int hold_on;

long restartTx(int age, long tid){
	long retries = 0;
	BENCH::benchmark_start(age, tid);
	jmp_buf _jmpbuf;
	int abort_flags = setjmp(_jmpbuf);
	retries++;
	oul_steal::Transaction* tx = new oul_steal::Transaction(age);
	LOG("restart " << age);
	tx->start(&_jmpbuf);
	BENCH::benchmark(tx, age, tid);
	tx->commit();
	if(tx->complete())
		if(cleaners == 0)
			tx->clean();
	LOG("restarted " << age);
	return retries;
}

void preInit(){
	oul_steal::init();
}

void init(){
	oul_steal::last_completed = -1;
	oul_steal::validation = 0;
	epochs = (workload + JOBS_SHARED_QUEUE_LENGTH - 1)/JOBS_SHARED_QUEUE_LENGTH;
	max_pending = workers * MAX_PENDING_PER_THREAD;
	min_pending = workers;
	hold_on = 0;
	for(int i=0; i<JOBS_SHARED_QUEUE_LENGTH; i++){
		jobs[i] = 0;
	}
	pending_queue_length = 1024;
	while(pending_queue_length < max_pending)
		pending_queue_length *= 2;
	committedTxs = new oul_steal::Transaction*[pending_queue_length];
	pending_mask = pending_queue_length - 1;
	for(int i=0; i<pending_queue_length; i++){
		committedTxs[i] = NULL;
	}
}

//long long validation_time = 0;

int try_validation(int tid){
	if(
		oul_steal::validation == 0 &&
		__sync_bool_compare_and_swap(&oul_steal::validation, 0, 1)
	)
	{ // try to be the validator
//		long long startTime = getElapsedTime();
		int temp = oul_steal::last_completed;
		int retries = 0;
		for(int j=oul_steal::last_completed+1; j< workload; j++){
			int i = j & pending_mask;
			if(committedTxs[i] == NULL){
				break;
			}
			LOG("+++++++++ " << j << " +++++++++ ");
			if(committedTxs[i]->complete()){
				if(cleaners == 0)
					committedTxs[i]->clean();
			}else{
				long r = restartTx(committedTxs[i]->getAge(), tid);
				retries += r;
			}
			committedTxs[i] = NULL;
			oul_steal::last_completed++;
		}
	//	INFO("<" << temp << ":" << last_completed << ">");
		oul_steal::validation = 0;	// reset the flag
//		long long endTime = getElapsedTime();
//		validation_time += endTime - startTime;
		return retries;
	}
	return 0;
}

void* run(void* threadid){
	INIT_THREAD_LOG
	long tid = (long) threadid;
	stick_worker((int) tid);
	oul_steal::init_thread();
	long retries = 0;
//	long long work_time = 0;
	INFO("Thread " << tid);
	int jobId = -1;
	for(int j=0; j<epochs; j++){
		int marked = j+1;
		for(int i=0; i<JOBS_SHARED_QUEUE_LENGTH; i++){
			jobId++;
			retries += try_validation(tid);
			if(jobs[i]==marked || !__sync_bool_compare_and_swap(&(jobs[i]), j, marked))	// acquired by another work
				continue;
			if(jobId >= workload) break;
			if(jobId - oul_steal::last_completed > max_pending){
				hold_on++;
				while(jobId - oul_steal::last_completed > min_pending);	// freeze execution
			}
			LOG(i << " start");
//		long long startTime = getElapsedTime();
			int age = jobId;
			BENCH::benchmark_start(age, tid);
			jmp_buf _jmpbuf;
			setjmp(_jmpbuf);
			retries++;
			LOG("exec " << age);
			oul_steal::Transaction* tx = new oul_steal::Transaction(age);
			tx->start(&_jmpbuf);
			BENCH::benchmark(tx, age, tid);
			tx->commit();
			tx->noRetry();
			committedTxs[age & pending_mask] = tx;
			LOG(i << " end");
//			long long endTime = getElapsedTime();
	//		work_time += endTime - startTime;
		}
	}
	retries += try_validation(tid);
//	INFO("Thread " << tid << " work=" << work_time);
	DUMP_LOG
	oul_steal::destory_thread();
	pthread_exit((void*)retries);
}
}


// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~ OUL-steal++ ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ //
namespace oul_steal_plus_plus_runner{

int jobs[JOBS_SHARED_QUEUE_LENGTH];
int pending_queue_length;
int pending_mask;
oul_steal::Transaction** committedTxs;
int epochs;
int min_pending;
int hold_on;
long aborts;

long restartTx(int age, long tid){
	long retries = 0;
	BENCH::benchmark_start(age, tid);
	jmp_buf _jmpbuf;
	int abort_flags = setjmp(_jmpbuf);
	retries++;
	oul_steal::Transaction* tx = new oul_steal::Transaction(age);
	LOG("restart " << age);
	tx->start(&_jmpbuf);
	BENCH::benchmark(tx, age, tid);
	tx->commit();
	if(tx->complete())
		if(cleaners == 0)
			tx->clean();
	LOG("restarted " << age);
	return retries;
}

void preInit(){
	oul_steal::init();
}

void init(){
	oul_steal::last_completed = -1;
	oul_steal::validation = 0;
	epochs = (workload + JOBS_SHARED_QUEUE_LENGTH - 1)/JOBS_SHARED_QUEUE_LENGTH;
	min_pending = workers;
	aborts = 0;
	hold_on = 0;
	for(int i=0; i<JOBS_SHARED_QUEUE_LENGTH; i++){
		jobs[i] = 0;
	}
	pending_queue_length = 1024;
//	while(pending_queue_length < workers * MAX_PENDING_PER_THREAD)
//		pending_queue_length *= 2;
	committedTxs = new oul_steal::Transaction*[pending_queue_length];
	pending_mask = pending_queue_length - 1;
	for(int i=0; i<pending_queue_length; i++){
		committedTxs[i] = NULL;
	}
}

//long long validation_time = 0;

int try_validation(int tid){
	if(
		oul_steal::validation == 0 &&
		__sync_bool_compare_and_swap(&oul_steal::validation, 0, 1)
	)
	{ // try to be the validator
//		long long startTime = getElapsedTime();
		int temp = oul_steal::last_completed;
		int retries = 0;
		for(int j=oul_steal::last_completed+1; j< workload; j++){
			int i = j & pending_mask;
			if(committedTxs[i] == NULL){
				break;
			}
			LOG("+++++++++ " << j << " +++++++++ ");
			if(committedTxs[i]->complete()){
				if(cleaners == 0)
					committedTxs[i]->clean();
			}else{
				long r = restartTx(committedTxs[i]->getAge(), tid);
				retries += r;
				aborts += r - 1;
			}
			committedTxs[i] = NULL;
			oul_steal::last_completed++;
		}
	//	INFO("<" << temp << ":" << last_completed << ">");
		oul_steal::validation = 0;	// reset the flag
//		long long endTime = getElapsedTime();
//		validation_time += endTime - startTime;
		return retries;
	}
	return 0;
}

void* run(void* threadid){
	INIT_THREAD_LOG
	long tid = (long) threadid;
	stick_worker((int) tid);
	oul_steal::init_thread();
	long retries = 0;
//	long long work_time = 0;
	INFO("Thread " << tid);
	int jobId = -1;
	for(int j=0; j<epochs; j++){
		int marked = j+1;
		for(int i=0; i<JOBS_SHARED_QUEUE_LENGTH; i++){
			jobId++;
			int r = try_validation(tid);
			retries += r;
			if(jobs[i]==marked || !__sync_bool_compare_and_swap(&(jobs[i]), j, marked))	// acquired by another work
				continue;
			if(jobId >= workload) break;
            if(aborts * MAX_ABORT_RATIO > jobId || jobId - oul_steal::last_completed > pending_queue_length){
//         		INFO("Hold " << aborts << " " << jobId);
                hold_on++;
                while(jobId - oul_steal::last_completed > min_pending) try_validation(tid); // freeze execution
            }
			LOG(i << " start");
//		long long startTime = getElapsedTime();
			int age = jobId;
			long temp = retries;
			BENCH::benchmark_start(age, tid);
			jmp_buf _jmpbuf;
			setjmp(_jmpbuf);
			retries++;
			LOG("exec " << age);
			oul_steal::Transaction* tx = new oul_steal::Transaction(age);
			tx->start(&_jmpbuf);
			BENCH::benchmark(tx, age, tid);
			tx->commit();
			tx->noRetry();
			committedTxs[age & pending_mask] = tx;
			aborts += retries - temp - 1;
			LOG(i << " end");
//			long long endTime = getElapsedTime();
	//		work_time += endTime - startTime;
		}
	}
	retries += try_validation(tid);
//	INFO("Thread " << tid << " work=" << work_time);
	DUMP_LOG
	oul_steal::destory_thread();
	pthread_exit((void*)retries);
}
}

// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ TCM - unordered ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ //
namespace stmlite_unordered_runner{

stmlite_unordered::TCM *manager;

int jobs[JOBS_SHARED_QUEUE_LENGTH];
int epochs;

void init(){
	manager = new stmlite_unordered::TCM(workers);
	epochs = (workload + JOBS_SHARED_QUEUE_LENGTH - 1)/JOBS_SHARED_QUEUE_LENGTH;
	for(int i=0; i<JOBS_SHARED_QUEUE_LENGTH; i++){
		jobs[i] = 0;
	}
}

void* run_tcm(void* threadid){
	INIT_THREAD_LOG
	stick_worker(0);
	while(true){
		manager->validatePreCommitted();
		manager->cleanCommitedLog();
	}
	DUMP_LOG
	return 0;
}

void* run_workers(void* threadid){
	INIT_THREAD_LOG
	long tid = (long) threadid;
	stick_worker((int)tid+1);
	INFO("Thread " << tid);
	long retries = 0;

	int jobId = -1;
	for(int j=0; j<epochs; j++){
		int marked = j+1;
		for(int i=0; i<JOBS_SHARED_QUEUE_LENGTH; i++){
			jobId++;
			if(jobs[i]==marked || !__sync_bool_compare_and_swap(&(jobs[i]), j, marked))	// acquired by another work
				continue;
			if(jobId >= workload) break;
			int age = jobId;
			BENCH::benchmark_start(age, tid);
			jmp_buf _jmpbuf;
			int abort_flags = setjmp(_jmpbuf);
			retries++;
			stmlite_unordered::Transaction* tx = new stmlite_unordered::Transaction(age);
			tx->start(&_jmpbuf);
			manager->started(tx, tid);
			BENCH::benchmark(tx, age, tid);
			manager->finished(tx, tid);
			while(!tx->commit_flag){
				if(tx->abort_flag)
					tx->abort();
	//			pthread_yield();
			}
			tx->commit();
			manager->committed(tx, tid);
		}
	}
	DUMP_LOG
	pthread_exit((void*)retries);
}

}


// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ TCM ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ //
extern stmlite::TCM *manager;
namespace stmlite_runner{

int jobs[JOBS_SHARED_QUEUE_LENGTH];
int epochs;

void init(){
	stmlite::manager = new stmlite::TCM(workers);
	epochs = (workload + JOBS_SHARED_QUEUE_LENGTH - 1)/JOBS_SHARED_QUEUE_LENGTH;
	for(int i=0; i<JOBS_SHARED_QUEUE_LENGTH; i++){
		jobs[i] = 0;
	}
}

void* run_tcm(void* threadid){
	INIT_THREAD_LOG
	stick_worker(0);
	while(true){
		stmlite::manager->validatePreCommitted();
		stmlite::manager->cleanCommitedLog();
	}
	DUMP_LOG
	return 0;
}

void* run_workers(void* threadid){
	INIT_THREAD_LOG
	long tid = (long) threadid;
	stick_worker((int)tid+1);
	INFO("Thread " << tid);
	long retries = 0;

	int jobId = -1;
	for(int j=0; j<epochs; j++){
		int marked = j+1;
		for(int i=0; i<JOBS_SHARED_QUEUE_LENGTH; i++){
			jobId++;
			if(jobs[i]==marked || !__sync_bool_compare_and_swap(&(jobs[i]), j, marked))	// acquired by another work
				continue;
			if(jobId >= workload) break;
			int age = jobId;
			BENCH::benchmark_start(age, tid);
			jmp_buf _jmpbuf;
			int abort_flags = setjmp(_jmpbuf);
			retries++;
			stmlite::Transaction* tx = new stmlite::Transaction(age);
			tx->start(&_jmpbuf);
			stmlite::manager->started(tx, tid);
			BENCH::benchmark(tx, age, tid);
			stmlite::manager->finished(tx, tid);
			while(!tx->commit_flag){
				if(tx->abort_flag)
					tx->abort();
	//			pthread_yield();
			}
			tx->commit();
			stmlite::manager->committed(tx, tid);
		}
	}
	DUMP_LOG
	pthread_exit((void*)retries);
}

}

// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ //
static void threadpool_exit_handler2(int sigNumber) {
	std::cout << "Exit (signal " << sigNumber << ")\n";
	exit(1);
}

int main(int argc, char *argv[]) {
	struct sigaction exitAction;
	memset(&exitAction, 0, sizeof(exitAction));
	sigemptyset(&exitAction.sa_mask);
	exitAction.sa_flags = 0;
	exitAction.sa_handler = threadpool_exit_handler2;
	sigaction(SIGINT, &exitAction, NULL); // Exit signal

	INIT_LOG
	cout << "!!!Hello!!!" << endl;
	workers = get_cpu_workers();
	cleaners = 0;
	workload = -1;
	int impl = -1;
	int opt;    // parse the command-line options
	opterr = 0;
	while ((opt = getopt(argc, argv, "hw:a:c:j:")) != -1) {
		switch(opt) {
		  case 'w': workers = strtol(optarg, NULL, 10); break;
		  case 'j': workload = strtol(optarg, NULL, 10); break;
		  case 'a': impl = strtol(optarg, NULL, 10); break;
		  case 'c': cleaners = strtol(optarg, NULL, 10); break;
		  case 'h':
			  cout << "./OOO -w<workers> -c<cleaners> -j<jobs-count> -a<algorith> { benchmarks-specific-arguments }" << endl;
			  cout << "   Algorithms:" << endl;
			  cout << "\t\t"	<<	0	<< "\t Dummy/Sequential" 	<< endl;

			  cout << "\t\t"	<<	1	<< "\t OWB v1" 				<< endl;
			  cout << "\t\t"	<<	2	<< "\t OWB v2"		 		<< endl;
			  cout << "\t\t"	<<	3	<< "\t OWB v3" 				<< endl;
			  cout << "\t\t"	<<	4	<< "\t OWB v4"			 	<< endl;
			  cout << "\t*\t"	<<	5	<< "\t OWB v5" 				<< endl;
			  cout << "\t*\t"	<<	6	<< "\t OWB v5+" 			<< endl;
			  cout << "\t*\t"	<<	7	<< "\t OWB v5++" 			<< endl;
			  cout << "\t*\t"	<<	8	<< "\t OWB RH+" 			<< endl;
			  cout << "\t*\t"	<<	9	<< "\t OWB RH++" 			<< endl;

			  cout << "\t+\t"	<<	10	<< "\t TL2" 				<< endl;
			  cout << "\t\t"	<<	11	<< "\t OTL2 v1"				<< endl;
			  cout << "\t\t"	<<	12	<< "\t OTL2 v2"				<< endl;
			  cout << "\t*\t"	<<	13	<< "\t OTL2 v3"				<< endl;

			  cout << "\t+\t"	<<	20	<< "\t NOrec"				<< endl;
			  cout << "\t\t"	<<	21	<< "\t ONOrec v1"			<< endl;
			  cout << "\t\t"	<<	22	<< "\t ONOrec v2"			<< endl;
			  cout << "\t*\t"	<<	23	<< "\t ONOrec v3"			<< endl;

			  cout << "\t+\t"	<<	30	<< "\t UndoLog-Invisible"	<< endl;
			  cout << "\t*\t"	<<	31	<< "\t OUndoLog-Invisible"	<< endl;

			  cout << "\t+\t"	<<	40	<< "\t UndoLog-Visible"		<< endl;
			  cout << "\t*\t"	<<	41	<< "\t OUndoLog-Visible"	<< endl;
			  cout << "\t\t"	<<	42	<< "\t OUndoLog-Visible"	<< endl;

			  cout << "\t*\t"	<<	50	<< "\t OUL-speculative"		<< endl;
			  cout << "\t*\t"	<<	51	<< "\t OUL-speculative+"	<< endl;
			  cout << "\t*\t"	<<	52	<< "\t OUL-speculative++"	<< endl;

			  cout << "\t*\t"	<<	53	<< "\t OUL-steal"			<< endl;
			  cout << "\t*\t"	<<	54	<< "\t OUL-steal+"			<< endl;
			  cout << "\t*\t"	<<	55	<< "\t OUL-steal++"			<< endl;

			  cout << "\t\t"	<<	60	<< "\t STMLite-Unordered"	<< endl;
			  cout << "\t*\t"	<<	61	<< "\t STMLite"				<< endl;

			  exit(0);
		}
	}
	if(workload < 0)
		workload = workers * DEFAULT_WORKER_LOAD;
	optind = 1;
	BENCH::benchmark_init(argc, argv);
	switch(impl){
		case 40:
			undolog_visible_runner::preInit();
			break;
		case 41:
			undolog_visible_runner::preInit();
			break;
		case 42:
			ordered_undolog_visible_v2_runner::preInit();
			break;

		case 50:
		case 51:
		case 52:
			oul_speculative_runner::preInit();
			break;
		case 53:
		case 54:
		case 55:
			oul_steal_runner::preInit();
			break;
	}
	long long startTime = getElapsedTime();
	long executed;
	char* algorithm;
	switch(impl){
		case 0:
			algorithm = "Dummy";
			executed = run_cpu_workers(dummy_runner::run, 1, SCHED_RR);
			break;

		case 1:
			/*
			 * Version 1.0
			 * Private queue per thread for pending tasks, each thread complete lowest pending task in the queue
			 *
			 * Cons. Enqueue N task, and Dequeue 1 task  leads to too many active tasks
			 */
			algorithm = "OWB1";
			executed = run_cpu_workers(owb_v1_runner::run, workers, SCHED_RR);
			break;
		case 2:
			/*
			 * Version 2.0
			 * Shared queue for all pending tasks.
			 * Separate validator to take care of completing tasks.
			 * Multiple cleaners for house-keeping and free memory
			 *
			 * Cons. Context switch of workers prevent validator from keep pace with workers
			 */
			algorithm = "OWB2";
			owb_v2_runner::init();
			owb::init_counters();
			executed = run_cpu_workers(owb_v2_runner::run, workers + 1 + cleaners, SCHED_RR);
			owb::print_counters();
			break;
		case 3:
			/*
			 * Version 3.0
			 * Shared queue of to-be executed tasks
			 * Dynamic assignment of tasks to workers
			 *
			 * Cons. Context switch of validator causes a lot of active tasks
			 */
			algorithm = "OWB3";
			owb_v3_runner::init();
			owb::init_counters();
			executed = run_cpu_workers(owb_v3_runner::run, workers + 1 + cleaners, SCHED_RR);
			owb::print_counters();
			break;
		case 4:
			/*
			 * Version 4.0
			 * Shared queue of to-be executed tasks
			 * Dynamic assignment of tasks to workers
			 *
			 * Cons. Context switch of validator causes a lot of active tasks
			 */
			algorithm = "OWB4";
			owb_v4_runner::init();
			owb::init_counters();
			executed = run_cpu_workers(owb_v4_runner::run, workers, SCHED_RR);
			owb::print_counters();
//			cout << "Validation Time =  \t" << owb_v4::validation_time << "[" << owb::vtime1 << " " << owb::vtime2 << " " << owb::vtime3 << "]" << endl;
			break;
		case 5:
			/*
			 * Version 5.0
			 * Dynamic version of queues to support any number of jobs
			 */
			algorithm = "OWB5";
			owb_v5_runner::init();
			owb::init_counters();
			executed = run_cpu_workers(owb_v5_runner::run, workers, SCHED_RR);
			owb::print_counters();
			break;
		case 6:
			algorithm = "OWB5+";
			owb_v5_plus_runner::init();
			owb::init_counters();
			executed = run_cpu_workers(owb_v5_plus_runner::run, workers, SCHED_RR);
			owb::print_counters();
			cout <<"Holdons=" << owb_v5_plus_runner::hold_on << endl;
			break;
		case 7:
			algorithm = "OWB5++";
			owb_v5_plus_plus_runner::init();
			owb::init_counters();
			executed = run_cpu_workers(owb_v5_plus_plus_runner::run, workers, SCHED_RR);
			owb::print_counters();
			cout <<"Holdons=" << owb_v5_plus_plus_runner::hold_on << endl;
			break;
		case 8:
			algorithm = "OWB-RH++";
			owbrh_plus_plus_runner::init();
			owbrh::init_counters();
			executed = run_cpu_workers(owbrh_plus_plus_runner::run, workers, SCHED_RR);
			owbrh::print_counters();
			cout <<"Holdons=" << owbrh_plus_plus_runner::hold_on << endl;
			cout <<"HTM: Count=" << owbrh::htm_count << ", Fail=" << owbrh::htm_fail_count << endl;
			break;


		case 10:
			algorithm = "TL2";
			executed = run_cpu_workers(tl2_runner::run, workers, -1);
			break;
		case 11:
			algorithm = "OTL2_1";
			otl2_v1_runner::init();
			executed = run_cpu_workers(otl2_v1_runner::run, workers, -1);
			break;
		case 12:
			algorithm = "OTL2_2";
			otl2_v2_runner::init();
			executed = run_cpu_workers(otl2_v2_runner::run, workers, -1);
			break;
		case 13:
			algorithm = "OTL2_3";
			otl2_v3_runner::init();
			executed = run_cpu_workers(otl2_v3_runner::run, workers, -1);
			break;


		case 20:
			algorithm = "NOrec";
			executed = run_cpu_workers(norec_runner::run, workers, -1);
			break;
		case 21:
			algorithm = "ONOrec_1";
			onorec_v1_runner::init();
			executed = run_cpu_workers(onorec_v1_runner::run, workers, -1);
			break;
		case 22:
			algorithm = "ONOrec_2";
			onorec_v2_runner::init();
			executed = run_cpu_workers(onorec_v2_runner::run, workers, -1);
			break;
		case 23:
			algorithm = "ONOrec_3";
			onorec_v3_runner::init();
			executed = run_cpu_workers(onorec_v3_runner::run, workers, -1);
			break;


		case 30:
			algorithm = "UndoLog-Invisible";
			executed = run_cpu_workers(undolog_invis_runner::run, workers, -1);
			break;

			break;
		case 31:
			algorithm = "Ordered UndoLog-Invisible";
			undolog_invis_ordered_runner::init();
			executed = run_cpu_workers(undolog_invis_ordered_runner::run, workers, -1);
			break;

			break;


		case 40:
			algorithm = "UndoLog-Visible";
			undolog_vis::init_counters();
			executed = run_cpu_workers(undolog_visible_runner::run, workers, -1);
			undolog_vis::print_counters();
			break;
		case 41:
			algorithm = "Ordered UndoLog-Visible_1";
			ordered_undolog_visible_v1_runner::init();
			undolog_vis::init_counters();
			executed = run_cpu_workers(ordered_undolog_visible_v1_runner::run, workers, -1);
			undolog_vis::print_counters();
			break;
		case 42:
			/**
			 * This version is prone to deadlock.
			 * Higher age transaction may be pending waiting try-validation and holding locks
			 * Younger transactions will wait for ever for pending longs
			 */
			algorithm = "Ordered UndoLog-Visible_2";
			ordered_undolog_visible_v2_runner::init();
			undolog_vis::init_counters();
			executed = run_cpu_workers(ordered_undolog_visible_v2_runner::run, workers, -1);
			undolog_vis::print_counters();
			break;


		case 50:
			{
				algorithm = "OUL-speculative";
				oul_speculative_runner::init();
				oul_speculative::init_counters();
				executed = run_cpu_workers(oul_speculative_runner::run, workers, -1);
				oul_speculative::print_counters();
//				for(int i=0; i<100; i++)
//					INFO(i << ";" << oul_speculative_runner::info[i][0] << ";" << oul_speculative_runner::info[i][1] << ";" <<  oul_speculative_runner::info[i][2] << ";" <<  oul_speculative_runner::info[i][3]);
				break;
			}
		case 51:
			{
				algorithm = "OUL-speculative+";
				oul_speculative_plus_runner::init();
				oul_speculative::init_counters();
				executed = run_cpu_workers(oul_speculative_plus_runner::run, workers, -1);
				oul_speculative::print_counters();
				cout <<"Holdons=" << oul_speculative_plus_runner::hold_on << endl;
				break;
			}
		case 52:
			{
				algorithm = "OUL-speculative++";
				oul_speculative_plus_plus_runner::init();
				oul_speculative::init_counters();
				executed = run_cpu_workers(oul_speculative_plus_plus_runner::run, workers, -1);
				oul_speculative::print_counters();
				cout <<"Holdons=" << oul_speculative_plus_plus_runner::hold_on << endl;
				break;
			}
		case 53:
			{
				algorithm = "OUL-steal";
				oul_steal_runner::init();
				oul_steal::init_counters();
				executed = run_cpu_workers(oul_steal_runner::run, workers, -1);
				oul_steal::print_counters();
//				for(int i=0; i<100; i++)
//					INFO(i << ";" << oul_steal_runner::info[i][0] << ";" << oul_steal_runner::info[i][1] << ";" <<  oul_steal_runner::info[i][2] << ";" <<  oul_steal_runner::info[i][3]);
				break;
			}
		case 54:
			{
				algorithm = "OUL-steal+";
				oul_steal_plus_runner::init();
				oul_steal::init_counters();
				executed = run_cpu_workers(oul_steal_plus_runner::run, workers, -1);
				oul_steal::print_counters();
				cout <<"Holdons=" << oul_steal_plus_runner::hold_on << endl;
				break;
			}
		case 55:
			{
				algorithm = "OUL-steal++";
				oul_steal_plus_plus_runner::init();
				oul_steal::init_counters();
				executed = run_cpu_workers(oul_steal_plus_plus_runner::run, workers, -1);
				oul_steal::print_counters();
				cout <<"Holdons=" << oul_steal_plus_plus_runner::hold_on << endl;
				break;
			}


		case 60:
			{
				/**
				 * WBActionList is not implemented in this version
				 * So WAW is not checked
				 */
				algorithm = "STMLite-Unordered";
				if(workers < 2){
					cout <<"Workers must be more than 1 for TCM" << endl;
				}else{
					workers--;
					stmlite_unordered_runner::init();
					pthread_t tcm_worker;
					{
						pthread_attr_t custom_sched_attr;
						pthread_attr_init(&custom_sched_attr);
						pthread_attr_setscope(&custom_sched_attr, PTHREAD_SCOPE_SYSTEM);
						pthread_attr_setinheritsched(&custom_sched_attr, PTHREAD_EXPLICIT_SCHED);
						struct sched_param s_param;
						pthread_attr_setschedpolicy(&custom_sched_attr, SCHED_RR);
						s_param.sched_priority = sched_get_priority_max(SCHED_RR);
						pthread_attr_setschedparam(&custom_sched_attr, &s_param);
						pthread_create(&tcm_worker, &custom_sched_attr, stmlite_unordered_runner::run_tcm, NULL);
					}
					executed = run_cpu_workers(stmlite_unordered_runner::run_workers, workers, SCHED_RR);
					cout <<"SigOp=" << stmlite_unordered::operations << endl;
				}
			}
			break;
		case 61:
			{
				algorithm = "STMLite";
				if(workers < 2){
					cout <<"Workers must be more than 1 for TCM" << endl;
				}else{
					workers--;
					stmlite_runner::init();
					pthread_t tcm_worker;
					{
//						pthread_attr_t custom_sched_attr;
//						pthread_attr_init(&custom_sched_attr);
//						pthread_attr_setscope(&custom_sched_attr, PTHREAD_SCOPE_SYSTEM);
//						pthread_attr_setinheritsched(&custom_sched_attr, PTHREAD_EXPLICIT_SCHED);
//						struct sched_param s_param;
//						pthread_attr_setschedpolicy(&custom_sched_attr, SCHED_RR);
//						s_param.sched_priority = sched_get_priority_max(SCHED_RR);
//						pthread_attr_setschedparam(&custom_sched_attr, &s_param);
						pthread_create(&tcm_worker, NULL, stmlite_runner::run_tcm, NULL);
					}
					executed = run_cpu_workers(stmlite_runner::run_workers, workers, -1);
					cout <<"SigOp=" << stmlite::operations << endl;
				}
			}
			break;

		default: cout <<"Invalid Algorithm" << endl; exit(1);
	}
	long long endTime = getElapsedTime();
	BENCH::benchmark_destroy();
	cout << "===================================" << endl;
	cout << "Algorithm =\t" << algorithm << endl;
	cout << "Time =  \t" << endTime - startTime << endl;
	cout << "Workers = \t" << workers << endl;
	cout << "Cleaners = \t" << cleaners << endl;
	cout << "Transactions =\t" << workload << endl;
	cout << "Aborts =\t" << executed - workload << endl;
	cout << "===================================" << endl;
	cout << "!!!Bye!!!" << endl;
//	pthread_exit(NULL);
	exit(0);
	return 0;
}
