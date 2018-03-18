#ifndef O_API_HPP_
#define O_API_HPP_

#include <setjmp.h>
#include <atomic>

#include <iostream>
using namespace std;

#include "../algs/commons.hpp"

#include "../algs/dummy.hpp"
#include "../algs/owb.hpp"
#include "../algs/tl2.hpp"
#include "../algs/norec.hpp"
#include "../algs/undolog_invis.hpp"
#include "../algs/undolog_vis.hpp"
#include "../algs/oul_speculative.hpp"
#include "../algs/oul_steal.hpp"
#include "../algs/stmlite.hpp"
#include "../algs/stmlite-unordered.hpp"

#define ARGS_SIZE					1024
#define ARGS_MASK					1023
#define MAX_PENDING_PER_THREAD		2
#define MAX_ABORT_RATIO		30

static pthread_key_t tx_key;
static pthread_barrier_t   finish_barrier;
static pthread_mutex_t lock;
static pthread_key_t history_key;

#ifdef ENABLE_STATISTICS
static atomic_int transactions_count(0);
static atomic_int executions_count(0);
#else
static int transactions_count = 0;
static int executions_count = 0;
#endif
static int workers_count = -1;

#

static int cores_counter = 0;

// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Dummy ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ //

namespace o_api_dummy{

	int get_workers_count(){
		return workers_count;
	}

	dummy::Transaction* getTx(){
		return new dummy::Transaction();
	}

	void init(int workers){
		INIT_LOG
		workers_count = workers;
		pthread_mutex_init(&lock, NULL);
		pthread_key_create(&tx_key, NULL);
		pthread_barrier_init (&finish_barrier, NULL, workers);
	}

	void init_thread(){
		INIT_THREAD_LOG
		stick_worker(__sync_fetch_and_add(&cores_counter, 1));
	}

	void run_in_order(void (func) (void**), void** args, int age){
		transactions_count++;
		func(args);
	}

	void wait_till_finish(){
		pthread_barrier_wait (&finish_barrier);
		DUMP_LOG
	}

	void print_statistics(){
		cout << "===================================" << endl;
		cout << "Algorithm =\t" << "Dummy" << endl;
		cout << "Workers =\t" << workers_count << endl;
		cout << "Transactions =\t" << transactions_count << endl;
		cout << "===================================" << endl;
	}

};

// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Lock ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ //

namespace o_api_lock{

	int get_workers_count(){
		return workers_count;
	}

	commons::AbstractTransaction* getTx(){
		return new dummy::Transaction();
	}

	void init(int workers){
		INIT_LOG
		workers_count = workers;
		pthread_mutex_init(&lock, NULL);
		pthread_key_create(&tx_key, NULL);
		pthread_barrier_init (&finish_barrier, NULL, workers);
	}

	void init_thread(){
		INIT_THREAD_LOG
		stick_worker(__sync_fetch_and_add(&cores_counter, 1));
	}

	void cs_start(){
		pthread_mutex_lock(&lock);
	}

	void cs_end(){
		pthread_mutex_unlock(&lock);
	}

	void run_in_order(void (func) (void**), void** args, int age){
		cs_start();
		transactions_count++;
		func(args);
		cs_end();
	}

	void wait_till_finish(){
		pthread_barrier_wait (&finish_barrier);
		DUMP_LOG
	}

	void print_statistics(){
		cout << "===================================" << endl;
		cout << "Algorithm =\t" << "Lock" << endl;
		cout << "Workers =\t" << workers_count << endl;
		cout << "Transactions =\t" << transactions_count << endl;
		cout << "===================================" << endl;
	}

};

// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ TL2 ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ //

namespace o_api_tl2{

	int get_workers_count(){
		return workers_count;
	}

	commons::AbstractTransaction* getTx(){
		return (commons::AbstractTransaction*)pthread_getspecific(tx_key);
	}

	void init(int workers){
		INIT_LOG
		workers_count = workers;
		pthread_mutex_init(&lock, NULL);
		pthread_key_create(&tx_key, NULL);
		pthread_barrier_init (&finish_barrier, NULL, workers);
#ifdef DEBUG_TL2
		pthread_key_create(&history_key, NULL);
#endif
	}

	void init_thread(){
		INIT_THREAD_LOG
		stick_worker(__sync_fetch_and_add(&cores_counter, 1));
#ifdef DEBUG_TL2
		UnsafeList<AbstractTransaction*> *localHistory = new UnsafeList<AbstractTransaction*>();
		pthread_setspecific(history_key, (void*)localHistory);
#endif
	}

	void run_in_order(void (func) (void**), void** args, int age){
		transactions_count++;
		jmp_buf _jmpbuf;
		int abort_flags = setjmp(_jmpbuf);
		executions_count++;
		tl2::Transaction* tx = new tl2::Transaction(age);
		pthread_setspecific(tx_key, (void*)tx);
#ifdef DEBUG_TL2
		((UnsafeList<AbstractTransaction*>*)pthread_getspecific(history_key))->add(tx);
#endif
		tx->start(&_jmpbuf);
		func(args);
		tx->commit();
	}

	void wait_till_finish(){
		pthread_barrier_wait (&finish_barrier);
#ifdef DEBUG_TL2
		ListNode<AbstractTransaction*>* itr = ((UnsafeList<AbstractTransaction*>*)pthread_getspecific(history_key))->header;
		while (itr != NULL) {
			((tl2::Transaction*)itr->value)->dump();
			itr = itr->next;
		}
#endif
		DUMP_LOG
	}

	void print_statistics(){
		cout << "===================================" << endl;
		cout << "Algorithm =\t" << "TL2" << endl;
		cout << "Workers =\t" << workers_count << endl;
		cout << "Transactions =\t" << transactions_count << endl;
		cout << "Aborts =\t" << (executions_count - transactions_count) << endl;
		cout << "===================================" << endl;
	}
};

namespace o_api_otl2{

	static volatile int last_committed;

	int get_workers_count(){
		return workers_count;
	}

	commons::AbstractTransaction* getTx(){
		return (commons::AbstractTransaction*)pthread_getspecific(tx_key);
	}

	void init(int workers){
		INIT_LOG
		workers_count = workers;
		pthread_mutex_init(&lock, NULL);
		pthread_key_create(&tx_key, NULL);
		pthread_barrier_init (&finish_barrier, NULL, workers);
		last_committed=0;
#ifdef DEBUG_TL2
		pthread_key_create(&history_key, NULL);
#endif
	}

	void init_thread(){
		INIT_THREAD_LOG
		stick_worker(__sync_fetch_and_add(&cores_counter, 1));
#ifdef DEBUG_TL2
		UnsafeList<AbstractTransaction*> *localHistory = new UnsafeList<AbstractTransaction*>();
		pthread_setspecific(history_key, (void*)localHistory);
#endif
	}

	void run_in_order(void (func) (void**), void** args, int age){
		transactions_count++;
		jmp_buf _jmpbuf;
		int abort_flags = setjmp(_jmpbuf);
		executions_count++;
		tl2::Transaction* tx = new tl2::Transaction(age);
		pthread_setspecific(tx_key, (void*)tx);
#ifdef DEBUG_TL2
		((UnsafeList<AbstractTransaction*>*)pthread_getspecific(history_key))->add(tx);
#endif
		tx->start(&_jmpbuf);
		func(args);
		while(last_committed != age);
		tx->commit();
		last_committed++;
	}

	void wait_till_finish(){
		pthread_barrier_wait (&finish_barrier);
		last_committed = 0;
#ifdef DEBUG_TL2
		ListNode<AbstractTransaction*>* itr = ((UnsafeList<AbstractTransaction*>*)pthread_getspecific(history_key))->header;
		while (itr != NULL) {
			((tl2::Transaction*)itr->value)->dump();
			itr = itr->next;
		}
#endif
		DUMP_LOG
	}

	void print_statistics(){
		cout << "===================================" << endl;
		cout << "Algorithm =\t" << "OTL2" << endl;
		cout << "Workers =\t" << workers_count << endl;
		cout << "Transactions =\t" << transactions_count << endl;
		cout << "Aborts =\t" << (executions_count - transactions_count) << endl;
		cout << "===================================" << endl;
	}
};

// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ NOrec ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ //

namespace o_api_norec{

	int get_workers_count(){
		return workers_count;
	}

	commons::AbstractTransaction* getTx(){
		return (commons::AbstractTransaction*)pthread_getspecific(tx_key);
	}

	void init(int workers){
		INIT_LOG
		workers_count = workers;
		pthread_mutex_init(&lock, NULL);
		pthread_key_create(&tx_key, NULL);
		pthread_barrier_init (&finish_barrier, NULL, workers);
	}

	void init_thread(){
		INIT_THREAD_LOG
		stick_worker(__sync_fetch_and_add(&cores_counter, 1));
	}

	void run_in_order(void (func) (void**), void** args, int age){
		transactions_count++;
		jmp_buf _jmpbuf;
		int abort_flags = setjmp(_jmpbuf);
		executions_count++;
		norec::Transaction* tx = new norec::Transaction();
		pthread_setspecific(tx_key, (void*)tx);
		tx->start(&_jmpbuf);
		func(args);
		tx->commit();
	}

	void wait_till_finish(){
		pthread_barrier_wait (&finish_barrier);
		DUMP_LOG
	}

	void print_statistics(){
		cout << "===================================" << endl;
		cout << "Algorithm =\t" << "NOrec" << endl;
		cout << "Workers =\t" << workers_count << endl;
		cout << "Transactions =\t" << transactions_count << endl;
		cout << "Aborts =\t" << (executions_count - transactions_count) << endl;
		cout << "===================================" << endl;
	}
};

namespace o_api_onorec{

	static volatile int last_committed;

	int get_workers_count(){
		return workers_count;
	}

	commons::AbstractTransaction* getTx(){
		return (commons::AbstractTransaction*)pthread_getspecific(tx_key);
	}

	void init(int workers){
		INIT_LOG
		workers_count = workers;
		pthread_mutex_init(&lock, NULL);
		pthread_key_create(&tx_key, NULL);
		pthread_barrier_init (&finish_barrier, NULL, workers);
		last_committed=0;
	}

	void init_thread(){
		INIT_THREAD_LOG
		stick_worker(__sync_fetch_and_add(&cores_counter, 1));
	}

	void run_in_order(void (func) (void**), void** args, int age){
		transactions_count++;
		jmp_buf _jmpbuf;
		int abort_flags = setjmp(_jmpbuf);
		executions_count++;
		norec::Transaction* tx = new norec::Transaction();
		pthread_setspecific(tx_key, (void*)tx);
		tx->start(&_jmpbuf);
		func(args);
		while(last_committed != age);
		tx->commit();
		last_committed++;
	}

	void wait_till_finish(){
		pthread_barrier_wait (&finish_barrier);
		last_committed = 0;
		DUMP_LOG
	}

	void print_statistics(){
		cout << "===================================" << endl;
		cout << "Algorithm =\t" << "ONOrec" << endl;
		cout << "Workers =\t" << workers_count << endl;
		cout << "Transactions =\t" << transactions_count << endl;
		cout << "Aborts =\t" << (executions_count - transactions_count) << endl;
		cout << "===================================" << endl;
	}
};

// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ UndoLog Invisible ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ //

namespace o_api_undolog_invis{

	int get_workers_count(){
		return workers_count;
	}

	commons::AbstractTransaction* getTx(){
		return (commons::AbstractTransaction*)pthread_getspecific(tx_key);
	}

	void init(int workers){
		INIT_LOG
		workers_count = workers;
		pthread_mutex_init(&lock, NULL);
		pthread_key_create(&tx_key, NULL);
		pthread_barrier_init (&finish_barrier, NULL, workers);
	}

	void init_thread(){
		INIT_THREAD_LOG
		stick_worker(__sync_fetch_and_add(&cores_counter, 1));
	}

	void run_in_order(void (func) (void**), void** args, int age){
		transactions_count++;
		jmp_buf _jmpbuf;
		int abort_flags = setjmp(_jmpbuf);
		executions_count++;
		undolog_invis::Transaction* tx = new undolog_invis::Transaction(age);
		pthread_setspecific(tx_key, (void*)tx);
		tx->start(&_jmpbuf);
		func(args);
		tx->commit();
	}

	void wait_till_finish(){
		pthread_barrier_wait (&finish_barrier);
		DUMP_LOG
	}

	void print_statistics(){
		cout << "===================================" << endl;
		cout << "Algorithm =\t" << "UndoLog-Invisible" << endl;
		cout << "Workers =\t" << workers_count << endl;
		cout << "Transactions =\t" << transactions_count << endl;
		cout << "Aborts =\t" << (executions_count - transactions_count) << endl;
		cout << "===================================" << endl;
	}
};

namespace o_api_undolog_invis_ordered{

	static volatile int last_committed;

	int get_workers_count(){
		return workers_count;
	}

	commons::AbstractTransaction* getTx(){
		return (commons::AbstractTransaction*)pthread_getspecific(tx_key);
	}

	void init(int workers){
		INIT_LOG
		workers_count = workers;
		pthread_mutex_init(&lock, NULL);
		pthread_key_create(&tx_key, NULL);
		pthread_barrier_init (&finish_barrier, NULL, workers);
		last_committed=0;
	}

	void init_thread(){
		INIT_THREAD_LOG
		stick_worker(__sync_fetch_and_add(&cores_counter, 1));
	}

	void run_in_order(void (func) (void**), void** args, int age){
		transactions_count++;
		jmp_buf _jmpbuf;
		int abort_flags = setjmp(_jmpbuf);
		executions_count++;
		undolog_invis::Transaction* tx = new undolog_invis::Transaction(age);
		pthread_setspecific(tx_key, (void*)tx);
		tx->start(&_jmpbuf);
		func(args);
		for(int i=0; i<TIMEOUT_VALUE; i++)
			if(last_committed == age){
				tx->commit();
				last_committed++;
				return;
			}
		tx->suicide(0);
	}

	void wait_till_finish(){
		pthread_barrier_wait (&finish_barrier);
		last_committed = 0;
		DUMP_LOG
	}

	void print_statistics(){
		cout << "===================================" << endl;
		cout << "Algorithm =\t" << "Ordered UndoLog-Invisible" << endl;
		cout << "Workers =\t" << workers_count << endl;
		cout << "Transactions =\t" << transactions_count << endl;
		cout << "Aborts =\t" << (executions_count - transactions_count) << endl;
		cout << "===================================" << endl;
	}
};

// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ UndoLog Visible ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ //

namespace o_api_undolog_vis{

	int get_workers_count(){
		return workers_count;
	}

	undolog_vis::Transaction* getTx(){
		return (undolog_vis::Transaction*)pthread_getspecific(tx_key);
	}

	void init(int workers){
		INIT_LOG
		workers_count = workers;
		pthread_mutex_init(&lock, NULL);
		pthread_key_create(&tx_key, NULL);
		pthread_barrier_init (&finish_barrier, NULL, workers);
		undolog_vis::init();
#ifdef DEBUG_UNDOLOG_VIS
		pthread_key_create(&history_key, NULL);
#endif
	}

	void init_thread(){
		INIT_THREAD_LOG
		stick_worker(__sync_fetch_and_add(&cores_counter, 1));
#ifdef DEBUG_UNDOLOG_VIS
		UnsafeList<undolog_vis::Transaction*> *localHistory = new UnsafeList<undolog_vis::Transaction*>();
		pthread_setspecific(history_key, (void*)localHistory);
#endif
	}

	void run_in_order(void (func) (void**), void** args, int age){
		transactions_count++;
		jmp_buf _jmpbuf;
		int abort_flags = setjmp(_jmpbuf);
		executions_count++;
		undolog_vis::Transaction* tx = new undolog_vis::Transaction(age);
		pthread_setspecific(tx_key, (void*)tx);
#ifdef DEBUG_UNDOLOG_VIS
		((UnsafeList<undolog_vis::Transaction*>*)pthread_getspecific(history_key))->add(tx);
#endif
		tx->start(&_jmpbuf);
		func(args);
		tx->commit();
		tx->complete();
	}

	void wait_till_finish(){
		pthread_barrier_wait (&finish_barrier);
#ifdef DEBUG_UNDOLOG_VIS
		ListNode<undolog_vis::Transaction*>* itr = ((UnsafeList<undolog_vis::Transaction*>*)pthread_getspecific(history_key))->header;
		while (itr != NULL) {
			((undolog_vis::Transaction*)itr->value)->dump();
			itr = itr->next;
		}
#endif
		DUMP_LOG
	}

	void print_statistics(){
		cout << "===================================" << endl;
		cout << "Algorithm =\t" << "UndoLog-Visible" << endl;
		cout << "Workers =\t" << workers_count << endl;
		cout << "Transactions =\t" << transactions_count << endl;
		cout << "Aborts =\t" << (executions_count - transactions_count) << endl;
		cout << "===================================" << endl;
	}
};

namespace o_api_undolog_vis_ordered{

	static volatile int last_committed;

	int get_workers_count(){
		return workers_count;
	}

	undolog_vis::Transaction* getTx(){
		return (undolog_vis::Transaction*)pthread_getspecific(tx_key);
	}

	void init(int workers){
		INIT_LOG
		workers_count = workers;
		pthread_mutex_init(&lock, NULL);
		pthread_key_create(&tx_key, NULL);
		pthread_barrier_init (&finish_barrier, NULL, workers);
		undolog_vis::init();
	#ifdef DEBUG_UNDOLOG_VIS
		pthread_key_create(&history_key, NULL);
	#endif
		last_committed=0;
	}

	void init_thread(){
		INIT_THREAD_LOG
		stick_worker(__sync_fetch_and_add(&cores_counter, 1));
	#ifdef DEBUG_UNDOLOG_VIS
		UnsafeList<undolog_vis::Transaction*> *localHistory = new UnsafeList<undolog_vis::Transaction*>();
		pthread_setspecific(history_key, (void*)localHistory);
	#endif
	}

	void run_in_order(void (func) (void**), void** args, int age){
		transactions_count++;
		jmp_buf _jmpbuf;
		int abort_flags = setjmp(_jmpbuf);
		executions_count++;
		undolog_vis::Transaction* tx = new undolog_vis::Transaction(age);
		pthread_setspecific(tx_key, (void*)tx);
	#ifdef DEBUG_UNDOLOG_VIS
		((UnsafeList<undolog_vis::Transaction*>*)pthread_getspecific(history_key))->add(tx);
	#endif
		tx->start(&_jmpbuf);
		func(args);
		tx->commit();
		for(int i=0; i<TIMEOUT_VALUE; i++)
			if(last_committed == age){
				tx->complete();
				last_committed++;
				return;
			}
		tx->suicide(TIMEOUT);
	}

	void wait_till_finish(){
		pthread_barrier_wait (&finish_barrier);
	#ifdef DEBUG_UNDOLOG_VIS
		ListNode<undolog_vis::Transaction*>* itr = ((UnsafeList<undolog_vis::Transaction*>*)pthread_getspecific(history_key))->header;
		while (itr != NULL) {
			((undolog_vis::Transaction*)itr->value)->dump();
			itr = itr->next;
		}
	#endif
		last_committed = 0;
		DUMP_LOG
	}

	void print_statistics(){
		cout << "===================================" << endl;
		cout << "Algorithm =\t" << "Ordered UndoLog-Visible" << endl;
		cout << "Workers =\t" << workers_count << endl;
		cout << "Transactions =\t" << transactions_count << endl;
		cout << "Aborts =\t" << (executions_count - transactions_count) << endl;
		cout << "===================================" << endl;
	}
};

// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ STMLite ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ //

namespace o_api_stmlite{

	stmlite::TCM *manager;
	static pthread_key_t tid_key;
	static int thread_counter;

	int get_workers_count(){
		return workers_count;
	}

	commons::AbstractTransaction* getTx(){
		return (commons::AbstractTransaction*)pthread_getspecific(tx_key);
	}

	void* run_tcm(void* arg){
		INIT_THREAD_LOG
		stick_worker(0);
		while(true){
			manager->validatePreCommitted();
			manager->cleanCommitedLog();
		}
		DUMP_LOG
		return 0;
	}

	void init(int workers){
		INIT_LOG
		workers_count = workers;
		pthread_mutex_init(&lock, NULL);
		pthread_key_create(&tx_key, NULL);
		pthread_barrier_init (&finish_barrier, NULL, workers);
		pthread_key_create(&tid_key, NULL);
		manager = new stmlite::TCM(workers);
		pthread_t tcm_worker;
		pthread_create(&tcm_worker, NULL, run_tcm, NULL);
	}

	void init_thread(){
		INIT_THREAD_LOG
		int tid = __sync_fetch_and_add(&cores_counter, 1);
		stick_worker(tid+1);
		pthread_setspecific(tid_key, (void*)tid);
	}

	void run_in_order(void (func) (void**), void** args, int age){
		int tid = (long)pthread_getspecific(tid_key);
		transactions_count++;
		jmp_buf _jmpbuf;
		int abort_flags = setjmp(_jmpbuf);
		executions_count++;
		stmlite::Transaction* tx = new stmlite::Transaction(age);
		pthread_setspecific(tx_key, (void*)tx);
		tx->start(&_jmpbuf);
		manager->started(tx, tid);
		func(args);
		manager->finished(tx, tid);
		while(!tx->commit_flag){
			if(tx->abort_flag)
				tx->abort();
//			pthread_yield();
		}
		tx->commit();
		manager->committed(tx, tid);
	}

	void wait_till_finish(){
		pthread_barrier_wait (&finish_barrier);
		int tid = (long)pthread_getspecific(tid_key);
		if(tid==0)	// only single thread do that
			manager->clean();
		DUMP_LOG
	}

	void print_statistics(){
		cout << "===================================" << endl;
		cout << "Algorithm =\t" << "STMLite" << endl;
		cout << "Workers =\t" << workers_count << endl;
		cout << "Transactions =\t" << transactions_count << endl;
		cout << "Aborts =\t" << (executions_count - transactions_count) << endl;
		cout << "===================================" << endl;
	}

};

// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ OWB ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ //

namespace o_api_owb{

	static owb::Transaction** committedTxs;
	int pending_queue_length;
	int pending_mask;
	int max_pending;
	void (*function) (void**);
	void*** arguments;

	int get_workers_count(){
		return workers_count;
	}

	owb::Transaction* getTx(){
		return (owb::Transaction*)pthread_getspecific(tx_key);
	}

	void init(int workers){
		INIT_LOG
		workers_count = workers;
		pthread_mutex_init(&lock, NULL);
		pthread_key_create(&tx_key, NULL);
		pthread_barrier_init (&finish_barrier, NULL, workers);
		owb::last_completed = -1;
		owb::validation = 0;
		max_pending = workers * MAX_PENDING_PER_THREAD;
		pending_queue_length = 1024;
		while(pending_queue_length < max_pending)
			pending_queue_length *= 2;
		pending_mask = pending_queue_length - 1;
		arguments = new void**[ARGS_SIZE];
		committedTxs = new owb::Transaction*[pending_queue_length];
		for(int i=0; i<pending_queue_length; i++){
			committedTxs[i] = NULL;
		}
	}

	void init_thread(){
		INIT_THREAD_LOG
		stick_worker(__sync_fetch_and_add(&cores_counter, 1));
	}

	long restartTx(int age){
		long retries = 0;
		jmp_buf _jmpbuf;
		int abort_flags = setjmp(_jmpbuf);
		retries++;
		owb::Transaction* tx = new owb::Transaction(age);
		pthread_setspecific(tx_key, (void*)tx);
		LOG("restart " << age);
		tx->start(&_jmpbuf);
		function(arguments[age & ARGS_MASK]);
		tx->commit();
		if(tx->complete()){
//			tx->clean();
		}
		LOG("restarted " << age);
		return retries;
	}

	int try_validation(){
		if(
			owb::validation == 0 &&
			__sync_bool_compare_and_swap(&owb::validation, 0, 1)
		)
		{ // try to be the validator
			int temp = owb::last_completed;
			int retries = 0;
			int j=owb::last_completed+1;
			while(true){
				int i = j & pending_mask;
				if(committedTxs[i] == NULL){
					break;
				}
				LOG("+++++++++ " << j << " +++++++++ ");
				if(committedTxs[i]->complete()){
//					committedTxs[i]->clean();
				}else
					retries += restartTx(committedTxs[i]->getAge());
				committedTxs[i] = NULL;
				owb::last_completed++;
				j++;
			}
			owb::validation = 0;	// reset the flag
			return retries;
		}
		return 0;
	}

	void run_in_order(void (func) (void**), void** args, int age){
		executions_count += try_validation();
		function = func;
		transactions_count++;
		jmp_buf _jmpbuf;
		int abort_flags = setjmp(_jmpbuf);
		executions_count++;
		owb::Transaction* tx = new owb::Transaction(age);
		pthread_setspecific(tx_key, (void*)tx);
		tx->start(&_jmpbuf);
		while(age - owb::last_completed > max_pending);
		func(args);
		tx->commit();
		tx->noRetry();
		arguments[age & ARGS_MASK] = args;
		committedTxs[age & pending_mask] = tx;
	}

	void wait_till_finish(){
		executions_count += try_validation();
		pthread_barrier_wait (&finish_barrier);
		if(owb::last_completed > 0){
			owb::last_completed = -1;
			owb::validation = 0;
			for(int i=0; i<pending_queue_length; i++){
				committedTxs[i] = NULL;
			}
		}
		DUMP_LOG
	}

	void print_statistics(){
		cout << "===================================" << endl;
		cout << "Algorithm =\t" << "OWB" << endl;
		cout << "Workers =\t" << workers_count << endl;
		cout << "Transactions =\t" << transactions_count << endl;
		cout << "Aborts =\t" << (executions_count - transactions_count) << endl;
		cout << "===================================" << endl;
	}
};


namespace o_api_owb_plus{

	static owb::Transaction** committedTxs;
	int pending_queue_length;
	int pending_mask;
	int max_pending;
	int min_pending;
	int hold_on;
	void (*function) (void**);
	void*** arguments;

	int get_workers_count(){
		return workers_count;
	}

	owb::Transaction* getTx(){
		return (owb::Transaction*)pthread_getspecific(tx_key);
	}

	void init(int workers){
		INIT_LOG
		workers_count = workers;
		pthread_mutex_init(&lock, NULL);
		pthread_key_create(&tx_key, NULL);
		pthread_barrier_init (&finish_barrier, NULL, workers);
		owb::last_completed = -1;
		owb::validation = 0;
		hold_on = 0;
		max_pending = workers * MAX_PENDING_PER_THREAD;
		min_pending = 2;
		pending_queue_length = 1024;
		while(pending_queue_length < max_pending)
			pending_queue_length *= 2;
		pending_mask = pending_queue_length - 1;
		arguments = new void**[ARGS_SIZE];
		committedTxs = new owb::Transaction*[pending_queue_length];
		for(int i=0; i<pending_queue_length; i++){
			committedTxs[i] = NULL;
		}
	}

	void init_thread(){
		INIT_THREAD_LOG
		stick_worker(__sync_fetch_and_add(&cores_counter, 1));
	}

	long restartTx(int age){
		long retries = 0;
		jmp_buf _jmpbuf;
		int abort_flags = setjmp(_jmpbuf);
		retries++;
		owb::Transaction* tx = new owb::Transaction(age);
		pthread_setspecific(tx_key, (void*)tx);
		LOG("restart " << age);
		tx->start(&_jmpbuf);
		function(arguments[age & ARGS_MASK]);
		tx->commit();
		if(tx->complete()){
//			tx->clean();
		}
		LOG("restarted " << age);
		return retries;
	}

	int try_validation(){
		if(
			owb::validation == 0 &&
			__sync_bool_compare_and_swap(&owb::validation, 0, 1)
		)
		{ // try to be the validator
			int temp = owb::last_completed;
			int retries = 0;
			int j=owb::last_completed+1;
			while(true){
				int i = j & pending_mask;
				if(committedTxs[i] == NULL){
					break;
				}
				LOG("+++++++++ " << j << " +++++++++ ");
				if(committedTxs[i]->complete()){
//					committedTxs[i]->clean();
				}else
					retries += restartTx(committedTxs[i]->getAge());
				committedTxs[i] = NULL;
				owb::last_completed++;
				j++;
			}
			owb::validation = 0;	// reset the flag
			return retries;
		}
		return 0;
	}

	void run_in_order(void (func) (void**), void** args, int age){
		executions_count += try_validation();
		function = func;
		transactions_count++;
		jmp_buf _jmpbuf;
		int abort_flags = setjmp(_jmpbuf);
		executions_count++;
		owb::Transaction* tx = new owb::Transaction(age);
		pthread_setspecific(tx_key, (void*)tx);
		tx->start(&_jmpbuf);
		if(age - owb::last_completed > max_pending){
        	hold_on++;
			while(age - owb::last_completed > min_pending);	// freeze execution
		}
		func(args);
		tx->commit();
		tx->noRetry();
		arguments[age & ARGS_MASK] = args;
		committedTxs[age & pending_mask] = tx;
	}

	void wait_till_finish(){
		executions_count += try_validation();
		pthread_barrier_wait (&finish_barrier);
		if(owb::last_completed > 0){
			owb::last_completed = -1;
			owb::validation = 0;
			for(int i=0; i<pending_queue_length; i++){
				committedTxs[i] = NULL;
			}
		}
		DUMP_LOG
	}

	void print_statistics(){
		cout << "===================================" << endl;
		cout << "Algorithm =\t" << "OWB+" << endl;
		cout << "Workers =\t" << workers_count << endl;
		cout << "Transactions =\t" << transactions_count << endl;
		cout << "Aborts =\t" << (executions_count - transactions_count) << endl;
		cout << "Holdons =\t" << hold_on << endl;
		cout << "===================================" << endl;
	}
};


namespace o_api_owb_plus_plus{

	static owb::Transaction** committedTxs;
	int pending_queue_length;
	int pending_mask;
	int min_pending;
	int hold_on;
	long aborts;
	void (*function) (void**);
	void*** arguments;

	int get_workers_count(){
		return workers_count;
	}

	owb::Transaction* getTx(){
		return (owb::Transaction*)pthread_getspecific(tx_key);
	}

	void init(int workers){
		INIT_LOG
		workers_count = workers;
		pthread_mutex_init(&lock, NULL);
		pthread_key_create(&tx_key, NULL);
		pthread_barrier_init (&finish_barrier, NULL, workers);
		owb::last_completed = -1;
		owb::validation = 0;
		hold_on = 0;
		min_pending = workers;
		aborts = 0;
		pending_queue_length = 1024;
		while(pending_queue_length < workers * MAX_PENDING_PER_THREAD)
			pending_queue_length *= 2;
		pending_mask = pending_queue_length - 1;
		arguments = new void**[ARGS_SIZE];
		committedTxs = new owb::Transaction*[pending_queue_length];
		for(int i=0; i<pending_queue_length; i++){
			committedTxs[i] = NULL;
		}
	}

	void init_thread(){
		INIT_THREAD_LOG
		stick_worker(__sync_fetch_and_add(&cores_counter, 1));
	}

	long restartTx(int age){
		long retries = 0;
		jmp_buf _jmpbuf;
		int abort_flags = setjmp(_jmpbuf);
		retries++;
		owb::Transaction* tx = new owb::Transaction(age);
		pthread_setspecific(tx_key, (void*)tx);
		LOG("restart " << age);
		tx->start(&_jmpbuf);
		function(arguments[age & ARGS_MASK]);
		tx->commit();
		if(tx->complete()){
//			tx->clean();
		}
		LOG("restarted " << age);
		return retries;
	}

	int try_validation(){
		if(
			owb::validation == 0 &&
			__sync_bool_compare_and_swap(&owb::validation, 0, 1)
		)
		{ // try to be the validator
			int temp = owb::last_completed;
			int retries = 0;
			int j=owb::last_completed+1;
			while(true){
				int i = j & pending_mask;
				if(committedTxs[i] == NULL){
					break;
				}
				LOG("+++++++++ " << j << " +++++++++ ");
				if(committedTxs[i]->complete()){
//					committedTxs[i]->clean();
				}else{
					long r = restartTx(committedTxs[i]->getAge());
					retries += r;
					aborts += r - 1;
				}
				committedTxs[i] = NULL;
				owb::last_completed++;
				j++;
			}
			owb::validation = 0;	// reset the flag
			return retries;
		}
		return 0;
	}

	void run_in_order(void (func) (void**), void** args, int age){
		executions_count += try_validation();
		 if(aborts * MAX_ABORT_RATIO > age || age - owb::last_completed > pending_queue_length){
			hold_on++;
			while(age - owb::last_completed > min_pending) try_validation(); // freeze execution
		}
		function = func;
		transactions_count++;
		int temp = executions_count;
		jmp_buf _jmpbuf;
		int abort_flags = setjmp(_jmpbuf);
		executions_count++;
		owb::Transaction* tx = new owb::Transaction(age);
		pthread_setspecific(tx_key, (void*)tx);
		tx->start(&_jmpbuf);
		func(args);
		tx->commit();
		tx->noRetry();
		arguments[age & ARGS_MASK] = args;
		committedTxs[age & pending_mask] = tx;
		aborts += executions_count - temp - 1;
	}

	void wait_till_finish(){
		executions_count += try_validation();
		pthread_barrier_wait (&finish_barrier);
		if(owb::last_completed > 0){
			owb::last_completed = -1;
			owb::validation = 0;
			for(int i=0; i<pending_queue_length; i++){
				committedTxs[i] = NULL;
			}
		}
		DUMP_LOG
	}

	void print_statistics(){
		cout << "===================================" << endl;
		cout << "Algorithm =\t" << "OWB++" << endl;
		cout << "Workers =\t" << workers_count << endl;
		cout << "Transactions =\t" << transactions_count << endl;
		cout << "Aborts =\t" << (executions_count - transactions_count) << endl;
		cout << "Holdons =\t" << hold_on << endl;
		cout << "===================================" << endl;
	}
};
// ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ OUL ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ //

namespace o_api_oul_speculative{

	static oul_speculative::Transaction** committedTxs;
	int pending_queue_length;
	int pending_mask;
	int max_pending;
	void (*function) (void**);
	void*** arguments;

	int get_workers_count(){
		return workers_count;
	}

	oul_speculative::Transaction* getTx(){
		return (oul_speculative::Transaction*)pthread_getspecific(tx_key);
	}

	void init(int workers){
		INIT_LOG
		workers_count = workers;
		pthread_mutex_init(&lock, NULL);
		pthread_key_create(&tx_key, NULL);
		pthread_barrier_init (&finish_barrier, NULL, workers);
		oul_speculative::init();
		oul_speculative::last_completed = -1;
		oul_speculative::validation = 0;
		max_pending = workers * MAX_PENDING_PER_THREAD;
		pending_queue_length = 1024;
		while(pending_queue_length < max_pending)
			pending_queue_length *= 2;
		pending_mask = pending_queue_length - 1;
		arguments = new void**[ARGS_SIZE];
		committedTxs = new oul_speculative::Transaction*[pending_queue_length];
		for(int i=0; i<pending_queue_length; i++){
			committedTxs[i] = NULL;
		}
	}

	void init_thread(){
		INIT_THREAD_LOG
		stick_worker(__sync_fetch_and_add(&cores_counter, 1));
	}

	long restartTx(int age){
		long retries = 0;
		jmp_buf _jmpbuf;
		int abort_flags = setjmp(_jmpbuf);
		retries++;
		oul_speculative::Transaction* tx = new oul_speculative::Transaction(age);
		pthread_setspecific(tx_key, (void*)tx);
		LOG("restart " << age);
		tx->start(&_jmpbuf);
		function(arguments[age & ARGS_MASK]);
		tx->commit();
		if(tx->complete()){
//			tx->clean();
		}
		LOG("restarted " << age);
		return retries;
	}

	int try_validation(){
		if(
			oul_speculative::validation == 0 &&
			__sync_bool_compare_and_swap(&oul_speculative::validation, 0, 1)
		)
		{ // try to be the validator
			int temp = oul_speculative::last_completed;
			int retries = 0;
			int j=oul_speculative::last_completed+1;
			while(true){
				int i = j & pending_mask;
				if(committedTxs[i] == NULL){
					break;
				}
				LOG("+++++++++ " << j << " +++++++++ ");
				if(committedTxs[i]->complete()){
//					committedTxs[i]->clean();
				}else
					retries += restartTx(committedTxs[i]->getAge());
				committedTxs[i] = NULL;
				oul_speculative::last_completed++;
				j++;
			}
			oul_speculative::validation = 0;	// reset the flag
			return retries;
		}
		return 0;
	}

	void run_in_order(void (func) (void**), void** args, int age){
		executions_count += try_validation();
		while(age - oul_speculative::last_completed > max_pending) try_validation();
		function = func;
		transactions_count++;
		jmp_buf _jmpbuf;
		int abort_flags = setjmp(_jmpbuf);
		executions_count++;
		oul_speculative::Transaction* tx = new oul_speculative::Transaction(age);
		pthread_setspecific(tx_key, (void*)tx);
		tx->start(&_jmpbuf);
		func(args);
		tx->commit();
		tx->noRetry();
		arguments[age & ARGS_MASK] = args;
		committedTxs[age & pending_mask] = tx;
	}

	void wait_till_finish(){
		executions_count += try_validation();
		pthread_barrier_wait (&finish_barrier);
		if(oul_speculative::last_completed > 0){
			oul_speculative::last_completed = -1;
			oul_speculative::validation = 0;
			for(int i=0; i<pending_queue_length; i++){
				committedTxs[i] = NULL;
			}
		}
		DUMP_LOG
	}

	void print_statistics(){
		cout << "===================================" << endl;
		cout << "Algorithm =\t" << "OUL Speculative" << endl;
		cout << "Workers =\t" << workers_count << endl;
		cout << "Transactions =\t" << transactions_count << endl;
		cout << "Aborts =\t" << (executions_count - transactions_count) << endl;
		cout << "===================================" << endl;
	}
};


namespace o_api_oul_speculative_plus{

	static oul_speculative::Transaction** committedTxs;
	int pending_queue_length;
	int pending_mask;
	int max_pending;
	int min_pending;
	int hold_on;
	void (*function) (void**);
	void*** arguments;

	int get_workers_count(){
		return workers_count;
	}

	oul_speculative::Transaction* getTx(){
		return (oul_speculative::Transaction*)pthread_getspecific(tx_key);
	}

	void init(int workers){
		INIT_LOG
		workers_count = workers;
		pthread_mutex_init(&lock, NULL);
		pthread_key_create(&tx_key, NULL);
		pthread_barrier_init (&finish_barrier, NULL, workers);
		oul_speculative::init();
		oul_speculative::last_completed = -1;
		oul_speculative::validation = 0;
		hold_on = 0;
		max_pending = workers * MAX_PENDING_PER_THREAD;
		min_pending = workers;
		pending_queue_length = 1024;
		while(pending_queue_length < max_pending)
			pending_queue_length *= 2;
		pending_mask = pending_queue_length - 1;
		arguments = new void**[ARGS_SIZE];
		committedTxs = new oul_speculative::Transaction*[pending_queue_length];
		for(int i=0; i<pending_queue_length; i++){
			committedTxs[i] = NULL;
		}
	}

	void init_thread(){
		INIT_THREAD_LOG
		stick_worker(__sync_fetch_and_add(&cores_counter, 1));
	}

	long restartTx(int age){
		long retries = 0;
		jmp_buf _jmpbuf;
		int abort_flags = setjmp(_jmpbuf);
		retries++;
		oul_speculative::Transaction* tx = new oul_speculative::Transaction(age);
		pthread_setspecific(tx_key, (void*)tx);
		LOG("restart " << age);
		tx->start(&_jmpbuf);
		function(arguments[age & ARGS_MASK]);
		tx->commit();
		if(tx->complete()){
//			tx->clean();
		}
		LOG("restarted " << age);
		return retries;
	}

	int try_validation(){
		if(
			oul_speculative::validation == 0 &&
			__sync_bool_compare_and_swap(&oul_speculative::validation, 0, 1)
		)
		{ // try to be the validator
			int temp = oul_speculative::last_completed;
			int retries = 0;
			int j=oul_speculative::last_completed+1;
			while(true){
				int i = j & pending_mask;
				if(committedTxs[i] == NULL){
					break;
				}
				LOG("+++++++++ " << j << " +++++++++ ");
				if(committedTxs[i]->complete()){
//					committedTxs[i]->clean();
				}else
					retries += restartTx(committedTxs[i]->getAge());
				committedTxs[i] = NULL;
				oul_speculative::last_completed++;
				j++;
			}
			oul_speculative::validation = 0;	// reset the flag
			return retries;
		}
		return 0;
	}

	void run_in_order(void (func) (void**), void** args, int age){
		executions_count += try_validation();
		if(age - oul_speculative::last_completed > max_pending){
			hold_on++;
			while(age - oul_speculative::last_completed > min_pending) try_validation();	// freeze execution
		}
		function = func;
		transactions_count++;
		jmp_buf _jmpbuf;
		int abort_flags = setjmp(_jmpbuf);
		executions_count++;
		oul_speculative::Transaction* tx = new oul_speculative::Transaction(age);
		pthread_setspecific(tx_key, (void*)tx);
		tx->start(&_jmpbuf);
		func(args);
		tx->commit();
		tx->noRetry();
		arguments[age & ARGS_MASK] = args;
		committedTxs[age & pending_mask] = tx;
	}

	void wait_till_finish(){
		executions_count += try_validation();
		pthread_barrier_wait (&finish_barrier);
		if(oul_speculative::last_completed > 0){
			oul_speculative::last_completed = -1;
			oul_speculative::validation = 0;
			for(int i=0; i<pending_queue_length; i++){
				committedTxs[i] = NULL;
			}
		}
		DUMP_LOG
	}

	void print_statistics(){
		cout << "===================================" << endl;
		cout << "Algorithm =\t" << "OUL Speculative+" << endl;
		cout << "Workers =\t" << workers_count << endl;
		cout << "Transactions =\t" << transactions_count << endl;
		cout << "Aborts =\t" << (executions_count - transactions_count) << endl;
		cout << "Holdons =\t" << hold_on << endl;
		cout << "===================================" << endl;
	}
};


namespace o_api_oul_speculative_plus_plus{

	static oul_speculative::Transaction** committedTxs;
	int pending_queue_length;
	int pending_mask;
	int min_pending;
	int hold_on;
	long aborts;
	void (*function) (void**);
	void*** arguments;

	int get_workers_count(){
		return workers_count;
	}

	oul_speculative::Transaction* getTx(){
		return (oul_speculative::Transaction*)pthread_getspecific(tx_key);
	}

	void init(int workers){
		INIT_LOG
		workers_count = workers;
		pthread_mutex_init(&lock, NULL);
		pthread_key_create(&tx_key, NULL);
		pthread_barrier_init (&finish_barrier, NULL, workers);
		oul_speculative::init();
		oul_speculative::last_completed = -1;
		oul_speculative::validation = 0;
		hold_on = 0;
		min_pending = workers;
		aborts = 0;
		pending_queue_length = 1024;
		while(pending_queue_length < workers * MAX_PENDING_PER_THREAD)
			pending_queue_length *= 2;
		pending_mask = pending_queue_length - 1;
		arguments = new void**[ARGS_SIZE];
		committedTxs = new oul_speculative::Transaction*[pending_queue_length];
		for(int i=0; i<pending_queue_length; i++){
			committedTxs[i] = NULL;
		}
	}

	void init_thread(){
		INIT_THREAD_LOG
		stick_worker(__sync_fetch_and_add(&cores_counter, 1));
	}

	long restartTx(int age){
		long retries = 0;
		jmp_buf _jmpbuf;
		int abort_flags = setjmp(_jmpbuf);
		retries++;
		oul_speculative::Transaction* tx = new oul_speculative::Transaction(age);
		pthread_setspecific(tx_key, (void*)tx);
		LOG("restart " << age);
		tx->start(&_jmpbuf);
		function(arguments[age & ARGS_MASK]);
		tx->commit();
		if(tx->complete()){
//			tx->clean();
		}
		LOG("restarted " << age);
		return retries;
	}

	int try_validation(){
		if(
			oul_speculative::validation == 0 &&
			__sync_bool_compare_and_swap(&oul_speculative::validation, 0, 1)
		)
		{ // try to be the validator
			int temp = oul_speculative::last_completed;
			int retries = 0;
			int j=oul_speculative::last_completed+1;
			while(true){
				int i = j & pending_mask;
				if(committedTxs[i] == NULL){
					break;
				}
				LOG("+++++++++ " << j << " +++++++++ ");
				if(committedTxs[i]->complete()){
//					committedTxs[i]->clean();
				}else{
					long r = restartTx(committedTxs[i]->getAge());
					retries += r;
					aborts += r - 1;
				}
				committedTxs[i] = NULL;
				oul_speculative::last_completed++;
				j++;
			}
			oul_speculative::validation = 0;	// reset the flag
			return retries;
		}
		return 0;
	}

	void run_in_order(void (func) (void**), void** args, int age){
		executions_count += try_validation();
        if(aborts * MAX_ABORT_RATIO > age || age - oul_speculative::last_completed > pending_queue_length){
        	hold_on++;
            while(age - oul_speculative::last_completed > min_pending) try_validation(); // freeze execution
        }
		function = func;
		transactions_count++;
		int temp = executions_count;
		jmp_buf _jmpbuf;
		int abort_flags = setjmp(_jmpbuf);
		executions_count++;
		oul_speculative::Transaction* tx = new oul_speculative::Transaction(age);
		pthread_setspecific(tx_key, (void*)tx);
		tx->start(&_jmpbuf);
		func(args);
		tx->commit();
		tx->noRetry();
		arguments[age & ARGS_MASK] = args;
		committedTxs[age & pending_mask] = tx;
		aborts += executions_count - temp - 1;
	}

	void wait_till_finish(){
		executions_count += try_validation();
		pthread_barrier_wait (&finish_barrier);
		if(oul_speculative::last_completed > 0){
			oul_speculative::last_completed = -1;
			oul_speculative::validation = 0;
			for(int i=0; i<pending_queue_length; i++){
				committedTxs[i] = NULL;
			}
		}
		DUMP_LOG
	}

	void print_statistics(){
		cout << "===================================" << endl;
		cout << "Algorithm =\t" << "OUL Speculative++" << endl;
		cout << "Workers =\t" << workers_count << endl;
		cout << "Transactions =\t" << transactions_count << endl;
		cout << "Aborts =\t" << (executions_count - transactions_count) << endl;
		cout << "Holdons =\t" << hold_on << endl;
		cout << "===================================" << endl;
	}
};

namespace o_api_oul_steal{

	static oul_steal::Transaction** committedTxs;
	int pending_queue_length;
	int pending_mask;
	int max_pending;
	void (*function) (void**);
	void*** arguments;

	int get_workers_count(){
		return workers_count;
	}

	oul_steal::Transaction* getTx(){
		return (oul_steal::Transaction*)pthread_getspecific(tx_key);
	}

	void init(int workers){
		INIT_LOG
		workers_count = workers;
		pthread_mutex_init(&lock, NULL);
		pthread_key_create(&tx_key, NULL);
		pthread_barrier_init (&finish_barrier, NULL, workers);
		oul_steal::init();
		oul_steal::last_completed = -1;
		oul_steal::validation = 0;
		max_pending = workers * MAX_PENDING_PER_THREAD;
		pending_queue_length = 1024;
		while(pending_queue_length < max_pending)
			pending_queue_length *= 2;
		pending_mask = pending_queue_length - 1;
		arguments = new void**[ARGS_SIZE];
		committedTxs = new oul_steal::Transaction*[pending_queue_length];
		for(int i=0; i<pending_queue_length; i++){
			committedTxs[i] = NULL;
		}
#ifdef DEBUG_OUL_STEAL
		pthread_key_create(&history_key, NULL);
#endif
	}

	void init_thread(){
		INIT_THREAD_LOG
		stick_worker(__sync_fetch_and_add(&cores_counter, 1));
#ifdef DEBUG_OUL_STEAL
		UnsafeList<oul_steal::Transaction*> *localHistory = new UnsafeList<oul_steal::Transaction*>();
		pthread_setspecific(history_key, (void*)localHistory);
#endif
	}

	long restartTx(int age){
		long retries = 0;
		jmp_buf _jmpbuf;
		int abort_flags = setjmp(_jmpbuf);
		retries++;
		oul_steal::Transaction* tx = new oul_steal::Transaction(age);
		pthread_setspecific(tx_key, (void*)tx);
#ifdef DEBUG_OUL_STEAL
		((UnsafeList<oul_steal::Transaction*>*)pthread_getspecific(history_key))->add(tx);
#endif
		LOG("restart " << age);
		tx->start(&_jmpbuf);
		function(arguments[age & ARGS_MASK]);
#ifdef DEBUG_OUL_STEAL
		tx->dump();
#endif
		tx->commit();
		if(tx->complete()){
//			tx->clean();
		}
		LOG("restarted " << age);
		return retries;
	}

	int try_validation(){
		if(
			oul_steal::validation == 0 &&
			__sync_bool_compare_and_swap(&oul_steal::validation, 0, 1)
		)
		{ // try to be the validator
			int temp = oul_steal::last_completed;
			int retries = 0;
			int j=oul_steal::last_completed+1;
			while(true){
				int i = j & pending_mask;
				if(committedTxs[i] == NULL){
					break;
				}
				LOG("+++++++++ " << j << " +++++++++ ");
				if(committedTxs[i]->complete()){
//					committedTxs[i]->clean();
				}else
					retries += restartTx(committedTxs[i]->getAge());
				committedTxs[i] = NULL;
				oul_steal::last_completed++;
				j++;
			}
			oul_steal::validation = 0;	// reset the flag
			return retries;
		}
		return 0;
	}

	void run_in_order(void (func) (void**), void** args, int age){
		executions_count += try_validation();
		while(age - oul_steal::last_completed > max_pending) executions_count += try_validation();
		function = func;
		transactions_count++;
		jmp_buf _jmpbuf;
		int abort_flags = setjmp(_jmpbuf);
		executions_count++;
		oul_steal::Transaction* tx = new oul_steal::Transaction(age);
		pthread_setspecific(tx_key, (void*)tx);
#ifdef DEBUG_OUL_STEAL
		((UnsafeList<oul_steal::Transaction*>*)pthread_getspecific(history_key))->add(tx);
#endif
		tx->start(&_jmpbuf);
		func(args);
#ifdef DEBUG_OUL_STEAL
		tx->dump();
#endif
		tx->commit();
		tx->noRetry();
		arguments[age & ARGS_MASK] = args;
		committedTxs[age & pending_mask] = tx;
	}

	void wait_till_finish(){
		executions_count += try_validation();
		pthread_barrier_wait (&finish_barrier);
#ifdef DEBUG_OUL_STEAL
		INFO("===============================================================================================");
		ListNode<oul_steal::Transaction*>* itr = ((UnsafeList<oul_steal::Transaction*>*)pthread_getspecific(history_key))->header;
		while (itr != NULL) {
			((oul_steal::Transaction*)itr->value)->dump();
			itr = itr->next;
		}
#endif
		if(oul_steal::last_completed > 0){
			oul_steal::last_completed = -1;
			oul_steal::validation = 0;
			for(int i=0; i<pending_queue_length; i++){
				committedTxs[i] = NULL;
			}
		}
		DUMP_LOG
	}

	void print_statistics(){
		cout << "===================================" << endl;
		cout << "Algorithm =\t" << "OUL Steal" << endl;
		cout << "Workers =\t" << workers_count << endl;
		cout << "Transactions =\t" << transactions_count << endl;
		cout << "Aborts =\t" << (executions_count - transactions_count) << endl;
		cout << "===================================" << endl;
	}
};


namespace o_api_oul_steal_plus{

	static oul_steal::Transaction** committedTxs;
	int pending_queue_length;
	int pending_mask;
	int max_pending;
	int min_pending;
	int hold_on;
	void (*function) (void**);
	void*** arguments;

	int get_workers_count(){
		return workers_count;
	}

	oul_steal::Transaction* getTx(){
		return (oul_steal::Transaction*)pthread_getspecific(tx_key);
	}

	void init(int workers){
		INIT_LOG
		workers_count = workers;
		pthread_mutex_init(&lock, NULL);
		pthread_key_create(&tx_key, NULL);
		pthread_barrier_init (&finish_barrier, NULL, workers);
		oul_steal::init();
		oul_steal::last_completed = -1;
		oul_steal::validation = 0;
		hold_on = 0;
		max_pending = workers * MAX_PENDING_PER_THREAD;
		min_pending = workers;
		pending_queue_length = 1024;
		while(pending_queue_length < max_pending)
			pending_queue_length *= 2;
		pending_mask = pending_queue_length - 1;
		arguments = new void**[ARGS_SIZE];
		committedTxs = new oul_steal::Transaction*[pending_queue_length];
		for(int i=0; i<pending_queue_length; i++){
			committedTxs[i] = NULL;
		}
#ifdef DEBUG_OUL_STEAL
		pthread_key_create(&history_key, NULL);
#endif
	}

	void init_thread(){
		INIT_THREAD_LOG
		stick_worker(__sync_fetch_and_add(&cores_counter, 1));
#ifdef DEBUG_OUL_STEAL
		UnsafeList<oul_steal::Transaction*> *localHistory = new UnsafeList<oul_steal::Transaction*>();
		pthread_setspecific(history_key, (void*)localHistory);
#endif
	}

	long restartTx(int age){
		long retries = 0;
		jmp_buf _jmpbuf;
		int abort_flags = setjmp(_jmpbuf);
		retries++;
		oul_steal::Transaction* tx = new oul_steal::Transaction(age);
		pthread_setspecific(tx_key, (void*)tx);
#ifdef DEBUG_OUL_STEAL
		((UnsafeList<oul_steal::Transaction*>*)pthread_getspecific(history_key))->add(tx);
#endif
		LOG("restart " << age);
		tx->start(&_jmpbuf);
		function(arguments[age & ARGS_MASK]);
#ifdef DEBUG_OUL_STEAL
		tx->dump();
#endif
		tx->commit();
		if(tx->complete()){
//			tx->clean();
		}
		LOG("restarted " << age);
		return retries;
	}

	int try_validation(){
		if(
			oul_steal::validation == 0 &&
			__sync_bool_compare_and_swap(&oul_steal::validation, 0, 1)
		)
		{ // try to be the validator
			int temp = oul_steal::last_completed;
			int retries = 0;
			int j=oul_steal::last_completed+1;
			while(true){
				int i = j & pending_mask;
				if(committedTxs[i] == NULL){
					break;
				}
				LOG("+++++++++ " << j << " +++++++++ ");
				if(committedTxs[i]->complete()){
//					committedTxs[i]->clean();
				}else
					retries += restartTx(committedTxs[i]->getAge());
				committedTxs[i] = NULL;
				oul_steal::last_completed++;
				j++;
			}
			oul_steal::validation = 0;	// reset the flag
			return retries;
		}
		return 0;
	}

	void run_in_order(void (func) (void**), void** args, int age){
		executions_count += try_validation();
		if(age - oul_steal::last_completed > max_pending){
        	hold_on++;
			while(age - oul_steal::last_completed > min_pending) try_validation();	// freeze execution
		}
		function = func;
		transactions_count++;
		jmp_buf _jmpbuf;
		int abort_flags = setjmp(_jmpbuf);
		executions_count++;
		oul_steal::Transaction* tx = new oul_steal::Transaction(age);
		pthread_setspecific(tx_key, (void*)tx);
#ifdef DEBUG_OUL_STEAL
		((UnsafeList<oul_steal::Transaction*>*)pthread_getspecific(history_key))->add(tx);
#endif
		tx->start(&_jmpbuf);
		func(args);
#ifdef DEBUG_OUL_STEAL
		tx->dump();
#endif
		tx->commit();
		tx->noRetry();
		arguments[age & ARGS_MASK] = args;
		committedTxs[age & pending_mask] = tx;
	}

	void wait_till_finish(){
		executions_count += try_validation();
		pthread_barrier_wait (&finish_barrier);
#ifdef DEBUG_OUL_STEAL
		INFO("===============================================================================================");
		ListNode<oul_steal::Transaction*>* itr = ((UnsafeList<oul_steal::Transaction*>*)pthread_getspecific(history_key))->header;
		while (itr != NULL) {
			((oul_steal::Transaction*)itr->value)->dump();
			itr = itr->next;
		}
#endif
		if(oul_steal::last_completed > 0){
			oul_steal::last_completed = -1;
			oul_steal::validation = 0;
			for(int i=0; i<pending_queue_length; i++){
				committedTxs[i] = NULL;
			}
		}
		DUMP_LOG
	}

	void print_statistics(){
		cout << "===================================" << endl;
		cout << "Algorithm =\t" << "OUL Steal+" << endl;
		cout << "Workers =\t" << workers_count << endl;
		cout << "Transactions =\t" << transactions_count << endl;
		cout << "Aborts =\t" << (executions_count - transactions_count) << endl;
		cout << "Holdons =\t" << hold_on << endl;
		cout << "===================================" << endl;
	}
};


namespace o_api_oul_steal_plus_plus{

	static oul_steal::Transaction** committedTxs;
	int pending_queue_length;
	int pending_mask;
	int min_pending;
	long aborts;
	int hold_on;
	void (*function) (void**);
	void*** arguments;

	int get_workers_count(){
		return workers_count;
	}

	oul_steal::Transaction* getTx(){
		return (oul_steal::Transaction*)pthread_getspecific(tx_key);
	}

	void init(int workers){
		INIT_LOG
		workers_count = workers;
		pthread_mutex_init(&lock, NULL);
		pthread_key_create(&tx_key, NULL);
		pthread_barrier_init (&finish_barrier, NULL, workers);
		oul_steal::init();
		oul_steal::last_completed = -1;
		oul_steal::validation = 0;
		hold_on = 0;
		min_pending = workers;
		aborts = 0;
		pending_queue_length = 1024;
		while(pending_queue_length < workers * MAX_PENDING_PER_THREAD)
			pending_queue_length *= 2;
		pending_mask = pending_queue_length - 1;
		arguments = new void**[ARGS_SIZE];
		committedTxs = new oul_steal::Transaction*[pending_queue_length];
		for(int i=0; i<pending_queue_length; i++){
			committedTxs[i] = NULL;
		}
#ifdef DEBUG_OUL_STEAL
		pthread_key_create(&history_key, NULL);
#endif
	}

	void init_thread(){
		INIT_THREAD_LOG
		stick_worker(__sync_fetch_and_add(&cores_counter, 1));
#ifdef DEBUG_OUL_STEAL
		UnsafeList<oul_steal::Transaction*> *localHistory = new UnsafeList<oul_steal::Transaction*>();
		pthread_setspecific(history_key, (void*)localHistory);
#endif
	}

	long restartTx(int age){
		long retries = 0;
		jmp_buf _jmpbuf;
		int abort_flags = setjmp(_jmpbuf);
		retries++;
		oul_steal::Transaction* tx = new oul_steal::Transaction(age);
		pthread_setspecific(tx_key, (void*)tx);
#ifdef DEBUG_OUL_STEAL
		((UnsafeList<oul_steal::Transaction*>*)pthread_getspecific(history_key))->add(tx);
#endif
		LOG("restart " << age);
		tx->start(&_jmpbuf);
		function(arguments[age & ARGS_MASK]);
#ifdef DEBUG_OUL_STEAL
		tx->dump();
#endif
		tx->commit();
		if(tx->complete()){
//			tx->clean();
		}
		LOG("restarted " << age);
		return retries;
	}

	int try_validation(){
		if(
			oul_steal::validation == 0 &&
			__sync_bool_compare_and_swap(&oul_steal::validation, 0, 1)
		)
		{ // try to be the validator
			int temp = oul_steal::last_completed;
			int retries = 0;
			int j=oul_steal::last_completed+1;
			while(true){
				int i = j & pending_mask;
				if(committedTxs[i] == NULL){
					break;
				}
				LOG("+++++++++ " << j << " +++++++++ ");
				if(committedTxs[i]->complete()){
//					committedTxs[i]->clean();
				}else{
					long r = restartTx(committedTxs[i]->getAge());
					retries += r;
					aborts += r - 1;
				}
				committedTxs[i] = NULL;
				oul_steal::last_completed++;
				j++;
			}
			oul_steal::validation = 0;	// reset the flag
			return retries;
		}
		return 0;
	}

	void run_in_order(void (func) (void**), void** args, int age){
		executions_count += try_validation();
        if(aborts * MAX_ABORT_RATIO > age || age - oul_steal::last_completed > pending_queue_length){
        	hold_on++;
            while(age - oul_steal::last_completed > min_pending) try_validation(); // freeze execution
        }
		function = func;
		transactions_count++;
		int temp = executions_count;
		jmp_buf _jmpbuf;
		int abort_flags = setjmp(_jmpbuf);
		executions_count++;
		oul_steal::Transaction* tx = new oul_steal::Transaction(age);
		pthread_setspecific(tx_key, (void*)tx);
#ifdef DEBUG_OUL_STEAL
		((UnsafeList<oul_steal::Transaction*>*)pthread_getspecific(history_key))->add(tx);
#endif
		tx->start(&_jmpbuf);
		func(args);
#ifdef DEBUG_OUL_STEAL
		tx->dump();
#endif
		tx->commit();
		tx->noRetry();
		arguments[age & ARGS_MASK] = args;
		committedTxs[age & pending_mask] = tx;
		aborts += executions_count - temp - 1;
	}

	void wait_till_finish(){
		executions_count += try_validation();
		pthread_barrier_wait (&finish_barrier);
#ifdef DEBUG_OUL_STEAL
		INFO("===============================================================================================");
		ListNode<oul_steal::Transaction*>* itr = ((UnsafeList<oul_steal::Transaction*>*)pthread_getspecific(history_key))->header;
		while (itr != NULL) {
			((oul_steal::Transaction*)itr->value)->dump();
			itr = itr->next;
		}
#endif
		if(oul_steal::last_completed > 0){
			oul_steal::last_completed = -1;
			oul_steal::validation = 0;
			for(int i=0; i<pending_queue_length; i++){
				committedTxs[i] = NULL;
			}
		}
		DUMP_LOG
	}

	void print_statistics(){
		cout << "===================================" << endl;
		cout << "Algorithm =\t" << "OUL Steal++" << endl;
		cout << "Workers =\t" << workers_count << endl;
		cout << "Transactions =\t" << transactions_count << endl;
		cout << "Aborts =\t" << (executions_count - transactions_count) << endl;
		cout << "Holdons =\t" << hold_on << endl;
		cout << "===================================" << endl;
	}
};

#endif
