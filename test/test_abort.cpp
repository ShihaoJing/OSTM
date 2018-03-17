/*
 * test_abort.cpp
 *
 *  Created on: Feb 19, 2018
 *      Author: root
 */

#include <iostream>
#include <time.h>
#include <stdlib.h>
#include <setjmp.h>
#include <errno.h>

using namespace std;

#include "../conf.h"

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

static long counter = 0;

static volatile int last_committed = -1;

void restartTx(int age) {
	jmp_buf _jmpbuf;
	int abort_flags = setjmp(_jmpbuf);
	owb::Transaction* tx = new owb::Transaction(age);
	LOG("restart " << age);
	tx->start(&_jmpbuf);
	long value = tx->read_l((long*)&counter);
	tx->write_l((long*)&counter, value+1);
	tx->commit();
	if(tx->complete())
		tx->clean();
	LOG("restarted " << age);
}

void* proc(void* arg) {
	INIT_THREAD_LOG

	UnsafeQueue<owb::Transaction*> pending;
	int min_pending = -1;
	long age = (long) arg;
	jmp_buf _jmpbuf;
	int abort_flags = setjmp(_jmpbuf);
	LOG("exec " << age);
	owb::Transaction* tx = new owb::Transaction(age);
	tx->start(&_jmpbuf);
	do{
		owb::Transaction* ptx = pending.peek();
		if(ptx!=NULL && last_committed == ptx->getAge() - 1){
			LOG("complete [" << ptx->getAge() << "] last completed " << last_committed);
			if(!ptx->complete())
				restartTx(ptx->getAge());
			ptx->clean();
			last_committed++;
			pending.remove();
			continue;
		}
		break;	// nothing pending
	} while(true);

	long value = tx->read_l((long*)&counter);
	tx->write_l((long*)&counter, value + 1);
	tx->commit();
	if(last_committed == age - 1){
		LOG("complete " << age << " last completed " << last_committed);
		if(!tx->complete())
			restartTx(tx->getAge());
		tx->clean();
		last_committed++;
	}else{
		LOG("enqueue " << age << " [pending: " << (pending.peek()==NULL ? -1 : pending.peek()->getAge() ) << "] last committed " << last_committed);
		tx->noRetry();
		pending.enqueue(tx);
	}
	
	do{
		owb::Transaction* ptx = pending.peek();
		if(ptx==NULL) break;
		if(last_committed == ptx->getAge() - 1){
			LOG("complete [" << ptx->getAge() << "] last committed " << last_committed);
			if(!ptx->complete())
				restartTx(ptx->getAge());
			ptx->clean();
			last_committed++;
			pending.remove();
		}
	} while(true);

	DUMP_LOG
}



int main(int argc, char *argv[]) {
	pthread_t t1, t2;
	pthread_create(&t1, NULL, proc, (void*)(long)0);
	pthread_create(&t1, NULL, proc, (void*)(long)1);
	pthread_join(t1, NULL);
	pthread_join(t2, NULL);

	LOG("counter " << counter);
}


