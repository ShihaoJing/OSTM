#include <iostream>
#include <time.h>
#include <stdlib.h>
#include <setjmp.h>
#include <errno.h>
#include <atomic>

using namespace std;


#include "../conf.h"
#include "./thread.h"
#include "./tm.h"

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



long *num;
int* nums[4];


static void transactional_work(void **args) {
	long index = *((int*)args[0]);
	OTM_BEGIN();
	OTM_SHARED_WRITE_L(num[index], index);
	//tx->write_l((long*)&num[index], index);
	//OTM_SHARED_WRITE_I((int*)&num[index], index);
	OTM_END();
}

static void worker(void *args) {
	int tid = thread_getId();
	void **arg = new void*[1];
	int workers = O_API::get_workers_count();
	for (int i = tid; i < 4; i += workers) {
		arg[0] = (void*)&i;
		O_API::run_in_order(transactional_work, arg, i);
	}

	O_API::wait_till_finish();
}


 int MAIN_MICROBENCH(int argc, char *argv[]) {

	 for (int i = 0; i < 4; ++i) {
		 nums[i] = new int;
	 }
	 num = new long[4];
	 for (int i = 0; i < 4; ++i) {
	 		 cout << num[i] << endl;
	 	 }
	 int threadNum = 4;
	 O_API::init(threadNum);
	 thread_startup(threadNum);

	 thread_start(worker, NULL);
	 thread_shutdown();

	 cout << "===========================" << endl;
	 for (int i = 0; i < 4; ++i) {
		 cout << num[i] << endl;
	 }
	 //cout << *num << endl;
	 return 0;
}
