/*
 * hashmap.cpp
 *
 *  Created on: Mar 18, 2018
 *      Author: root
 */

#include <unordered_set>
#include <unistd.h>
#include <thread>
#include <iostream>
#include <chrono>

#include "simpleMap.h"
#include "../../conf.h"
#include "../thread.h"
#include "../tm.h"

#include "../../conf.h"

#include "../../algs/commons.hpp"
#include "../../algs/dummy.hpp"
#include "../../algs/owb.hpp"
#include "../../algs/owbrh.hpp"
#include "../../algs/tl2.hpp"
#include "../../algs/norec.hpp"
#include "../../algs/undolog_invis.hpp"
#include "../../algs/undolog_vis.hpp"
#include "../../algs/oul_speculative.hpp"
#include "../../algs/oul_steal.hpp"
#include "../../algs/stmlite.hpp"
#include "../../algs/stmlite-unordered.hpp"

/// help() - Print a help message
void help(char *progname) {
	using std::cout;
	using std::endl;
	cout << "Usage: " << progname << " [OPTIONS]" << endl;
	cout << "Execute a concurrent set microbenchmark" << endl;
	cout << "  -k    key range for elements in the list" << endl;
	cout << "  -o    operations per thread" << endl;
	cout << "  -b    # buckets for hash tests" << endl;
	cout << "  -r    ratio of lookup operations" << endl;
	cout << "  -t    number of threads" << endl;
	cout << "  -n    test name" << endl;
	cout << "        [l]ist, [r]wlist, [h]ash, [s]entinel hash" << endl;
}

static unsigned keyrange = 1024;
static unsigned iters = 1000;
static unsigned op_ratio = 60;
static unsigned hashpower = 16;
static int nthreads = 1;
static int* count;

static void worker(void *arg) {
	int tid = thread_getId();
	seq_map<int> *map = (seq_map<int>*) arg;

	std::random_device r;
	std::default_random_engine e(r());
	std::uniform_int_distribution<int> key_rand(0, keyrange);
	std::uniform_int_distribution<int> ratio_rand(1, 100);

	for (int i = 0; i < iters; ++i) {
		int key = key_rand(e);
		int action = ratio_rand(e);
		if (action <= op_ratio) {
			if (map->add(key))
				count[tid]++;
		}
		else {
			if(map->remove(key))
				count[tid]--;
		}
	}
}

int MAIN_HASHMAP(int argc, char** argv) {
	using std::cout;
	using std::cerr;
	using std::endl;

	// for getopt
	long opt;
	// parameters
	unsigned keyrange = 256;
	unsigned ops = 1000;
	unsigned hashpower = 16;
	unsigned ratio = 60;
	unsigned threads = 8;
	char test = 's';

	// parse the command-line options.  see help() for more info
	while ((opt = getopt(argc, argv, "hr:o:c:R:m:n:t:p:")) != -1) {
		switch (opt) {
		case 'h':
			help(argv[0]);
			return 0;
		case 'r':
			keyrange = atoi(optarg);
			break;
		case 'o':
			ops = atoi(optarg);
			break;
		case 'c':
			hashpower = atoi(optarg);
			break;
		case 'R':
			ratio = atoi(optarg);
			break;
		case 't':
			threads = atoi(optarg);
			break;
		case 'm':
			test = optarg[0];
			break;
		}
	}

	// print the configuration
	cout << "Configuration: " << endl;
	cout << "  key range:            " << keyrange << endl;
	cout << "  ops/thread:           " << ops << endl;
	cout << "  hashpower:            " << hashpower << endl;
	cout << "  insert/remove:        " << ratio << "/" << (100 - ratio) << endl;
	cout << "  threads:              " << threads << endl;
	cout << "  test name:            " << test << endl;
	cout << endl;

	seq_map<int> *map = new seq_map<int>(hashpower);
	map->populate();

	count = new int[threads];
	for (int i = 0; i < threads; ++i)
		count[i] = 0;

	// run the microbenchmark
	O_API::init(threads);
	thread_startup(threads);

	long long startTime = getElapsedTime();

	thread_start(worker, (void*) map);

	long long endTime = getElapsedTime();
	thread_shutdown();

	//cout << "Algorithm =\t" << algorithm << endl;
	cout << "Time =  \t" << endTime - startTime << endl;
	cout << "Workers = \t" << threads << endl;
	//cout << "Cleaners = \t" << cleaners << endl;
	//cout << "Transactions =\t" << workload << endl;
	//cout << "Aborts =\t" << executed - workload << endl;
	cout << "===================================" << endl;
#ifdef ENABLE_THREADS
	O_API::print_statistics();
#endif

	int expect_size = 1024;
	for (int i = 0; i < nthreads; ++i) {
		expect_size += count[i];
	}

	int set_size = map->size();

	cout << "expect_size: " << expect_size << endl;
	cout << "set_size     " << set_size << endl;


	cout << "!!!Bye!!!" << endl;
	return 0;
}
