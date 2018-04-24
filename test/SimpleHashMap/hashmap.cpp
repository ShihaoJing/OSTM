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
#include <random>
#include <vector>

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

#include "cuckoo_map_long.h"

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

static int keyrange = 102400;
static int ops = 512;
static int op_ratio = 60;
cuckoo_map<long> *map;

static int groupCount = 1024;

static void transactional_work(void **args) {
	bool add_op = *(bool*)args[0];
	int key = *(int*)args[1];
	if (add_op) {
		map->add(key);
	} else {
		map->remove(key);
	}
}

static void worker(void *arg) {
	int tid = thread_getId();
	int workers_count = O_API::get_workers_count();

	vector<int> keys;

	std::random_device r;
	std::default_random_engine e(r());
	std::uniform_int_distribution<int> key_rand(0, keyrange);

	void* args[2];
	bool add_op = true;

	for (int age = tid; age < ops; age += workers_count) {
		int key = key_rand(e);
		keys.push_back(key);
		args[0] = (void*)&add_op;
		args[1] = (void*)&key;
		O_API::run_in_order(transactional_work, args, age);
	}

	O_API::wait_till_finish();

	add_op = false;

	for (int age = tid; age < ops; age += workers_count) {
		int key = keys.back();
		keys.pop_back();
		args[0] = (void*)&add_op;
		args[1] = (void*)&key;
		O_API::run_in_order(transactional_work, args, age);
	}

	O_API::wait_till_finish();
}

int MAIN_HASHMAP(int argc, char** argv) {
	using std::cout;
	using std::cerr;
	using std::endl;

	// for getopt
	long opt;
	// parameters
	int hashpower = 10;
	int threads = 1;
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
			op_ratio = atoi(optarg);
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
	cout << "  insert/remove:        " << op_ratio << "/" << (100 - op_ratio) << endl;
	cout << "  threads:              " << threads << endl;
	cout << "  test name:            " << test << endl;
	cout << endl;

	map = new cuckoo_map<long>(hashpower);
	map->populate();


	// run the microbenchmark
	O_API::init(threads);
	thread_startup(threads);

	long long startTime = getElapsedTime();

	thread_start(worker, NULL);

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
	cout << map->size() << endl;
	cout << "!!!Bye!!!" << endl;
	return 0;
}
