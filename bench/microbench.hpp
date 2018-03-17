#include <iostream>
using namespace std;

#include "../algs/commons.hpp"
#include "../algs/library_inst.hpp"
#include "../algs/owb.hpp"
#include "../algs/tl2.hpp"
#include "../algs/norec.hpp"
#include "../algs/undolog_invis.hpp"
#include "../algs/undolog_vis.hpp"
#include "../algs/oul_speculative.hpp"
#include "../algs/oul_steal.hpp"
#include "../algs/stmlite.hpp"
#include "../algs/stmlite-unordered.hpp"


//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ COUNTER ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~//

namespace counter_bench{

static long counter = 0;

void benchmark_init(int argc, char *argv[]){
	cout << "Counter Bench: started" << endl;
    int opt;    // parse the command-line options
    while ((opt = getopt(argc, argv, "v:")) != -1) {
        switch(opt) {
          case 'v': counter = strtol(optarg, NULL, 10); break;
          case 'h':
            std::cout << "Usage: -v[initial-value]" << endl;
        }
    }
}

void benchmark_start(int age, long tid){
}

template<typename T>
void benchmark(T* tx, int age, long tid){
	long value = tx->read_l((long*)&counter);
	tx->write_l((long*)&counter, value+1);
}

void benchmark_destroy(){
	cout << "Counter Bench: Value=" << counter << endl;
}

}

//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ DISJOINT ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~//

namespace dis_bench{

static int n = 1;
static int o = 0;
static long** elements;

void benchmark_init(int argc, char *argv[]){
	cout << "Disjoint Bench: started" << endl;
    elements = new long*[20000000];
    int opt;    // parse the command-line options
	while ((opt = getopt(argc, argv, "n:o:h")) != -1) {
		switch(opt) {
		  case 'n': n = strtol(optarg, NULL, 10); break;
		  case 'o': o = strtol(optarg, NULL, 10); break;
		  case 'h':
			std::cout << "Usage: -n[reads] -o[operations]" << endl;
		}
	}
}

void benchmark_start(int age, long tid){
}

template<typename T>
void benchmark(T* tx, int age, long tid){
	commons::rand_set_seed(age);
	long sum = 0;
	int loc;
	for(int i=0; i<n; i++){
		loc = age * 40 + commons::rand_r_32() % 40;
		LOG(age << ": R:" << loc);
		sum += tx->read_l((long*)&elements[loc]);
		for(int j=0; j<o; j++)
			sum = sum * o;
	}
	LOG(age << ": W:" << loc);
	for(int j=0; j<o; j++)
		sum = sum * o;
	tx->write_l((long*)&elements[loc], sum);
}

void benchmark_destroy(){
	cout << "Disjoint Bench: N=" << n << ", Operations=" << o << endl;
	delete elements;
}

}

//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ RNW1 ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~//

namespace rnw1_bench{

static int o = 0;
static long n = 100;
static long m = 10000;
static long* elements;

void benchmark_init(int argc, char *argv[]){
	cout << "RNW1 Bench: started" << endl;
    int opt;    // parse the command-line options
    while ((opt = getopt(argc, argv, "n:m:o:h")) != -1) {
        switch(opt) {
          case 'n': n = strtol(optarg, NULL, 10); break;
          case 'm': m = strtol(optarg, NULL, 10); break;
          case 'o': o = strtol(optarg, NULL, 10); break;
          case 'h':
            std::cout << "Usage: -n[reads] -m[size] -o[operations]" << endl;
        }
    }
    elements = new long[m];
    for(int i=0; i<m; i++)
    	elements[m] = i;
}

void benchmark_start(int age, long tid){
	LOG("benchmark started age: " << age << " tid: " << tid);
}

template<typename T>
void benchmark(T* tx, int age, long tid){
	commons::rand_set_seed(age);
	long sum = 1;
	int loc = 0;
	for(int i=0; i<n; i++){
		loc = commons::rand_r_32() % m;
		LOG(age << ": R:" << loc);
		sum += tx->read_l((long*)&elements[loc]);
		for(int j=0; j<o; j++)
			sum = sum * o;
	}
	LOG(age << ": W:" << loc);
	for(int j=0; j<o; j++)
		sum = sum * o;
	tx->write_l((long*)&elements[loc], sum);
}

void benchmark_destroy(){
	cout << "RNW1 Bench: N=" << n << ", Elements=" << m << ", Operations=" << o << endl;
	delete elements;
}

}

//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ RNWN ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~//

namespace rwn_bench{

static int o = 0;
static long n = 100;
static long m = 10000;
static long* elements;

void benchmark_init(int argc, char *argv[]){
	cout << "RWN Bench: started" << endl;
    int opt;    // parse the command-line options
    while ((opt = getopt(argc, argv, "n:m:o:h")) != -1) {
        switch(opt) {
          case 'n': n = strtol(optarg, NULL, 10); break;
          case 'm': m = strtol(optarg, NULL, 10); break;
		  case 'o': o = strtol(optarg, NULL, 10); break;
          case 'h':
            std::cout << "Usage: -n[reads/writes] -m[size] -o[operations]" << endl;
        }
    }
    elements = new long[m];
    for(int i=0; i<m; i++)
    	elements[m] = i;
}

void benchmark_start(int age, long tid){
}

template<typename T>
void benchmark(T* tx, int age, long tid){
	commons::rand_set_seed(age);
	int snapshot[n];
	int loc[n];
	for(int i=0; i<n; i++){
		loc[i] = commons::rand_r_32() % m;
		LOG(age << ": R:" << loc[i])
		snapshot[i] = tx->read_l((long*)&elements[loc[i]]);
		for(int j=0; j<o; j++)
			snapshot[i] = snapshot[i] * o;
	}
	for(int i=0; i<n; i++){
		LOG(age << ": W:" << loc);
		for(int j=0; j<o; j++)
			snapshot[i] = snapshot[i] * o;
		tx->write_l((long*)&elements[loc[i]], snapshot[i] + 1);
	}
}

void benchmark_destroy(){
	cout << "RWN Bench: N=" << n << ", Elements=" << m << ", Operations=" << o << endl;
	delete elements;
}

}

//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ MCAS ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~//

namespace mcas_bench{

static int o = 0;
static long n = 100;
static long m = 10000;
static long* elements;

void benchmark_init(int argc, char *argv[]){
	cout << "MCAS Bench started" << endl;
    int opt;    // parse the command-line options
    while ((opt = getopt(argc, argv, "n:m:o:h")) != -1) {
        switch(opt) {
          case 'n': n = strtol(optarg, NULL, 10); break;
          case 'm': m = strtol(optarg, NULL, 10); break;
		  case 'o': o = strtol(optarg, NULL, 10); break;
          case 'h':
            std::cout << "Usage: -n[locations] -m[size] -o[operations]" << endl;
        }
    }
    elements = new long[m];
}

void benchmark_start(int age, long tid){
}

template<typename T>
void benchmark(T* tx, int age, long tid){
	commons::rand_set_seed(age);
	for(int i=0; i<n; i++){
		int loc = commons::rand_r_32() % m;
		LOG(age << ": R:" << loc)
		long val = tx->read_l((long*)&elements[loc]);
		for(int j=0; j<o; j++)
			val = val * o;
		for(int j=0; j<o; j++)
			val = val * o;
		tx->write_l((long*)&elements[loc], val + 1);
	}
}

void benchmark_destroy(){
	cout << "MCAS Bench: N=" << n << ", Elements=" << m << ", Operations=" << o << endl;
	delete elements;
}

}

//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~//
