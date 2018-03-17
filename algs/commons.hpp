
#ifndef _THREAD_UTIL_
#define _THREAD_UTIL_

#include <pthread.h>
#include <errno.h>
#include <sched.h>
#include <stdlib.h>
#include <syscall.h>
#include <unistd.h>
#include <cstdio>
#include <iostream>
#include <stdint.h>
#include <signal.h>
#include <time.h>
#include <sys/time.h>
#include <iostream>
#include <stdio.h>
#include <string.h>
#include <sstream>

#include "./../conf.h"

//#define LOGGING
//#define DEFFERED

#define INFO(X) 				{ std::stringstream ss; ss << pthread_self() << ":" << X << std::endl; std::cout << ss.str(); }

#ifdef LOGGING
#ifdef DEFFERED
	static volatile long timer = 0;
	static pthread_key_t glob_var_key;
	#define INIT_LOG	 		{ pthread_key_create(&glob_var_key, NULL); }
	#define INIT_THREAD_LOG	 	{ std::stringstream *ss = malloc(sizeof(stringstream(); pthread_setspecific(glob_var_key, ss); }
	#define LOG(X) 				{ std::stringstream* ss = (std::stringstream*)pthread_getspecific(glob_var_key); (*ss) << timer++ << "\t" << pthread_self() << ":" << X << std::endl;  }
	#define DUMP_LOG			{ std::stringstream* ss = (std::stringstream*)pthread_getspecific(glob_var_key); std::cout << ss->str(); }
#else
	#define INIT_LOG
	#define INIT_THREAD_LOG
	#define LOG(X) 				{ std::stringstream ss; ss << pthread_self() << ":" << X << std::endl; std::cout << ss.str(); }
	#define DUMP_LOG
#endif
#else
#define INIT_LOG 
#define INIT_THREAD_LOG 
#define LOG(X)
#define DUMP_LOG 
#endif

#define DEBUG(X) 		LOG(X)
//#define DEBUG(X)

#define LOCK_SIZE	1048576		// 1024 * 1024
#define LOCK_MASK	1048575		// 1024 * 1024 - 1

namespace commons{


static pthread_key_t rand_key;

template<class T>
class ListNode {
public:
	ListNode* next;
	T value;
};

template<class T>
class List {
public:
	ListNode<T>* header;
	ListNode<T>* tail;
	List() : header(NULL), tail(NULL) { }

	int size(){
		ListNode<T>* temp = header;
		int count = 0;
		while (temp != NULL) {
			temp = temp->next;
			count++;
		}
		return count;
	}

	bool exists(T v){
		ListNode<T>* temp = header;
		while (temp != NULL) {
			if(temp->value == v)
				return true;
			temp = temp->next;
		}
		return false;
	}

	void reset() {
//		int count = 0;
		ListNode<T>* itr = header;
		while (itr != NULL) {
			ListNode<T>* temp = itr->next;
//			count++;
			delete itr;
			itr = temp;
		}
		header = NULL;
		tail = NULL;
//		LOG("Count " << count);
	}
};

template<class T>
class SafeList : public List<T>{
public:
	void add(T value) {
		ListNode<T>* node = (ListNode<T>*)malloc(sizeof(ListNode<T>()));
		node->value = value;
		do{node->next = this->header;} while(!__sync_bool_compare_and_swap(&(this->header), node->next, node));
	}
};

template<class T>
class UnsafeList : public List<T> {
public:
	void add(T value) {
		ListNode<T>* node = (ListNode<T>*)malloc(sizeof(ListNode<T>()));
		node->value = value;
		node->next = NULL;
		if(this->tail != NULL)
			this->tail->next = node;
		else
			this->header = node;	// first node
		this->tail = node;
	}

	void add(ListNode<T>* node) {
		node->next = NULL;
		if(this->tail != NULL)
			this->tail->next = node;
		else
			this->header = node;	// first node
		this->tail = node;
	}
};

template<class T>
class UnsafeReversedList : public List<T> {
public:
	void add(T value) {
		ListNode<T>* node = (ListNode<T>*)malloc(sizeof(ListNode<T>()));
		node->value = value;
		node->next = this->header;
		this->header = node;
	}

	void add(ListNode<T>* node) {
		node->next = this->header;
		this->header = node;	// first node
	}
};

template<class T>
class UnsafeQueue : public List<T> {
public:
	UnsafeQueue(){
		tail = NULL;
	}
	ListNode<T>* tail;
	void enqueue(T value) {
		ListNode<T>* node = (ListNode<T>*)malloc(sizeof(ListNode<T>()));
		node->value = value;
		node->next = NULL;
		if(this->header == NULL){
			this->tail = node;
			this->header = node;
		}else{
			this->tail->next = node;
			this->tail = node;
		}
	}
	T dequeue() {
		ListNode<T>* node = this->header;
		this->header = node->next;
		T v = node->value;
		delete node;
		return v;
	}
	T peek() {
		if(this->header != NULL)
			return this->header->value;
		return NULL;
	}
	void remove() {
		ListNode<T>* node = this->header;
		this->header = node->next;
		delete node;
	}
};

template<class O, class I>
class Entry {
public:
	O object;
	I data;
};

template<class O, class I>
class EntryVersion {
public:
	O object;
	I data;
	int version;
};

template<class I>
class Set {
public:
	UnsafeList<Entry<void**, I>*> elements;
	void add(void** o, I data) {
		Entry<void**, I>* entry = (Entry<void**, I>*)malloc(sizeof(Entry<void**, I>()));
		entry->object = o;
		entry->data = data;
		elements.add(entry);
	}

	I find(void** o, bool &found) {
		//TODO enhance speed, order of N now !
		ListNode<Entry<void**, I>*>* itr = elements.header;
		I val;
		found = false;
		while (itr != NULL) {
			if (itr->value->object == o){
				found = true;
				val = itr->value->data;
			}
			itr = itr->next;
		}
		return found ? val : (void*)-1;
	}

	void reset() {
		elements.reset();
	}
};

template<class I>
class ReversedSet {
public:
	UnsafeReversedList<Entry<void**, I>*> elements;
	void add(void** o, I data) {
		Entry<void**, I>* entry = (Entry<void**, I>*)malloc(sizeof(Entry<void**, I>()));
		entry->object = o;
		entry->data = data;
		elements.add(entry);
	}

	I find(void** o, bool &found) {
		//TODO enhance speed, order of N now !
		ListNode<Entry<void**, I>*>* itr = elements.header;
		while (itr != NULL) {
			if (itr->value->object == o){
				found = true;
				return itr->value->data;
			}
			itr = itr->next;
		}
		found = false;
		return (void*)-1;
	}

	void reset() {
		elements.reset();
	}
};

#define SIGNATURE_LENGTH 	8
static int SIG_MASK = SIGNATURE_LENGTH * 64 - 1;

template<class I>
class Signature{
	uint64_t signature[SIGNATURE_LENGTH];
	bool empty;
public:
	Signature() : empty(true){
		for(int i=0; i<SIGNATURE_LENGTH; i++){
			signature[i] = 0;
		}
	}
	void reset(){
		empty = true;
		for(int i=0; i<SIGNATURE_LENGTH; i++){
			signature[i] = 0;
		}
	}
	std::hash<I> std_hash;
	TM_INLINE inline int insert(I addr){
		int index = std_hash(addr) & SIG_MASK;
//		LOG("HASH " << addr << " " << index << " " << (long)std_hash(addr));
		int byte_index = index >> 6;
		int bit_index = index & 0x3F;
		signature[byte_index] |= (1ULL << bit_index);
//		LOG(byte_index << "\t" << bit_index << "\t" << signature[byte_index])
		empty = false;
		return index;
	}
	TM_INLINE inline bool exists(I addr){
		if(empty) return false;
		int index = std_hash(addr) & SIG_MASK;
//		LOG(addr << " " << index);
		int byte_index = index >> 6;
		int bit_index = index & 0x3F;
		return signature[byte_index] & (1ULL << bit_index);
	}
	bool intersec(Signature* s){
		for(int i=0; i<SIGNATURE_LENGTH; i++){
			if(s->signature[i] & signature[i]){
				DEBUG("HASH INTERSECT " << i);
				return true;
			}
		}
		return false;
	}
	void copy(Signature* s){
		for(int i=0; i<SIGNATURE_LENGTH; i++){
			signature[i] = s->signature[i];
		}
	}
};

#define BUCKET_SIZE		8
#define BUCKET_MASK		7

template<class I>
class BucketList {
public:
	int size;
	UnsafeList<I*> elements;
	ListNode<I*>* reusable;
#ifdef STAT2
	int adds = -1;
	int allocas = -1;
#endif

	BucketList() : size(0), reusable(NULL) {};

	inline int getBucketSize(bool lastBucket){
		if(!lastBucket)
			return BUCKET_SIZE;
		int firstSize = size&BUCKET_MASK;
		if(firstSize > 0)
			return firstSize;
		return size==0 ? 0 : BUCKET_SIZE;
	}

	void add(I data) {
#ifdef STAT2
		adds++;
#endif
		int index = size&BUCKET_MASK;
		I* bucket;
		if(index == 0){
			if(reusable==NULL){
#ifdef STAT2
				allocas++;
#endif
				bucket = (I*)malloc(sizeof(I) * BUCKET_SIZE);
				elements.add(bucket);
			}else{
				bucket = reusable->value;
				ListNode<I*>* reusableEntry = reusable;
				reusable = reusableEntry->next;
				elements.add(reusableEntry);
			}
		}else{
			bucket = elements.tail->value;
		}
		bucket[index] = data;
		size++;
	}

	void reset() {
		reusable = elements.header;
		elements.header = NULL;
		elements.tail = NULL;
		size = 0;
	}
};

template<class I>
class BucketSet {
public:
	int size;
	UnsafeList<Entry<I*, I>*> elements;
	ListNode<Entry<I*, I>*>* reusable;
#ifdef STAT2
	int adds = -1;
	int allocas = -1;
#endif

	BucketSet() : size(0), reusable(NULL) {};

	inline int getBucketSize(bool lastBucket){
		if(!lastBucket)
			return BUCKET_SIZE;
		int firstSize = size&BUCKET_MASK;
		if(firstSize > 0)
			return firstSize;
		return size==0 ? 0 : BUCKET_SIZE;
	}

	void add(I* o, I data) {
#ifdef STAT2
		adds++;
#endif
		int index = size&BUCKET_MASK;
		Entry<I*, I>* bucket;
		if(index == 0){
			if(reusable==NULL){
#ifdef STAT2
				allocas++;
#endif
				bucket = (Entry<I*, I>*)malloc(sizeof(Entry<I*, I>) * BUCKET_SIZE);
				elements.add(bucket);
			}else{
				bucket = reusable->value;
				ListNode<Entry<I*, I>*>* reusableEntry = reusable;
				reusable = reusableEntry->next;
				elements.add(reusableEntry);
			}
		}else{
			bucket = elements.tail->value;
		}
		bucket[index].object = o;
		bucket[index].data = data;
		size++;
	}

	void reset() {
		reusable = elements.header;
		elements.header = NULL;
		elements.tail = NULL;
		size = 0;
	}
};

template<class K, class I, class T>
class BloomBucketSet {
public:
	Signature<K> signature;
	int size = 0;
	UnsafeList<T*> elements;
	ListNode<T*>* reusable;
#ifdef STAT2
	int finds = -1;
	int sig = -1;
	int adds = -1;
	int allocas = -1;
#endif

	BloomBucketSet() : size(0), reusable(NULL) {};

	inline int getBucketSize(bool lastBucket){
		if(!lastBucket)
			return BUCKET_SIZE;
		int firstSize = size&BUCKET_MASK;
		if(firstSize > 0)
			return firstSize;
		return size==0 ? 0 : BUCKET_SIZE;
	}

	void add(K o, I data) {
#ifdef STAT2
		adds++;
#endif
		int index = size&BUCKET_MASK;
		T* bucket;
		if(index == 0){
			if(reusable==NULL){
#ifdef STAT2
				allocas++;
#endif
				bucket = (T*)malloc(sizeof(T) * BUCKET_SIZE);
				elements.add(bucket);
			}else{
				bucket = reusable->value;
				ListNode<T*>* reusableEntry =reusable;
				reusable = reusableEntry->next;
				elements.add(reusableEntry);
			}
		}else{
			bucket = elements.tail->value;
		}
		bucket[index].object = o;
		bucket[index].data = data;
		signature.insert(o);
		size++;
	}

	I find(K o, bool &found) {
		I value;
		found = false;
		if(signature.exists(o)){
#ifdef STAT2
			finds++;
#endif
			ListNode<T*>* itr = elements.header;
			while (itr != NULL) {
				T* bucket = itr->value;
				int bucket_size = getBucketSize(itr->next == NULL);
				for(int i=0; i<bucket_size; i++){
					if (bucket[i].object == o){
						found = true;
						value = bucket[i].data;
					}
				}
				itr = itr->next;
			}
		}else{
#ifdef STAT2
			sig++;
#endif
		}
		return value;
	}

	void reset() {
		reusable = elements.header;
		elements.header = NULL;
		elements.tail = NULL;
		signature.reset();
		size = 0;
	}
};

#define HASHSET_SIZE	128

template<class I>
class HashSet {
public:
	Signature<I*> signature;
	int capacity = HASHSET_SIZE;
	int size = 0;
	int mask;
	Entry<I*, I>* elements;
	bool* full;
#ifdef STAT2
	int finds = -1;
	int sig = -1;
	int adds = -1;
	int allocas = -1;
#endif
	HashSet(){
		elements = (Entry<I*, I>*)malloc(sizeof(Entry<I*, I>) * capacity);
		full = (bool*)malloc(sizeof(bool) * capacity);
		mask = capacity - 1;
		for(int i=0; i<capacity; i++)
			full[i] = false;
	}

	void add(I* o, I data) {
#ifdef STAT2
		adds++;
#endif
		int index = signature.insert(o);
		int i;
		// TODO rehash if full
		while(full[(i = (index & mask))]) index++;
		elements[i].object = o;
		elements[i].data = data;
		full[i] = true;
		size++;
	}

	I find(I* o, bool &found) {
		if(signature.exists(o)){
#ifdef STAT2
			finds++;
#endif
			int index = signature.std_hash(o);
			int i;
			while(full[(i = (index & mask))]){
				if (elements[i].object == o){
					found = true;
					return elements[i].data;
				}
				index++;
			}
		}else{
#ifdef STAT2
			sig++;
#endif
		}
		found = false;
		return (void*)-1;
	}

	void reset() {
		for(int i=0; i<capacity; i++)
			full[i] = false;
		signature.reset();
		size = 0;
	}
};

inline long long getElapsedTime()
{
    struct timespec t;
    clock_gettime(CLOCK_REALTIME, &t);

    return (((long long)t.tv_sec) * 1000000000L) + ((long long)t.tv_nsec);
}

static void stick_worker(int proc_num) {
	if(proc_num >=0 ){
		cpu_set_t set;
		CPU_ZERO(&set);
		CPU_SET(proc_num, &set);

		pid_t tid = syscall( __NR_gettid);
		if (sched_setaffinity(tid, sizeof(cpu_set_t), &set)) {
			std::cerr << "sched_setaffinity fails " << tid << std::endl;
			perror("sched_setaffinity fails");
		}
	}

	unsigned int* local_seed = (unsigned int*)malloc(sizeof(unsigned int));
	*local_seed = (proc_num+1)*getElapsedTime();
	pthread_setspecific(rand_key, (void*)local_seed);
}

static int get_cpu_workers(){
	return (int) sysconf( _SC_NPROCESSORS_ONLN);
}


static void threadpool_memory_violation_handler(int sigNumber) {
	LOG("Thread (signal " << sigNumber << ")");
	exit(1);
}

static long init_rand(){
	pthread_key_create(&rand_key, NULL);
}

static long run_cpu_workers(void* (func)(void*), int workers, int sched) {
	pthread_t *thrs = (pthread_t *) malloc(sizeof(pthread_t) * workers); // Getting number of CPUs
	init_rand();
	std::cout << "Register SIG Handler" << std::endl;
	struct sigaction actions;
	memset(&actions, 0, sizeof(actions));
	sigemptyset(&actions.sa_mask);
	actions.sa_flags = 0;
	actions.sa_handler = threadpool_memory_violation_handler;
	sigaction(SIGILL, &actions, NULL); // Illegal Instruction
	sigaction(SIGFPE, &actions, NULL); // Floating point exception
	sigaction(SIGSEGV, &actions, NULL); // Invalid memory reference

	for (int i = 0; i < workers; i++) {
		pthread_attr_t custom_sched_attr;

		pthread_attr_init(&custom_sched_attr);
		pthread_attr_setscope(&custom_sched_attr, PTHREAD_SCOPE_SYSTEM);
		pthread_attr_setinheritsched(&custom_sched_attr, PTHREAD_EXPLICIT_SCHED);

		if(sched >= 0){
			int max_prio, min_prio;
			struct sched_param s_param;
			pthread_attr_setschedpolicy(&custom_sched_attr, sched);
			min_prio = sched_get_priority_min(sched);
			max_prio = sched_get_priority_max(sched);
			INFO("Sched= " << sched << " range " << min_prio << ":" << max_prio);
//			s_param.sched_priority = min_prio + w;
//			s_param.sched_priority = max_prio - w;
			s_param.sched_priority = max_prio;
			pthread_attr_setschedparam(&custom_sched_attr, &s_param);
		}

		int ret = pthread_create(&thrs[i], &custom_sched_attr, func, (void *) (long) i);
		if (ret != 0) {
			printf("pthread create fails\n");
			if (ret == EAGAIN) {
				printf("EAGAIN\n");
			}
			if (ret == EINVAL) {
				printf("EINVAL\n");
			}
			if (ret == EPERM) {
				printf("EPERM\n");
			}
			workers = i;
			break;
		}
	}

	long result = 0;
	for (int i = 0; i < workers; i++){
		void* r;
		pthread_join(thrs[i], &r);
		std::cout << "Thread " << i << ", Result:" << (long)r << std::endl;
		result += (long)(r);
	}

	free(thrs);

	return result;
}

//static unsigned int static_seed = getElapsedTime();

static void
rand_set_seed(int seed){
	*((unsigned int*)pthread_getspecific(rand_key)) = seed;
}

static int
rand_r_32 ()
{
	unsigned int *seed = (unsigned int*)pthread_getspecific(rand_key);
    unsigned int next = *seed;
    int result;

    next *= 1103515245;
    next += 12345;
    result = (unsigned int) (next / 65536) % 2048;

    next *= 1103515245;
    next += 12345;
    result <<= 10;
    result ^= (unsigned int) (next / 65536) % 1024;

    next *= 1103515245;
    next += 12345;
    result <<= 10;
    result ^= (unsigned int) (next / 65536) % 1024;

    *seed = next;

    return result;
}


}

#endif
