/*
 * blackscholes.cpp
 *
 *  Created on: Feb 26, 2018
 *      Author: root
 */

#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <string.h>
#include <iostream>

#include <pthread.h>
#include <time.h>

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

using namespace std;

#define MAX_THREADS 128

pthread_t _M4_threadsTable[MAX_THREADS];
int _M4_threadsTableAllocated[MAX_THREADS];
pthread_mutexattr_t _M4_normalMutexAttr;
int _M4_numThreads = MAX_THREADS;

//Precision to use for calculations
#define fptype float

#define NUM_RUNS 10

typedef struct OptionData_ {
        fptype s;          // spot price
        fptype strike;     // strike price
        fptype r;          // risk-free interest rate
        fptype divq;       // dividend rate
        fptype v;          // volatility
        fptype t;          // time to maturity or option expiration in years
                           //     (1yr = 1.0, 6mos = 0.5, 3mos = 0.25, ..., etc)
        char OptionType;   // Option type.  "P"=PUT, "C"=CALL
        fptype divs;       // dividend vals (not used in this test)
        fptype DGrefval;   // DerivaGem Reference Value
} OptionData;

OptionData *data;
int numOptions;

int    * otype;
fptype * sptprice;
fptype * strike;
fptype * rate;
fptype * volatility;
fptype * otime;
int numError = 0;
static int nThreads;

fptype *prices;

int groupCount = 1024;

////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////
// Cumulative Normal Distribution Function
// See Hull, Section 11.8, P.243-244
#define inv_sqrt_2xPI 0.39894228040143270286

fptype CNDF ( fptype InputX )
{
    int sign;

    fptype OutputX;
    fptype xInput;
    fptype xNPrimeofX;
    fptype expValues;
    fptype xK2;
    fptype xK2_2, xK2_3;
    fptype xK2_4, xK2_5;
    fptype xLocal, xLocal_1;
    fptype xLocal_2, xLocal_3;

    // Check for negative value of InputX
    if (InputX < 0.0) {
        InputX = -InputX;
        sign = 1;
    } else
        sign = 0;

    xInput = InputX;

    // Compute NPrimeX term common to both four & six decimal accuracy calcs
    expValues = exp(-0.5f * InputX * InputX);
    xNPrimeofX = expValues;
    xNPrimeofX = xNPrimeofX * inv_sqrt_2xPI;

    xK2 = 0.2316419 * xInput;
    xK2 = 1.0 + xK2;
    xK2 = 1.0 / xK2;
    xK2_2 = xK2 * xK2;
    xK2_3 = xK2_2 * xK2;
    xK2_4 = xK2_3 * xK2;
    xK2_5 = xK2_4 * xK2;

    xLocal_1 = xK2 * 0.319381530;
    xLocal_2 = xK2_2 * (-0.356563782);
    xLocal_3 = xK2_3 * 1.781477937;
    xLocal_2 = xLocal_2 + xLocal_3;
    xLocal_3 = xK2_4 * (-1.821255978);
    xLocal_2 = xLocal_2 + xLocal_3;
    xLocal_3 = xK2_5 * 1.330274429;
    xLocal_2 = xLocal_2 + xLocal_3;

    xLocal_1 = xLocal_2 + xLocal_1;
    xLocal   = xLocal_1 * xNPrimeofX;
    xLocal   = 1.0 - xLocal;

    OutputX  = xLocal;

    if (sign) {
        OutputX = 1.0 - OutputX;
    }

    return OutputX;
}


/*calculation*/
fptype BlkSchlsEqEuroNoDiv( fptype sptprice,
                            fptype strike, fptype rate, fptype volatility,
                            fptype time, int otype, float timet )
{
    fptype OptionPrice;

    // local private working variables for the calculation
    fptype xStockPrice;
    fptype xStrikePrice;
    fptype xRiskFreeRate;
    fptype xVolatility;
    fptype xTime;
    fptype xSqrtTime;

    fptype logValues;
    fptype xLogTerm;
    fptype xD1;
    fptype xD2;
    fptype xPowerTerm;
    fptype xDen;
    fptype d1;
    fptype d2;
    fptype FutureValueX;
    fptype NofXd1;
    fptype NofXd2;
    fptype NegNofXd1;
    fptype NegNofXd2;

    xStockPrice = sptprice;
    xStrikePrice = strike;
    xRiskFreeRate = rate;
    xVolatility = volatility;

    xTime = time;
    xSqrtTime = sqrt(xTime);

    logValues = log( sptprice / strike );

    xLogTerm = logValues;


    xPowerTerm = xVolatility * xVolatility;
    xPowerTerm = xPowerTerm * 0.5;

    xD1 = xRiskFreeRate + xPowerTerm;
    xD1 = xD1 * xTime;
    xD1 = xD1 + xLogTerm;

    xDen = xVolatility * xSqrtTime;
    xD1 = xD1 / xDen;
    xD2 = xD1 -  xDen;

    d1 = xD1;
    d2 = xD2;

    NofXd1 = CNDF( d1 );
    NofXd2 = CNDF( d2 );

    FutureValueX = strike * ( exp( -(rate)*(time) ) );
    if (otype == 0) {
        OptionPrice = (sptprice * NofXd1) - (FutureValueX * NofXd2);
    } else {
        NegNofXd1 = (1.0 - NofXd1);
        NegNofXd2 = (1.0 - NofXd2);
        OptionPrice = (FutureValueX * NegNofXd2) - (sptprice * NegNofXd1);
    }

    return OptionPrice;
}

static void transactional_work(void **args) {
	int workers_count = O_API::get_workers_count();
	int i = *(int*)args[0];
	fptype* price = (fptype*)args[1];
	OTM_BEGIN();

	for (int j = 0; j < groupCount * workers_count; ++j) {
		tx->write_f((fptype*)&prices[i], price[j]);
		i += workers_count;
	}
	//tx->write_f((fptype*)&prices[index], price);
	//OTM_SHARED_WRITE_F((fptype*)&prices[index], price);
	OTM_END();
}

void bs_thread(void *arg) {
 	int tid = thread_getId();
	LOG("Thread " << tid);
	int workers_count = O_API::get_workers_count();
	if (workers_count < 0)
		workers_count = 1;

	long long startTime = getElapsedTime();

#ifdef ENABLE_THREADS

	int numTransactions = numOptions / (groupCount * workers_count);

	int i = tid;

	for (int age = tid; age < numTransactions; age += workers_count) {
		fptype priceGroup[groupCount * workers_count];
		int index = i;
		for (int j = 0; j < groupCount * workers_count; ++j) {
			priceGroup[j] = BlkSchlsEqEuroNoDiv(
					sptprice[i],
					strike[i],
					rate[i],
					volatility[i],
					otime[i],
					otype[i],
					0);;

			i += workers_count;
		}

		void **args = (void**)malloc(sizeof(void*) * 2);
		args[0] = (void*)&index;
		args[1] = (void*)priceGroup;
		O_API::run_in_order(transactional_work, args, age);
	}

	long long endTime = getElapsedTime();

	INFO("Thread " << tid << " time " << (endTime - startTime) / 1000000000.0);
#else
	for (int i = 0; i < numOptions; ++i) {
		fptype price = BlkSchlsEqEuroNoDiv(
				sptprice[i],
				strike[i],
				rate[i],
				volatility[i],
				otime[i],
				otype[i],
				0);
		prices[i] = price;
	}


#endif

#ifdef ENABLE_THREADS

	startTime = getElapsedTime();
	O_API::wait_till_finish();
	endTime = getElapsedTime();
	INFO("Thread " << tid << " wait time " << (endTime - startTime) / 1000000000.0);
#endif
}

int MAIN_BLACKSCHOLES(int argc, char **argv)
{
	INIT_LOG
	cout << "!!!Hello!!!" << endl;

    FILE *file;
    fptype * buffer;
    int * buffer2;
    int rv;




    if (argc != 3)
	{
		printf("Usage:\n\t%s <nthreads> <inputFile>\n", argv[0]);
		exit(1);
	}
	nThreads = atoi(argv[1]);
	char *inputFile = argv[2];

	//Read input data from file
	file = fopen(inputFile, "r");
	if(file == NULL) {
	  printf("ERROR: Unable to open file `%s'.\n", inputFile);
	  exit(1);
	}
	rv = fscanf(file, "%i", &numOptions);
	if(rv != 1) {
	  printf("ERROR: Unable to read from file `%s'.\n", inputFile);
	  fclose(file);
	  exit(1);
	}
	if(nThreads > numOptions) {
	  printf("WARNING: Not enough work, reducing number of threads to match number of options.\n");
	  nThreads = numOptions;
	}

	// alloc spaces for the option data
	data = (OptionData*)malloc(numOptions*sizeof(OptionData));
	prices = (fptype*)malloc(numOptions*sizeof(fptype));
	for (int loopnum = 0; loopnum < numOptions; ++ loopnum )
	{
		rv = fscanf(file, "%f %f %f %f %f %f %c %f %f", &data[loopnum].s, &data[loopnum].strike, &data[loopnum].r, &data[loopnum].divq, &data[loopnum].v, &data[loopnum].t, &data[loopnum].OptionType, &data[loopnum].divs, &data[loopnum].DGrefval);
		if(rv != 9) {
		  printf("ERROR: Unable to read from file `%s'.\n", inputFile);
		  fclose(file);
		  exit(1);
		}
	}
	rv = fclose(file);
	if(rv != 0) {
		printf("ERROR: Unable to close file `%s'.\n", inputFile);
		exit(1);
	}

#ifdef ENABLE_THREADS


	if ((nThreads < 1) || (nThreads > MAX_THREADS))
	{
		fprintf(stderr,"Number of threads must be between 1 and %d.\n", MAX_THREADS);
		exit(1);
	}

	O_API::init(nThreads);
	thread_startup(nThreads);

#else
	if (nThreads != 1)
	{
		fprintf(stderr,"Number of threads must be 1 (serial version)\n");
		exit(1);
	}
#endif //ENABLE_THREADS

	printf("Num of Options: %d\n", numOptions);
	printf("Num of Runs: %d\n", NUM_RUNS);

#define PAD 256
#define LINESIZE 64

	buffer = (fptype *) malloc(5 * numOptions * sizeof(fptype) + PAD);
	sptprice = (fptype *) (((unsigned long long)buffer + PAD) & ~(LINESIZE - 1));
	strike = sptprice + numOptions;
	rate = strike + numOptions;
	volatility = rate + numOptions;
	otime = volatility + numOptions;

	buffer2 = (int *) malloc(numOptions * sizeof(fptype) + PAD);
	otype = (int *) (((unsigned long long)buffer2 + PAD) & ~(LINESIZE - 1));

	for (int i=0; i<numOptions; i++) {
		otype[i]      = (data[i].OptionType == 'P') ? 1 : 0;
		sptprice[i]   = data[i].s;
		strike[i]     = data[i].strike;
		rate[i]       = data[i].r;
		volatility[i] = data[i].v;
		otime[i]      = data[i].t;
	}

	printf("Size of data: %d\n", numOptions * (sizeof(OptionData) + sizeof(int)));

	long long startTime = getElapsedTime();

#ifdef ENABLE_THREADS

	for (int i = 0; i < NUM_RUNS; ++i) {
		thread_start(bs_thread, prices);
	}


	long long endTime = getElapsedTime();
	thread_shutdown();

#else
	int threadID=0;
	bs_thread(prices);
	long long endTime = getElapsedTime();
#endif

	cout << "===================================" << endl;

#ifdef ERR_CHK
	for (int i = 0; i < numOptions; i++) {
		fptype priceDelta = data[i].DGrefval - prices[i];
		if( fabs(priceDelta) >= 1e-4 ){
			printf("Error on %d. Computed=%.5f, Ref=%.5f, Delta=%.5f\n",
				   i, prices[i], data[i].DGrefval, priceDelta);
			numError ++;
		}
	}

	cout << "Num Errors = \t" << numError << endl;
#endif

	//cout << "Algorithm =\t" << algorithm << endl;
	cout << "Time =  \t" << endTime - startTime << endl;
	cout << "Workers = \t" << nThreads << endl;
	//cout << "Cleaners = \t" << cleaners << endl;
	//cout << "Transactions =\t" << workload << endl;
	//cout << "Aborts =\t" << executed - workload << endl;
	cout << "===================================" << endl;
#ifdef ENABLE_THREADS
	O_API::print_statistics();
#endif
	cout << "!!!Bye!!!" << endl;

	return 0;
}

