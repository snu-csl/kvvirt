#include "zipf.h"
#include <math.h>
#include <mutex>
#include <stdlib.h>
#include <time.h>
#include <stdio.h>

/* zipf - from https://bitbucket.org/theoanab/rocksdb-ycsb/src/master/util/zipf.h */
static long items; //initialized in init_zipf_generator function
static long base; //initialized in init_zipf_generator function
static double zipfianconstant; //initialized in init_zipf_generator function
static double alpha; //initialized in init_zipf_generator function
static double zetan; //initialized in init_zipf_generator function
static double eta; //initialized in init_zipf_generator function
static double theta; //initialized in init_zipf_generator function
static double zeta2theta; //initialized in init_zipf_generator function
static long countforzeta; //initialized in init_zipf_generator function

std::mutex mutex_;
void init_zipf_generator(long min, long max);
double zeta(long st, long n, double initialsum);
double zetastatic(long st, long n, double initialsum);
long next_long(long itemcount);
long zipf_next();
void set_last_value(long val);

static unsigned int __thread seed;
static __uint128_t __thread g_lehmer64_state;
void init_seed(void) {
   seed = rand();
   g_lehmer64_state = seed;
}

unsigned long long lehmer64() {
  return rand_r(&seed);
}

void init_zipf_generator(long min, long max, double coef){
	items = max-min+1;
	base = min;
	zipfianconstant = coef;
	theta = zipfianconstant;
	zeta2theta = zeta(0, 2, 0);
	alpha = 1.0/(1.0-theta);
	zetan = zetastatic(0, max-min+1, 0);
	countforzeta = items;
	eta=(1 - pow(2.0/items,1-theta) )/(1-zeta2theta/zetan);

	zipf_next();
}


double zeta(long st, long n, double initialsum) {
	countforzeta=n;
	return zetastatic(st,n,initialsum);
}

//initialsum is the value of zeta we are computing incrementally from
double zetastatic(long st, long n, double initialsum){
	double sum=initialsum;
	for (long i=st; i<n; i++){
		sum+=1/(pow(i+1,theta));
	}
	return sum;
}

long next_long(long itemcount){
	//from "Quickly Generating Billion-Record Synthetic Databases", Jim Gray et al, SIGMOD 1994
	if (itemcount!=countforzeta){
		if (itemcount>countforzeta){
            std::lock_guard<std::mutex> lock(mutex_);
			//printf("WARNING: Incrementally recomputing Zipfian distribtion. (itemcount= %ld; countforzeta= %ld)", itemcount, countforzeta);
			//we have added more items. can compute zetan incrementally, which is cheaper
			zetan = zeta(countforzeta,itemcount,zetan);
			eta = ( 1 - pow(2.0/items,1-theta) ) / (1-zeta2theta/zetan);
		}
	}

   //double u = (double)(rand_r(&seed)%RAND_MAX) / ((double)RAND_MAX);
	double u = (double)(lehmer64()%RAND_MAX) / ((double)RAND_MAX);
	double uz=u*zetan;
	if (uz < 1.0){
		return base;
	}

	if (uz<1.0 + pow(0.5,theta)) {
		return base + 1;
	}
	long ret = base + (long)((itemcount) * pow(eta*u - eta + 1, alpha));
	return ret;
}

long zipf_next() {
	return next_long(items);
}
