#include "zipf.h"
#include "latest-generator.h"
#include <math.h>
#include <stdlib.h>
#include <time.h>
#include <stdio.h>

long last_value_latestgen;
long count_basis_latestgen;

// init_val should be the same parameter as the one the zipf generator is initialized with
void init_latestgen(long init_val){
	count_basis_latestgen = init_val;
	//should init the zipf generator here, but it is already initialized
	next_value_latestgen(init_val);
}

long next_value_latestgen(unsigned long long max){
	long next = max - next_long(max);
	last_value_latestgen = next;
	return next;
}
