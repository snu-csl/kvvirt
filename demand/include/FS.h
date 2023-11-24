#ifndef __H_FS__
#define __H_FS__
#define DMARBUF	1
#define DMAWBUF 2

#include <linux/slab.h>

int F_kzalloc(void **,int size, int rw);
void F_kfree(void *, int tag, int rw);
#endif
