#ifndef RWLOCK_HEADER
#define RWLOCK_HEADER

#include <linux/mutex.h>

typedef struct{
	struct mutex lock;
	struct mutex cnt_lock;
	int readcnt;
}rwlock;

void rwlock_init(rwlock *);
void rwlock_read_lock(rwlock*);
void rwlock_read_unlock(rwlock*);
void rwlock_write_lock(rwlock*);
void rwlock_write_unlock(rwlock*);
#endif
