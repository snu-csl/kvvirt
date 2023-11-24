#ifndef __SEM_LOCK_H__
#define __SEM_LOCK_H__
#include <linux/semaphore.h>
#define fdriver_lock_t struct semaphore
#define fdriver_mutex_init(val) sem_init(val,0,1)
#define fdriver_lock_init(a,val) sem_init(a,0,val)
#define fdriver_lock(a) sem_wait(a)
#define fdriver_try_lock(a) sem_trywait(a)
#define fdriver_unlock(a) sem_post(a)
#define fdriver_destroy(a) sem_destroy(a)
#endif
