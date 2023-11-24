#include "rwlock.h"
void rwlock_init(rwlock *rw){
	mutex_init(&rw->lock);
	mutex_init(&rw->cnt_lock);
	rw->readcnt=0;
}

void rwlock_read_lock(rwlock* rw){
	mutex_lock(&rw->cnt_lock);
	rw->readcnt++;
	if(rw->readcnt==1){
		mutex_lock(&rw->lock);
	}
	mutex_unlock(&rw->cnt_lock);
}

void rwlock_read_unlock(rwlock *rw){
	mutex_lock(&rw->cnt_lock);
	rw->readcnt--;
	if(rw->readcnt==0){
		mutex_unlock(&rw->lock);
	}
	mutex_unlock(&rw->cnt_lock);
}

void rwlock_write_lock(rwlock *rw){
	mutex_lock(&rw->lock);
}

void rwlock_write_unlock(rwlock *rw){
	mutex_unlock(&rw->lock);
}
