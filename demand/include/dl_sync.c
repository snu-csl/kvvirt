#include "dl_sync.h"
#include "settings.h"
void dl_sync_init(dl_sync *s, uint64_t cnt){
#ifdef SPINSYNC
	s->target_cnt=cnt;
	s->now_cnt=0;
#else
	mutex_init(&s->mutex_sync);
	mutex_lock(&s->mutex_sync);
#endif
}
//static int max_cnt, cnt;
void dl_sync_wait(dl_sync*s){
#ifdef SPINSYNC

//	cnt=0;
	while(s->target_cnt!=s->now_cnt){
		//cnt++;
	}
	/*
	if(max_cnt<cnt){
		printk("%d\n",cnt);
		max_cnt=cnt;
	}*/
	s->now_cnt=0;
#else
	mutex_lock(&s->mutex_sync);
#endif
}

void dl_sync_arrive(dl_sync*s){
#ifdef SPINSYNC
	s->now_cnt++;
#else
	mutex_unlock(&s->mutex_sync);
#endif
}

void dl_syncM_init(dl_sync_m *s, uint64_t cnt){
	s->target_cnt=cnt;
	s->now_cnt=0;
	mutex_init(&s->mutex_sync);
	mutex_lock(&s->mutex_sync);
}

void dl_syncM_wait(dl_sync_m* s){
#ifdef SPINSYNC
	while(s->target_cnt!=s->now_cnt){}
	s->now_cnt=0;
#else
	mutex_lock(&s->mutex_sync);
#endif
}

void dl_syncM_arrive(dl_sync_m* s){
	s->now_cnt++;
#ifdef SPINSYNC
#else
	if(s->now_cnt==s->target_cnt){
		mutex_unlock(&s->mutex_sync);
		s->now_cnt=0;
	}
#endif
}

void dl_syncM_cnt_update(dl_sync_m *s, uint64_t cnt){
	s->target_cnt=cnt;
	s->now_cnt=0;
}
