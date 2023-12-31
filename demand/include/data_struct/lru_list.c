#include "lru_list.h"

void lru_init(LRU** lru){
	*lru = (LRU*)kzalloc(sizeof(LRU), GFP_KERNEL);
	(*lru)->size=0;
	(*lru)->head = (*lru)->tail = NULL;
}

void lru_kfree(LRU* lru){
	while(lru_pop(lru)){}
	kfree(lru);
}

NODE* lru_push(LRU* lru, void* table_ptr){
	NODE *now = (NODE*)kzalloc(sizeof(NODE), GFP_KERNEL);
	now->DATA = table_ptr;
	now->next = now->prev = NULL;
	if(lru->size == 0){
		lru->head = lru->tail = now;
	}
	else{
		lru->head->prev = now;
		now->next = lru->head;
		lru->head = now;
	}
	lru->size++;
	return now;
}

void* lru_peek(LRU* lru){
	if(!lru->head || lru->size == 0){
		return NULL;
	}
	NODE *now = lru->tail;
	void *re = now->DATA;
	return re;
}

void* lru_pop(LRU* lru){
	if(!lru->head || lru->size == 0){
		return NULL;
	}
	NODE *now = lru->tail;
	void *re = now->DATA;
	lru->tail = now->prev;
	if(lru->tail != NULL){
		lru->tail->next = NULL;
	}
	else{
		lru->head = NULL;
	}
	lru->size--;
	kfree(now);
	return re;
}

void* lru_it(LRU* lru, NODE** it) {
    void *ret = NULL;
	if(!lru->head || lru->size == 0){
		return NULL;
	}
    if(!(*it)) {
        *it = lru->head->next;
        return lru->head->DATA;
    } else if((*it) == lru->tail) {
        return NULL;
    } else {
        ret = (*it)->next->DATA;
        *it = (*it)->next;
        return ret;
    }
}

void lru_update(LRU* lru, NODE* now){
	if(now == NULL){
		return ;
	}
	if(now == lru->head){
		return ;
	}
	if(now == lru->tail){
		lru->tail = now->prev;
		lru->tail->next = NULL;
	}
	else{
		now->prev->next = now->next;
		now->next->prev = now->prev;
	}
	now->prev = NULL;
	lru->head->prev = now;
	now->next = lru->head;
	lru->head = now;
}

void lru_delete(LRU* lru, NODE* now){
	if(now == NULL){
		return ;
	}
	if(now == lru->head){
		lru->head = now->next;
		if(lru->head != NULL){
			lru->head->prev = NULL;
		}
		else{
			lru->tail = NULL;
		}
	}
	else if(now == lru->tail){
		lru->tail = now->prev;
		lru->tail->next = NULL;
	}
	else{
		now->prev->next = now->next;
		now->next->prev = now->prev;
	}	
	lru->size--;
	kfree(now);
}
