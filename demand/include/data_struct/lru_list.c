#include "lru_list.h"

#include <linux/slab.h>
#include <linux/vmalloc.h>

void** items;
uint64_t head, tail;


typedef struct {
    size_t head;
    size_t tail;
    size_t size;
    void** data;
} queue_t;
queue_t *example; // = {0, 0, QUEUE_SIZE, malloc(sizeof(void*) * QUEUE_SIZE)};

void* queue_read(queue_t *queue) {
    if (queue->tail == queue->head) {
        return NULL;
    }
    void* handle = queue->data[queue->tail];
    queue->data[queue->tail] = NULL;
    queue->tail = (queue->tail + 1) % queue->size;
    return handle;
}

int queue_write(queue_t *queue, void* handle) {
    if (((queue->head + 1) % queue->size) == queue->tail) {
        return -1;
    }
    queue->data[queue->head] = handle;
    queue->head = (queue->head + 1) % queue->size;
    return 0;
}

void lru_init(LRU** lru){
    example = (queue_t*) kzalloc(sizeof(*example), GFP_KERNEL);
    example->head = example->tail = 0;
    example->size = 1000000;
    example->data = (void*) vmalloc(sizeof(void*) * 1000000);
	*lru = (LRU*)kzalloc(sizeof(LRU), GFP_KERNEL);
	(*lru)->size=0;
	(*lru)->head = (*lru)->tail = NULL;
}

void lru_kfree(LRU* lru){
	while(lru_pop(lru)){}
	kfree(lru);
    vfree(example->data);
}

NODE* lru_push(LRU* lru, void* table_ptr){
    //queue_write(example, table_ptr);
    //return (NODE*) 0xDEADBEEF;

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
    //printk("1 LRU size %u\n", lru->size);
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
    //return queue_read(example);
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
    //printk("2 LRU size %u\n", lru->size);
	return re;
}

void* lru_prev(LRU* lru, NODE* now){
	if(now == NULL){
		return NULL;
	}
	if(now == lru->head){
		return NULL;
	}

    return now->prev->DATA;
}

void lru_update(LRU* lru, NODE* now){
    //return;
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
        BUG_ON(true);
		return;
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
    //printk("LRU size %u\n", lru->size);
	kfree(now);
}
