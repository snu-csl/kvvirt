#include "fifo.h"

#include <linux/kernel.h>
#include <linux/module.h>
#include <linux/slab.h>

#define RING_BUFFER_SIZE 131072 // Define your required size

struct ring_buffer {
    struct cmt_struct* buffer[RING_BUFFER_SIZE];
    int head;
    int tail;
    spinlock_t lock;
};

static struct ring_buffer rb;

void rb_init(struct ring_buffer *rb) {
    rb->head = 0;
    rb->tail = 0;
    spin_lock_init(&rb->lock);
}

int rb_is_full(struct ring_buffer *rb) {
    return ((rb->head + 1) % RING_BUFFER_SIZE) == rb->tail;
}

int rb_is_empty(struct ring_buffer *rb) {
    return rb->head == rb->tail;
}

int rb_push(struct ring_buffer *rb, struct cmt_struct *data) {
    unsigned long flags;

    //spin_lock_irqsave(&rb->lock, flags);

    if (rb_is_full(rb)) {
        //spin_unlock_irqrestore(&rb->lock, flags);
        return -1; // Buffer is full
    }

    //NVMEV_ERROR("Placing CMT IDX %u at %d\n", data->idx, rb->head);

    rb->buffer[rb->head] = data;
    rb->head = (rb->head + 1) % RING_BUFFER_SIZE;

    //spin_unlock_irqrestore(&rb->lock, flags);
    return 0; // Success
}

struct cmt_struct *rb_pop(struct ring_buffer *rb) {
    void* ret;
    unsigned long flags;

    //spin_lock_irqsave(&rb->lock, flags);

    if (rb_is_empty(rb)) {
        //spin_unlock_irqrestore(&rb->lock, flags);
        return NULL; // Buffer is empty
    }

    ret = rb->buffer[rb->tail];
    rb->tail = (rb->tail + 1) % RING_BUFFER_SIZE;

    NVMEV_INFO("Popping CMT IDX %u from %d\n", 
                 ((struct cmt_struct*) ret)->idx, rb->tail);

    //spin_unlock_irqrestore(&rb->lock, flags);
    return ret; // Success
}

void rb_free(struct ring_buffer *rb) {
    // If your cmt_struct contains dynamically allocated memory, free it here
}

spinlock_t fifo_spin;
void fifo_init(struct fifo **queue) {
    rb_init(&rb);	
    //*queue = kzalloc(sizeof(struct fifo), GFP_KERNEL);
    //INIT_LIST_HEAD(&((*queue)->head));
}

void* fifo_enqueue(struct fifo *queue, void *data) {
    spin_lock(&fifo_spin);
    rb_push(&rb, (struct cmt_struct*) data);
    spin_unlock(&fifo_spin);
    //struct q_entry *new_node = kmalloc(sizeof(*new_node), GFP_KERNEL);
    //new_node->data = data;
    //list_add_tail(&new_node->list, &queue->head);
    return (void*) 0xA;
}

void *fifo_dequeue(struct fifo *queue) {
    void *ret;
    spin_lock(&fifo_spin);
    if(rb_is_empty(&rb)) {
        spin_unlock(&fifo_spin);
        return NULL;
    }

    ret = rb_pop(&rb);
    spin_unlock(&fifo_spin);
    return ret;
    //if (list_empty(&queue->head)) {
    //    spin_unlock(&fifo_spin);
    //    return NULL;
    //}

    //struct q_entry *node = list_first_entry(&queue->head, struct q_entry, list);
    //void *data = node->data;
    //list_del(&node->list);
    //kfree(node);
    //spin_unlock(&fifo_spin);
    //return data;
}

void fifo_destroy(struct fifo *queue) {
    return;
    struct q_entry *cur, *temp;
    list_for_each_entry_safe(cur, temp, &queue->head, list) {
        list_del(&cur->list);
        kfree(cur);
    }
}
