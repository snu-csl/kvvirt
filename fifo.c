#include "fifo.h"

spinlock_t fifo_spin;
void fifo_init(struct fifo **queue) {
    *queue = kzalloc(sizeof(struct fifo), GFP_KERNEL);
    INIT_LIST_HEAD(&((*queue)->head));
}

void* fifo_enqueue(struct fifo *queue, void *data) {
    spin_lock(&fifo_spin);
    struct q_entry *new_node = kmalloc(sizeof(*new_node), GFP_KERNEL);
    new_node->data = data;
    list_add_tail(&new_node->list, &queue->head);
    spin_unlock(&fifo_spin);
    return (void*) 0xA;
}

void *fifo_dequeue(struct fifo *queue) {
    spin_lock(&fifo_spin);
    if (list_empty(&queue->head)) {
        spin_unlock(&fifo_spin);
        return NULL;
    }

    struct q_entry *node = list_first_entry(&queue->head, struct q_entry, list);
    void *data = node->data;
    list_del(&node->list);
    kfree(node);
    spin_unlock(&fifo_spin);
    return data;
}

void fifo_destroy(struct fifo *queue) {
    struct q_entry *cur, *temp;
    list_for_each_entry_safe(cur, temp, &queue->head, list) {
        list_del(&cur->list);
        kfree(cur);
    }
}
