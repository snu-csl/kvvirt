#include "fifo.h"

void fifo_init(struct fifo *queue) {
    INIT_LIST_HEAD(&queue->head);
}

void* fifo_enqueue(struct fifo *queue, void *data) {
    struct q_entry *new_node = kmalloc(sizeof(*new_node), GFP_KERNEL);
    new_node->data = data;
    list_add_tail(&new_node->list, &queue->head);
    return (void*) 0xDEADBEEF;
}

void *fifo_dequeue(struct fifo *queue) {
    if (list_empty(&queue->head)) {
        return NULL;
    }

    struct q_entry *node = list_first_entry(&queue->head, struct q_entry, list);
    void *data = node->data;
    list_del(&node->list);
    kfree(node);
    return data;
}

void fifo_destroy(struct fifo *queue) {
    struct q_entry *cur, *temp;
    list_for_each_entry_safe(cur, temp, &queue->head, list) {
        list_del(&cur->list);
        kfree(cur);
    }
}
