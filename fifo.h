#include <linux/kfifo.h>
#include <linux/slab.h>

#include "demand/demand.h"

struct q_entry {
    void *data;
    struct list_head list;
};

struct fifo {
    struct list_head head;
};

void fifo_init(struct fifo **queue);
void* fifo_enqueue(struct fifo *queue, void *data);
void *fifo_dequeue(struct fifo *queue);
void fifo_destroy(struct fifo *queue);
