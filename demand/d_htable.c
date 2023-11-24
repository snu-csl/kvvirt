#include "d_htable.h"

struct d_htable *d_htable_init(int max) {
	struct d_htable *ht = (struct d_htable *)kzalloc(sizeof(struct d_htable), GFP_KERNEL);
	ht->bucket = (struct d_hnode *)kzalloc(max * sizeof(struct d_hnode), GFP_KERNEL);
	ht->max = max;
	return ht;
}

void d_htable_kfree(struct d_htable *ht) {
	for (int i = 0; i < ht->max; i++) {
		struct d_hnode *hn = &ht->bucket[i];
		struct d_hnode *next = hn->next;

		while (next != NULL) {
			hn = next;
			next = hn->next;
			kfree(hn);
		}
	}
	kfree(ht->bucket);
	kfree(ht);
}

int d_htable_insert(struct d_htable *ht, ppa_t ppa, lpa_t lpa) {
	struct d_hnode *hn = &ht->bucket[ppa%ht->max];
	while (hn->next != NULL) {
		hn = hn->next;
	}
	hn->next = (struct d_hnode *)kzalloc(sizeof(struct d_hnode), GFP_KERNEL);
	hn->next->item = lpa;
	hn->next->next = NULL;
	return 0;
}

int d_htable_find(struct d_htable *ht, ppa_t ppa, lpa_t lpa) {
	struct d_hnode *hn = &ht->bucket[ppa%ht->max];
	while (hn->next != NULL) {
		if (hn->next->item == lpa) {
			return 1;
		}
		hn = hn->next;
	}
	return 0;
}

