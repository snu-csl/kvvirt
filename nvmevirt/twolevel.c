#include "nvmev.h"
#include "ssd_config.h"

#ifndef ORIGINAL
#include "twolevel.h"
int reloading = 0;

static inline uint64_t __cycles(void)
{
    uint32_t low, high;
    asm volatile ("rdtsc" : "=a" (low), "=d" (high));
    return ((uint64_t)high << 32) | low;
}

void twolevel_init(struct root *root) {
    struct leaf *leaf;
    char* ptr;
    
    ptr = (char*) root;
    root->cnt = ORIG_GLEN - ROOT_G;

    //NVMEV_INFO("Initializing BTree ORIG_GLEN %d ROOT_G %u EPP %lu IN_ROOT %lu IN_LEAF %lu total size %lu\n", 
    //        ORIG_GLEN, ROOT_G, EPP, IN_ROOT, IN_LEAF, 
    //        sizeof(struct root) + (sizeof(struct leaf) * IN_ROOT));

    for(int i = 0; i < IN_ROOT; i++) {
        root->entries[i] = UINT_MAX;
        leaf = (struct leaf*) (ptr + sizeof(struct root) + (sizeof(struct leaf) * i));

        for(int j = 0; j < IN_LEAF; j++) {
            leaf->hidx[j] = UINT_MAX;
            leaf->ppa[j] = UINT_MAX;
        }
    }
}

void twolevel_expand(struct root *root) {
    root->cnt++;
}

static int cmp_hidx(const void *a, const void *b)
{
    const struct leaf_e *la = a;
    const struct leaf_e *lb = b;
    
    if (la->hidx < lb->hidx)
        return -1;
    if (la->hidx > lb->hidx)
        return 1;
    return 0;
}

struct root tmp_root;
struct leaf_e tmp_leaves[IN_ROOT * IN_LEAF];
uint32_t tmp_leaf_idx = 0;

uint32_t __new_roots(uint32_t *root_keys, struct leaf_e* leaves, 
					 uint32_t key_cnt, uint32_t num_leaves) {
    int root_entries = num_leaves;
    int keys_per_leaf = key_cnt / num_leaves;
    int extra_keys = key_cnt % num_leaves;
    int key_index = 0, i;

    if(extra_keys > num_leaves) {
        NVMEV_ERROR("Extra keys %d num_leaves %d key_cnt %u\n", 
                     extra_keys, num_leaves, key_cnt);
    }

    NVMEV_ASSERT(extra_keys <= num_leaves);

    for (i = 0; i < root_entries; i++) {
        key_index += keys_per_leaf - 1;

        if(extra_keys > 0) {
            key_index++;
            extra_keys--;
        }

        if(i < root_entries - 1) {
            root_keys[i] = leaves[key_index].hidx;
        } else {
            root_keys[i] = UINT_MAX;
        }
        key_index++;
    }

    return root_entries;
}

void twolevel_bulk_insert(struct root* root, struct leaf_e *e, uint32_t cnt) {
    struct leaf *l;
    char* ptr;
    uint32_t new_root_keys[IN_ROOT];
    uint32_t cur_root, cur_root_idx, cur_leaf, leaf_idx, idx;

    ptr = (char*) root;
    sort(e, cnt, sizeof(struct leaf_e), cmp_hidx, NULL);

    __new_roots(new_root_keys, e, cnt, root->cnt);

    for(int i = 0; i < root->cnt; i++) {
        root->entries[i] = new_root_keys[i];
    }

    cur_root = new_root_keys[0];
    cur_root_idx = 0;
    cur_leaf = 0;
    leaf_idx = 0;
    idx = 0;
    l = (struct leaf*) (ptr + sizeof(struct root));

    while(1) {
        if(leaf_idx == cnt) {
            break;
        } else if(e[leaf_idx].hidx <= cur_root) {
            NVMEV_ASSERT(e[leaf_idx].hidx > 0);

            l->hidx[idx] = e[leaf_idx].hidx;
            l->ppa[idx] = e[leaf_idx].ppa;

            leaf_idx++;
            idx++;
        } else {
            cur_root_idx++;
            cur_root = new_root_keys[cur_root_idx];
            cur_leaf++;
            idx = 0;

            l = (struct leaf*) (ptr + sizeof(struct root) + 
                               (sizeof(struct leaf) * cur_leaf));
        }
    }

    return;
}

void twolevel_reload(struct ht_section *ht, struct root* root, 
                  uint32_t hidx, uint32_t ppa) {
    uint32_t new_root_cnt = 0, before;
    uint32_t new_root_keys[IN_ROOT];
    struct leaf *tmp_leaf;
    char* ptr;
    int i;

    before = ht->cached_cnt;
    tmp_leaf_idx = 0;
    ptr = (char*) root;
    i = 0;

    reloading = 1;

    for(i = 0; i < root->cnt; i++) {
        tmp_leaf = (struct leaf*) (ptr + sizeof(struct root) + (sizeof(struct leaf) * i));
        for(int j = 0; j < IN_LEAF; j++) {
            if(tmp_leaf->hidx[j] == UINT_MAX) {
                break;
            }

            tmp_leaves[tmp_leaf_idx].hidx = tmp_leaf->hidx[j];
            tmp_leaves[tmp_leaf_idx].ppa = tmp_leaf->ppa[j];

            tmp_leaf->hidx[j] = UINT_MAX;
            tmp_leaf->ppa[j] = UINT_MAX;

            tmp_leaf_idx++;
        }
    }

    if(hidx != UINT_MAX) {
        tmp_leaves[tmp_leaf_idx].hidx = hidx;
        tmp_leaves[tmp_leaf_idx].ppa = ppa;
        tmp_leaf_idx++;
    }

    sort(tmp_leaves, tmp_leaf_idx, sizeof(struct leaf_e), cmp_hidx, NULL);
    ht->cached_cnt -= tmp_leaf_idx;

    /*
     * Get the new set of root keys.
     */
    __new_roots(new_root_keys, tmp_leaves, tmp_leaf_idx, root->cnt);

    /*
     * Copy them to the root.
     */
    for(int i = 0; i < root->cnt; i++) {
        root->entries[i] = new_root_keys[i];
    }

    /*
     * Reinsert. Includes original to-be-inserted pair.
     */
    for(int i = 0; i < tmp_leaf_idx; i++) {
        twolevel_insert(ht, root, tmp_leaves[i].hidx, tmp_leaves[i].ppa, UINT_MAX);
    }

    reloading = 0;
    NVMEV_ASSERT(ht->cached_cnt == before);
    return;
}

int __lower_bound(struct root *root, uint32_t hidx) {
	int left = 0, right = root->cnt - 1, mid;
	while (left <= right) {
		mid = left + (right - left) / 2;
		if (root->entries[mid] < hidx) {
			left = mid + 1;
		} else {
			right = mid - 1;
		}
	}
	return left;
}

void twolevel_direct_read(struct root *root, uint32_t pos, 
                       void* out, uint32_t len) {
    char* ptr;
    ptr = (char*) root;
    memcpy(out, ptr + pos, len);
}

void twolevel_insert(struct ht_section *ht, struct root *root, 
                  uint32_t hidx, uint32_t ppa, uint32_t pos) {
    int i = 0;
    struct leaf *leaf;
    char* ptr;
    uint32_t max;
   
    ptr  = (char*) root;
    max = root->cnt * IN_LEAF;

    if(pos != UINT_MAX) {
        if(ppa == UINT_MAX) {
            ht->cached_cnt--;

            uint32_t my_leaf_idx = (pos / GRAINED_UNIT) - ROOT_G;
            uint32_t my_leaf_bytes = sizeof(struct root) + 
                                     (sizeof(struct leaf) * my_leaf_idx);
            uint32_t my_in_leaf = ((pos - my_leaf_bytes) - 32) / sizeof(uint32_t);
            struct leaf *my_leaf = (struct leaf*) ((char*) root + my_leaf_bytes);

            for(int i = my_in_leaf; i < IN_LEAF - 1; i++) {
                my_leaf->hidx[i] = my_leaf->hidx[i + 1];
                my_leaf->ppa[i] = my_leaf->ppa[i + 1];
            }

            my_leaf->hidx[IN_LEAF - 1] = UINT_MAX;
            my_leaf->ppa[IN_LEAF - 1] = UINT_MAX;

            return;
        }
        memcpy(ptr + pos, &ppa, sizeof(ppa));
        return;
    }

    ht->cached_cnt++;
	i = __lower_bound(root, hidx);

    leaf = (struct leaf*) (ptr + sizeof(struct root) + (sizeof(struct leaf) * i));
    if(leaf->hidx[IN_LEAF - 1] != UINT_MAX) {
        if(reloading) {
            NVMEV_ERROR("Full while reloading. root->cnt %u max %u cached_cnt %u\n", 
                         root->cnt, max, ht->cached_cnt);
        }

        NVMEV_ASSERT(!reloading);
        twolevel_reload(ht, root, hidx, ppa);
        return;
    }

    if((i != root->cnt - 1) && 
       (hidx >= root->entries[i] || root->entries[i] == UINT_MAX))  {
        root->entries[i] = hidx;
    }

    for(int j = 0; j < IN_LEAF; j++) {
        if(leaf->hidx[j] == UINT_MAX) {
            leaf->hidx[j] = hidx;
            leaf->ppa[j] = ppa;
            break;
        }
    }
}

uint32_t twolevel_find(struct root *root, uint32_t hidx, uint32_t *pos) {
    int i, j;
    struct leaf *leaf;
    uint32_t ret;
    char* ptr;

    i = 0;
    ret = UINT_MAX;
    ptr = (char*) root;

    NVMEV_ASSERT(ptr);

	i = __lower_bound(root, hidx);

    if(i == root->cnt) {
        goto out;
    }

    leaf = (struct leaf*) (ptr + sizeof(struct root) + (sizeof(struct leaf) * i));
    for(j = 0; j < IN_LEAF; j++) {
        if(leaf->hidx[j] == hidx) {
            if(pos) {
                *pos = sizeof(struct root) + (sizeof(struct leaf) * i) + 
                       (sizeof(uint32_t) * IN_LEAF) + 
                       (j * sizeof(uint32_t));
            }

            return leaf->ppa[j];
        } else if(leaf->hidx[j] == UINT_MAX) {
            break;
        }
    }

out:
    return ret;
}

#endif
