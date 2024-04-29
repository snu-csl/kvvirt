#include "nvmev.h"
#include "ssd_config.h"

#ifndef ORIGINAL
#include "btree.h"
int reloading = 0;

static inline uint64_t __cycles(void)
{
    uint32_t low, high;
    asm volatile ("rdtsc" : "=a" (low), "=d" (high));
    return ((uint64_t)high << 32) | low;
}

void btree_init(struct root *root) {
    struct leaf *leaf;
    char* ptr;
    
    ptr = (char*) root;
    root->cnt = ORIG_GLEN - ROOT_G;

    NVMEV_INFO("Initializing BTree ORIG_GLEN %d ROOT_G %u EPP %lu IN_ROOT %lu IN_LEAF %lu total size %lu\n", 
            ORIG_GLEN, ROOT_G, EPP, IN_ROOT, IN_LEAF, 
            sizeof(struct root) + (sizeof(struct leaf) * IN_ROOT));

    for(int i = 0; i < IN_ROOT; i++) {
        root->entries[i] = UINT_MAX;
        leaf = (struct leaf*) (ptr + sizeof(struct root) + (sizeof(struct leaf) * i));
        //leaf->cnt = 0;

        for(int j = 0; j < IN_LEAF; j++) {
            leaf->lpa[j] = UINT_MAX;
            leaf->ppa[j] = UINT_MAX;
        }
    }
}

void btree_expand(struct root *root) {
    root->cnt++;
    NVMEV_INFO("Expanded btree to %u leaves.\n", root->cnt);
}

static int cmp_lpa(const void *a, const void *b)
{
    const struct leaf_e *la = a;
    const struct leaf_e *lb = b;
    
    if (la->lpa < lb->lpa)
        return -1;
    if (la->lpa > lb->lpa)
        return 1;
    return 0;
}

struct root tmp_root;
struct leaf_e tmp_leaves[IN_ROOT * IN_LEAF];
uint32_t tmp_leaf_idx = 0;

uint32_t __new_roots(uint32_t *root_keys, struct leaf_e* leaves, 
					 uint32_t key_cnt, uint32_t num_leaves) {
    int root_entries = num_leaves;
    int keys_per_leaf = key_cnt / num_leaves;  // Minimum keys per leaf
    int extra_keys = key_cnt % num_leaves;  // Extra keys after even distribution
    int key_index = 0, i;

    //NVMEV_ERROR("Getting %d new root entries %u total keys %d keys per leaf %u leaves %d extra.\n",
    //            root_entries, key_cnt, keys_per_leaf, num_leaves, extra_keys);
    //NVMEV_ASSERT(extra_keys <= IN_LEAF);

    for (i = 0; i < root_entries; i++) {
        key_index += keys_per_leaf - 1;  // Move to the end of the current leaf's key range

        if(extra_keys > 0) {
            key_index++;
            extra_keys--;
        }

        if(i < root_entries - 1) {
            root_keys[i] = leaves[key_index].lpa;
        } else {
            root_keys[i] = UINT_MAX;
        }
        key_index++;
    }

    return root_entries;
}

void btree_bulk_insert(struct root* root, struct leaf_e *e, uint32_t cnt) {
    struct leaf *l;
    char* ptr;
    uint32_t new_root_keys[IN_ROOT];
    uint32_t cur_root, cur_root_idx, cur_leaf, leaf_idx, idx;

    ptr = (char*) root;
    sort(e, cnt, sizeof(struct leaf_e), cmp_lpa, NULL);

    __new_roots(new_root_keys, e, cnt, root->cnt);

    for(int i = 0; i < root->cnt; i++) {
        //NVMEV_ERROR("Bulk insert copy root key %u\n", new_root_keys[i]);
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
        } else if(e[leaf_idx].lpa <= cur_root) {
            NVMEV_ASSERT(e[leaf_idx].lpa > 0);

            l->lpa[idx] = e[leaf_idx].lpa;
            l->ppa[idx] = e[leaf_idx].ppa;
            //NVMEV_ERROR("Inserted LPA %u PPA %u to leaf %u pos %u cur_root %u\n", 
            //             e[leaf_idx].lpa, e[leaf_idx].ppa, cur_leaf, 
            //             idx, cur_root);
            leaf_idx++;
            idx++;
        } else {
            cur_root_idx++;
            cur_root = new_root_keys[cur_root_idx];
            cur_leaf++;
            idx = 0;

            l = (struct leaf*) (ptr + sizeof(struct root) + 
                               (sizeof(struct leaf) * cur_leaf));

            //NVMEV_ERROR("Moving to new root %u\n", cur_root);
        }
    }

    return;
}

void btree_reload(struct cmt_struct *cmt, struct root* root, 
                  uint32_t lpa, uint32_t ppa) {
    uint32_t new_root_cnt = 0, before;
    uint32_t new_root_keys[IN_ROOT];
    struct leaf *tmp_leaf;
    char* ptr;
    int i;

    before = cmt->cached_cnt;
    tmp_leaf_idx = 0;
    ptr = (char*) root;
    i = 0;

    reloading = 1;

    for(i = 0; i < root->cnt; i++) {
        tmp_leaf = (struct leaf*) (ptr + sizeof(struct root) + (sizeof(struct leaf) * i));
        for(int j = 0; j < IN_LEAF; j++) {
            if(tmp_leaf->lpa[j] == UINT_MAX) {
                break;
            }

            //NVMEV_INFO("Adding leaf %d LPA %u PPA %u to list.\n", 
            //        i, tmp_leaf->lpa[j], tmp_leaf->ppa[j]);
            tmp_leaves[tmp_leaf_idx].lpa = tmp_leaf->lpa[j];
            tmp_leaves[tmp_leaf_idx].ppa = tmp_leaf->ppa[j];

            tmp_leaf->lpa[j] = UINT_MAX;
            tmp_leaf->ppa[j] = UINT_MAX;

            tmp_leaf_idx++;
        }
    }

    if(lpa != UINT_MAX) {
        //NVMEV_INFO("Finally adding LPA %u PPA %u to list.\n", 
        //             lpa, ppa);
        tmp_leaves[tmp_leaf_idx].lpa = lpa;
        tmp_leaves[tmp_leaf_idx].ppa = ppa;
        tmp_leaf_idx++;
    }

    sort(tmp_leaves, tmp_leaf_idx, sizeof(struct leaf_e), cmp_lpa, NULL);
    cmt->cached_cnt -= tmp_leaf_idx;

    NVMEV_INFO("Reduced cached count to %u before upcoming shuffle.\n",
            cmt->cached_cnt);

    //NVMEV_INFO("After sorting:\n");
    //for(int i = 0; i < tmp_leaf_idx; i++) {
    //    NVMEV_INFO("LPA %u PPA %u\n", tmp_leaves[i].lpa, tmp_leaves[i].ppa);
    //}    

    /*
     * Get the new set of root keys.
     */
    __new_roots(new_root_keys, tmp_leaves, tmp_leaf_idx, root->cnt);

    /*
     * Copy them to the root.
     */
    for(int i = 0; i < root->cnt; i++) {
        //NVMEV_INFO("Placing new root LPA %u\n", new_root_keys[i]);
        root->entries[i] = new_root_keys[i];
    }

    /*
     * Clear leaves.
     */
    //memset(ptr + sizeof(struct root), 0x0, sizeof(struct leaf) * root->cnt);

    /*
     * Reinsert. Includes original to-be-inserted pair.
     */
    for(int i = 0; i < tmp_leaf_idx; i++) {
        btree_insert(cmt, root, tmp_leaves[i].lpa, tmp_leaves[i].ppa, UINT_MAX);
    }

    reloading = 0;

    if(cmt->cached_cnt != before) {
        NVMEV_INFO("Mismatch before %d after %d\n", before, cmt->cached_cnt);
    }
    NVMEV_ASSERT(cmt->cached_cnt == before);
    return;
}

int __lower_bound(struct root *root, uint32_t lpa) {
	int left = 0, right = root->cnt - 1, mid;
	while (left <= right) {
		mid = left + (right - left) / 2;
		if (root->entries[mid] < lpa) {
			left = mid + 1;
		} else {
			right = mid - 1;
		}
	}
	return left;
}

void btree_direct_read(struct root *root, uint32_t pos, 
                       void* out, uint32_t len) {
    char* ptr;
    ptr = (char*) root;
    memcpy(out, ptr + pos, len);
}

void btree_insert(struct cmt_struct *cmt, struct root *root, 
                  uint32_t lpa, uint32_t ppa, uint32_t pos) {
    int i = 0;
    struct leaf *leaf;
    char* ptr;
    uint32_t max;
   
    ptr  = (char*) root;
    max = root->cnt * IN_LEAF;

    if(!reloading) {
        //NVMEV_INFO("Inserting LPA %u PPA %u pos %u. Had %u leaves. Will be %u cached %u max.\n", 
        //        lpa, ppa, pos, root->cnt, cmt->cached_cnt + 1, max);
    } else {
        NVMEV_INFO("Inserting LPA %u PPA %u in reload. Had %u leaves. Will be %u cached %u max.\n", 
                lpa, ppa, root->cnt, cmt->cached_cnt + 1, max);
    }

    if(pos != UINT_MAX) {
        NVMEV_INFO("Updating LPA %u PPA %u directly at %u\n",
                     lpa, ppa, pos);
        memcpy(ptr + pos, &ppa, sizeof(ppa));
        return;
    }

    cmt->cached_cnt++;
	i = __lower_bound(root, lpa);

    leaf = (struct leaf*) (ptr + sizeof(struct root) + (sizeof(struct leaf) * i));
    if(leaf->lpa[IN_LEAF - 1] != UINT_MAX) {
        NVMEV_ASSERT(!reloading);
        NVMEV_INFO("LEAF FULL!!! REDISTRIBUTE!!!\n");
        btree_reload(cmt, root, lpa, ppa);
        return;
    }

    if(i == root->cnt - 1) {
        NVMEV_INFO("Not going to update highest key of last leaf.\n");
    } else if(lpa >= root->entries[i] || root->entries[i] == UINT_MAX) {
        NVMEV_INFO("Assigning new highest LPA %u when old LPA was %u\n", 
                     lpa, root->entries[i]);
        root->entries[i] = lpa;
    }

    for(int j = 0; j < IN_LEAF; j++) {
        if(leaf->lpa[j] == UINT_MAX) {
            leaf->lpa[j] = lpa;
            leaf->ppa[j] = ppa;
            break;
        }
    }

    //NVMEV_INFO("Added LPA %u PPA %u to slot %u in leaf %d\n", 
    //             lpa, ppa, leaf->cnt - 1, i);
}

static uint64_t sample_cnt = 0;
uint32_t btree_find(struct root *root, uint32_t lpa, uint32_t *pos) {
    int i, j;
    struct leaf *leaf;
    uint32_t ret;
    char* ptr;
    uint64_t start, end;

    i = 0;
    ret = UINT_MAX;
    ptr = (char*) root;

    NVMEV_INFO("Trying to find LPA %u\n", lpa);

	i = __lower_bound(root, lpa);

    if(i == root->cnt) {
        goto out;
    }

    leaf = (struct leaf*) (ptr + sizeof(struct root) + (sizeof(struct leaf) * i));
    NVMEV_INFO("Got leaf %d\n", i);

    for(j = 0; j < IN_LEAF; j++) {
        //jumps++;
        //NVMEV_INFO("Checking LPA %u leaf %d cnt %u\n", 
        //             leaf->lpa[j], i, leaf->cnt);
        if(leaf->lpa[j] == lpa) {
            NVMEV_INFO("Returning PPA %u for LPA %u\n", leaf->ppa[j], lpa);

            //if(sample) {
            //    end = __cycles();
            //    printk("Search took %llu cycles %llu jumps.\n", 
            //            end - start, jumps);
            //}

            if(pos) {
                *pos = sizeof(struct root) + (sizeof(struct leaf) * i) + 
                       (sizeof(uint32_t) * IN_LEAF) + 
                       (j * sizeof(uint32_t));
            }

            return leaf->ppa[j];
        } else if(leaf->lpa[j] == UINT_MAX) {
            break;
        }
    }

out:
    NVMEV_INFO("LPA %u not found!!\n", lpa);
    return ret;
}

//#define NUM 1000
//uint32_t keys[NUM];
//
//void shuffle(uint32_t *array, size_t n) {
//    if (n > 1) {
//        size_t i;
//        for (i = 0; i < n - 1; i++) {
//            size_t j = i + rand() / (RAND_MAX / (n - i) + 1);
//            uint32_t temp = array[j];
//            array[j] = array[i];
//            array[i] = temp;
//        }
//    }
//}
//
//int main() {
//    void* buf;
//    struct root *root;
//    struct cmt_struct cmt;
//    int entry;
//    uint32_t max;
//    uint32_t pos;
//
//    srand(time(NULL));
//
//
//    buf = malloc(PAGESIZE);
//    memset(buf, 0x0, PAGESIZE);
//
//    root = (struct root*) buf;
//    btree_init(root);
//
//    cmt.cached_cnt = 0;
//    cmt.len_on_disk = 5;
//
//    for(int i = 0; i < NUM; i++) {
//again:
//        int entry = rand() % EPP;
//        for(int j = 0; j < NUM; j++) {
//            if(keys[j] == entry) {
//                goto again;
//            }
//        }
//        keys[i] = rand() % EPP;
//    }
//
//    for(int i = 0; i < NUM; i++) {
//        max = root->cnt * IN_LEAF;
//
//        if(cmt.cached_cnt == max) {
//            btree_expand(root);
//            btree_reload(&cmt, root, UINT_MAX, UINT_MAX);
//            cmt.len_on_disk++;
//        }
//
//        entry = keys[i];
//        if(btree_find(root, keys[i], &pos) == UINT_MAX) {
//            btree_insert(&cmt, root, entry, rand(), UINT_MAX);
//
//            if(btree_find(root, keys[i], NULL) == UINT_MAX) {
//                NVMEV_INFO("Find failed for key %d!\n", keys[i]);
//                NVMEV_ASSERT(0);
//            }
//        } else {
//            btree_insert(&cmt, root, entry, rand(), pos);
//        }
//
//        pos = UINT_MAX;
//        NVMEV_INFO("%d inserts done.\n", i + 1);
//    }
//
//    for(int i = 0; i < NUM; i++) {
//        if(btree_find(root, keys[i], NULL) == UINT_MAX) {
//            NVMEV_INFO("Find failed for key %d!\n", keys[i]);
//        }
//    }
//}
#endif
