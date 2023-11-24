/*
 * Copyright (c) 1995 Jason W. Cox.  All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 * 3. All advertising materials mentioning features or use of this software
 *    must display the following acknowledgement:
 *      This product includes software developed by Jason W. Cox.
 * 4. The name of the author may not be used to endorse or promote products
 *    derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE AUTHOR ``AS IS'' AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 * IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
 * NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF
 * THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 *      $Id: redblack.h,v 1.1.1.1 2007/03/26 00:29:59 j Exp $
 */

/*
 * redblack.h
 *
 * Double linked bottom-up 2-3-4 tree using the red-black framework.
 *
 * Jason W. Cox
 * <cox@cs.utk.edu>
 *
 * December 21, 1995
 *
 * $Id: redblack.h,v 1.1.1.1 2007/03/26 00:29:59 j Exp $
 *
 * References:
 *
 * L. Guibas and R. Sedgewick.  "A Dichromatic Framework For Balanced
 * Trees.", In Proceedings of the 19th Annual Symposium on Foundations
 * of Computer Science, pages 8-21, 1978.
 *
 * N. Sarnak and R.E. Tarjan.  "Planar Point Location Using Persistent
 * Search Trees.", Communications of the ACM, 29:669-679, 1986.
 *
 */

#ifndef REDBLACK_H
#define REDBLACK_H

#include <linux/slab.h>
#include <linux/types.h>

#include "../settings.h"

/* typedef struct str_key{
	uint8_t len;
	//char key[30];
	char *key;
}str_key;
#define KEYT str_key */

typedef struct redblack {
	void *item;
	union {
		uint32_t ikey;
		char *key;
	}k;
	KEYT key;
	struct redblack *parent;
	struct redblack *left;
	struct redblack *right;

#ifdef RB_LINK
	struct redblack *next;
	struct redblack *prev;
#endif
	unsigned red : 1;
} *Redblack;


void print_stat(void);

Redblack drb_create (void);

int drb_find_int (Redblack rb, uint32_t key, Redblack *node);
//int drb_find_str (Redblack rb, char *key, Redblack *node);
int drb_find_fnt (Redblack rb, char *key, Redblack *node, int (*func)(char *,char *));

Redblack drb_insert_int (Redblack rb, uint32_t key, void *item);
//Redblack drb_insert_str (Redblack rb, char* key, void *item);
Redblack drb_insert_fnt (Redblack rb, char *key, void *item, int (*func)(char *,char*));

void drb_delete      (Redblack node,bool isint);
void drb_delete_item (Redblack node, uint32_t key, int item);

void drb_clear     (Redblack rb, uint32_t keys, int items, bool isint);
void drb_destroy   (Redblack rb, uint32_t keys, int items,bool isint);

int drb_count (Redblack rb);
int drb_height (Redblack rb);
int drb_check(Redblack rb);
#ifdef KVSSD
int drb_find_str (Redblack rb, KEYT key, Redblack *node);
Redblack drb_insert_str (Redblack rb, KEYT key, void *item);
#endif


#ifndef RB_LINK
Redblack drb_first(Redblack);
Redblack drb_last(Redblack);
Redblack drb_next(Redblack);
Redblack drb_prev(Redblack);
#else
#define drb_first(n) n->next
#define drb_last(n) n->prev
#define drb_next(n) n->next
#define drb_prev(n) n->prev
#endif

#define drb_isempty(rb) (rb->right == rb)
#define drb_nil(rb) rb
#define drb_item(rb) (rb->item)

#define drb_traverse(tmp, rb) \
	for((tmp) = drb_first(rb); (tmp) != (rb); (tmp) = drb_next(tmp))

#define drb_rtraverse(tmp, rb) \
	for((tmp) = drb_last(rb); (tmp) != (rb); (tmp) = drb_prev(tmp))


#ifndef TRUE
#define TRUE  1
#define FALSE 0
#endif

#endif
