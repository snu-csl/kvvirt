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
 *      $Id: redblack.c,v 1.1.1.1 2007/03/26 00:29:59 j Exp $
 */

/*
 * redblack.c
 *
 * Double linked bottom-up 2-3-4 tree using the red-black framework.
 *
 * Jason W. Cox
 * <cox@cs.utk.edu>
 *
 * December 21, 1995
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

#include <linux/string.h>
#include "redblack.h"

static char rcsid[] = "$Id: redblack.c,v 1.1.1.1 2007/03/26 00:29:59 j Exp $";

static void single_rotate(Redblack, Redblack, Redblack, int);
static void double_rotate(Redblack, Redblack, Redblack, Redblack);

#define root(rb) (rb->right)
#define isred(rb) (rb->red)
#define isblack(rb) (!isred(rb))
#define isleft(rb) (rb->parent->left == rb)
#define isright(rb) (rb->parent->right == rb)
#define isroot(rb) (rb->parent->left == rb->parent)
#define setred(rb) rb->red = TRUE
#define setblack(rb) rb->red = FALSE
#define sibling(rb) (isright(rb) ? rb->parent->left : rb->parent->right)
#define issentinel(rb) (rb == rb->left)

#define RED   1
#define BLACK 0

/*static inline int KEYCMP(KEYT a,KEYT b){
	if(!a.len && !b.len) return 0;
	else if(a.len==0) return -1;
	else if(b.len==0) return 1;

	int r=memcmp(a.key,b.key,a.len>b.len?b.len:a.len);
	if(r!=0 || a.len==b.len){
		return r;
	}
	return a.len<b.len?-1:1;
}*/

/*
 * single_rotate
 *
 *                        g                           g
 *                        |                           |
 *                        f                           x
 *                       / \                         / \
 *                      x   3       ===>            1   f
 *                     / \                             / \
 *                    1   2                           2   3
 *
 */
static void single_rotate(Redblack x, Redblack f, Redblack g, int left)
{
	if (g->right == f)
		g->right = x;
	else
		g->left = x;

	if (left) {
		f->left = x->right;
		x->right = f;
	}
	else {
		f->right = x->left;
		x->left = f;
	}
	f->parent = x;
	f->left->parent = f;
	f->right->parent = f;
	x->parent = g;
}


/*
 * double_rotate
 *                        gg                          gg
 *                        |                           |
 *                        g                           x
 *                       / \                         / \
 *                      f   4                       f   g
 *                     / \                         / \ / \
 *                    1   x        ===>           1  2 3  4
 *                       / \
 *                      2   3
 */
static void double_rotate(Redblack x, Redblack f, Redblack g, Redblack gg)
{
	if (gg->left == g)
		gg->left = x;
	else
		gg->right = x;

	if (f->right == x) {
		f->right = x->left;
		x->left = f;
		g->left = x->right;
		x->right = g;
	}
	else {
		f->left = x->right;
		x->right = f;
		g->right = x->left;
		x->left = g;
	}
	x->parent = gg;
	x->left->right->parent = x->left;
	x->left->parent = x;
	x->right->left->parent = x->right;
	x->right->parent = x;
}


/*
 * drb_create
 *
 * Creates a red-black sentinel and returns a pointer to it.
 *
 */
Redblack drb_create (void)
{
	Redblack rb;

	rb = (Redblack) kzalloc(sizeof(struct redblack), GFP_KERNEL);
	if (rb == NULL)
		return NULL;

#ifdef RB_LINK
	rb->next   = \
	rb->prev   = \

#endif
	rb->parent =\
	rb->left   =\
	rb->right  = rb;
#ifdef KVSSD
	rb->key.key= NULL;
#endif
	rb->item  = NULL;

	setblack(rb);

	return rb;
}


/*
 * drb_find_int
 *
 * Search the red-black tree <rb> for the integer <key>.  If the key
 * is found, the reference <node> will be set to the node and a 1 will
 * be returned.  If the key is not found, a 0 will be returned.  If
 * the reference <node> is NULL, then it will not be set.
 *
 */
int drb_find_int(Redblack rb,uint32_t key,Redblack *node)
{
	Redblack x = root(rb);

	BUG_ON(!issentinel(rb));

	while (x != drb_nil(rb) && key != x->k.ikey)
		x = (key < x->k.ikey) ? x->left : x->right;

	if (node) *node = x;

	return (x != drb_nil(rb) && key == x->k.ikey);
}


/*
 * drb_find_str
 *
 * Search the red-black tree <rb> for the string <key>.  If the key is
 * found, the reference <node> will be set to the node and a 1 will be
 * returned.  If the key is not found, a 0 will be returned.  If the
 * reference <node> is NULL, then it will not be set.
 *
 */
#ifdef KVSSD
int drb_find_str(Redblack rb,KEYT key,Redblack *node)
{
	Redblack x = root(rb);
	int c = 1;

	BUG_ON(!issentinel(rb));

	//while (x != drb_nil(rb) && (c = strcmp(key, x->k.key)))
	while (x != drb_nil(rb) && (c = KEYCMP(key, x->key)))
		x = (c < 0) ? x->left : x->right;

	if (node) *node = x;
	if(!KEYTEST(key,x->key)) c=0;
	return (x != drb_nil(rb) && c == 0);
}

#endif
/*
 * drb_find_fnt
 *
 * Search the red-black tree <rb> for the <key> using the function
 * <func>.  If the key is found, the reference <node> will be set to
 * the node and a 1 will be returned.  If the key is not found, a 0
 * will be returned.  If the reference <node> is NULL, then it will
 * not be set.
 *
 */
int drb_find_fnt(Redblack rb,char *key,	Redblack *node,	int (*func)(char*, char*))
{
	Redblack x = root(rb);
	int c = 1;

	BUG_ON(!issentinel(rb));

	while (x != drb_nil(rb) && (c = (*func)(key, x->k.key)))
		x = (c < 0) ? x->left : x->right;

	if (node) *node = x;

	return (x != drb_nil(rb) && c == 0);
}


/*
 * drb_insert_int
 *
 * Insert <item> into the red-black tree <rb> using the integer <key>.
 *
 */
Redblack drb_insert_int(	Redblack rb,uint32_t key,void *item)
{
	Redblack x = root(rb), y = x, z, r;

	BUG_ON(!issentinel(rb));

	while (x != rb) {
		y = x;
		x = (key < x->k.ikey) ? x->left : x->right;
	}

	z = (Redblack) kzalloc(sizeof(struct redblack), GFP_KERNEL);
	if (!(r = z)) return NULL;
	z->k.ikey = key;
	z->item = item;
	z->parent = y;
	z->left  =
		z->right = rb;
	setred(z);

	if (y != rb && key < y->k.ikey) {
#ifdef RB_LINK
		z->prev = y->prev;
		y->prev = z;
		z->next = y;
		z->prev->next =
#endif
			y->left       = z;

	}
	else {
#ifdef RB_LINK
		z->next = y->next;
		y->next = z;
		z->prev = y;
		z->next->prev =
#endif
			y->right      = z;
	}

	if (isroot(z) || isroot(z->parent)) {
		setblack(root(rb));
		return r;
	}

	y = z->parent;
	x = y->parent;

	/* (a) */
	while(isred(x->left) && isred(x->right)) {
		x->left->red  =
			x->right->red = FALSE;
		x->red = TRUE;
		if (isroot(x))
			break;
		z = x;
		y = x->parent;
		x = y->parent;
	}

	if (isred(y) && isred(z)) {
		/* (b) */
		if (x == rb) {
			setblack(y);
			return r;
		}
		/* (c) */
		else if ((isleft(y) && isleft(z)) || (isright(y) && isright(z))) {
			single_rotate(y, x, x->parent, isleft(z));
			setred(x);
			setblack(y);
		}
		/* (d) */
		else {
			double_rotate(z, y, x, x->parent);
			setred(x);
			setblack(z);
		}
	}
	return r;
}


/*
 * drb_insert_str
 *
 * Insert <item> into the red-black tree <rb> using the string <key>.
 *
 */

#ifdef KVSSD
Redblack drb_insert_str(Redblack rb,	KEYT key,void *item)
{
	Redblack x = root(rb), y = x, z, r;

	BUG_ON(!issentinel(rb));

	while (x != rb) {
		y = x;
	//	x = (strcmp(key, x->k.key) < 0) ? x->left : x->right;
		x = (KEYCMP(key, x->key) < 0) ? x->left : x->right;
	}

	z = (Redblack) kzalloc(sizeof(struct redblack), GFP_KERNEL);
	if (!(r = z)) return NULL;
	z->key.len = key.len;
	z->key.key = (char *)kzalloc(z->key.len, GFP_KERNEL);
	memcpy(z->key.key, key.key, z->key.len);
	z->item = item;
	z->parent = y;
	z->left  =
		z->right = rb;
	setred(z);

	//if (y != rb && strcmp(key, y->k.key) < 0) {
	if (y != rb && KEYCMP(key, y->key) < 0) {
#ifdef RB_LINK
		z->prev = y->prev;
		y->prev = z;
		z->next = y;
		z->prev->next =
#endif
			y->left       = z;

	}
	else {
#ifdef RB_LINK
		z->next = y->next;
		y->next = z;
		z->prev = y;
		z->next->prev =
#endif
			y->right      = z;
	}

	if (isroot(z) || isroot(z->parent)) {
		setblack(root(rb));
		return r;
	}

	y = z->parent;
	x = y->parent;

	/* (a) */
	while(isred(x->left) && isred(x->right)) {
		x->left->red  =
			x->right->red = FALSE;
		x->red = TRUE;
		if (isroot(x))
			break;
		z = x;
		y = x->parent;
		x = y->parent;
	}

	if (isred(y) && isred(z)) {
		/* (b) */
		if (x == rb) {
			setblack(y);
			return r;
		}
		/* (c) */
		else if ((isleft(y) && isleft(z)) || (isright(y) && isright(z))) {
			single_rotate(y, x, x->parent, isleft(z));
			setred(x);
			setblack(y);
		}
		/* (d) */
		else {
			double_rotate(z, y, x, x->parent);
			setred(x);
			setblack(z);
		}
	}
	return r;
}
#endif

/*
 * drb_insert_fnt
 *
 * Insert <item> into the red-black tree <rb> using the <key> and comparing
 * keys with the function <func>.
 *
 */
Redblack drb_insert_fnt(	Redblack rb,char *key,void *item,int (*func)(char *,char*))
{
	Redblack x = root(rb), y = x, z, r;

	BUG_ON(!issentinel(rb));

	while (x != rb) {
		y = x;
		x = ((*func)(key, x->k.key) < 0) ? x->left : x->right;
	}

	z = (Redblack) kzalloc(sizeof(struct redblack), GFP_KERNEL);
	if (!(r = z)) return NULL;
	z->k.key = key;
	z->item = item;
	z->parent = y;
	z->left  =
		z->right = rb;
	setred(z);

	if (y != rb && (*func)(key, y->k.key) < 0) {
#ifdef RB_LINK
		z->prev = y->prev;
		y->prev = z;
		z->next = y;
		z->prev->next =
#endif
			y->left       = z;

	}
	else {
#ifdef RB_LINK
		z->next = y->next;
		y->next = z;
		z->prev = y;
		z->next->prev =
#endif
			y->right      = z;
	}

	if (isroot(z) || isroot(z->parent)) {
		setblack(root(rb));
		return r;
	}

	y = z->parent;
	x = y->parent;

	/* (a) */
	while(isred(x->left) && isred(x->right)) {
		x->left->red  =
			x->right->red = FALSE;
		x->red = TRUE;
		if (isroot(x))
			break;
		z = x;
		y = x->parent;
		x = y->parent;
	}

	if (isred(y) && isred(z)) {
		/* (b) */
		if (x == rb) {
			setblack(y);
			return r;
		}
		/* (c) */
		else if ((isleft(y) && isleft(z)) || (isright(y) && isright(z))) {
			single_rotate(y, x, x->parent, isleft(z));
			setred(x);
			setblack(y);
		}
		/* (d) */
		else {
			double_rotate(z, y, x, x->parent);
			setred(x);
			setblack(z);
		}
	}
	return r;
}


/*
 * drb_delete
 *
 * Delete <node> from its red-black tree.
 *
 */
void drb_delete(Redblack node,bool isint)
{
	Redblack x, sib;
	int balance;

	BUG_ON(issentinel(node));

	if (issentinel(node->left)) {
		/*
		 *       /              / \
		 *     node    ===>    x  sib
		 *       \            / \
		 *        x          1   2
		 *       / \
		 *      1   2
		 */
		x = node->right;
		if (isleft(node)) {
			node->parent->left = x;
			sib = node->parent->right;
		}
		else {
			node->parent->right = x;
			sib = node->parent->left;
		}
		x->parent = node->parent;

		balance = (isred(node) || isred(x)) ? FALSE : TRUE;
		setblack(x);
	}
	else {
		x = drb_prev(node);
		if (x == node->left) {
			/*
			 *         /                 /
			 *       node               x  
			 *       /  \     ===>     / \
			 *      x    2            1   2(sib)
			 *     /
			 *    1
			 */
			sib = node->right;
			if (isleft(node))
				node->parent->left = x;
			else
				node->parent->right = x;

			x->parent = node->parent;
			x->right = node->right;
			x->right->parent = x;
			balance = !x->red;
			x->red = node->red;
		}
		else {
			/*
			 *         /                       /
			 *       node                     x
			 *       /  \                    / \
			 *      *    2      ===>        *   2
			 *     / \                     / \
			 *    *   x                   sib 1
			 *   /   /
			 *  *   1                  
			 */
			sib = x->parent->left;
			x->parent->right = x->left;
			x->left->parent = x->parent;
			x->parent = node->parent;
			node->right->parent = node->left->parent = x;
			x->left = node->left;
			x->right = node->right;
			if (isleft(node))
				node->parent->left = x;
			else
				node->parent->right = x;
			balance = !x->red;
			x->red = node->red;
		}
	}

#ifdef RB_LINK
	node->next->prev = node->prev;
	node->prev->next = node->next;
#endif
	
	if(!isint){
#ifdef KVSSD
		kfree(node->key.key);
#endif
		kfree(node);
	}

	if (!balance || issentinel(sib))
		return;

	x = sibling(sib);
	if (isred(x)) {
		setblack(x);
		return;
	}

	/*
	 * Recolor tree
	 */

	/* (a) */
	while (isblack(x) && isblack(sib) && isblack(sib->parent) &&
			isblack(sib->left) && isblack(sib->right)) {
		setred(sib);
		x = sib->parent;
		if (issentinel(x->parent)) return;
		sib = sibling(x);
	}

	/* (b) */
	if (isred(sib) && isblack(sib->parent)) {
		setblack(sib);
		setred(sib->parent);
		if (isleft(sib)) {
			single_rotate(sib, sib->parent, sib->parent->parent, TRUE);
			sib = sib->right->left;
		}
		else {
			single_rotate(sib, sib->parent, sib->parent->parent, FALSE);
			sib = sib->left->right;
		}
	}

	/* (c) */
	if (isred(sib->parent) && isblack(sib) &&
			isblack(sib->left) && isblack(sib->right)) {
		setred(sib);
		setblack(sib->parent);
	}
	else if (isright(sib)) {
		/* (d) */
		if (isred(sib->right)) {
			sib->red = sib->parent->red;
			setblack(sib->parent);
			setblack(sib->right);
			single_rotate(sib, sib->parent, sib->parent->parent, FALSE);
		}
		/* (e) */
		else if (isred(sib->left)) {
			sib->left->red = sib->parent->red;
			setblack(sib->parent);
			double_rotate(sib->left, sib, sib->parent, sib->parent->parent);
		}
	}
	else {
		/* (d) */
		if (isred(sib->left)) {
			sib->red = sib->parent->red;
			setblack(sib->parent);
			setblack(sib->left);
			single_rotate(sib, sib->parent, sib->parent->parent, TRUE);
		}
		/* (e) */
		else if (isred(sib->right)) {
			sib->right->red = sib->parent->red;
			setblack(sib->parent);
			double_rotate(sib->right, sib, sib->parent, sib->parent->parent);
		}
	}
}


/*
 * drb_delete_item
 *
 * Deletes the node from its red-black tree.  It also deletes the key
 * and/or item based on the flags <key> and <item>.
 *
 */
void drb_delete_item(Redblack node, uint32_t key,int item)
{
	if (key)
		kfree(node->k.key);
	if (item)
		kfree(node->item);
	drb_delete(node,true);
}


/*
 * drb_clear
 *
 * Clear all entries in the red-black tree.  If the flags <keys>
 * and/or <items> are true, free them too.
 *
 */
void drb_clear(Redblack rb, uint32_t keys,	int items, bool isint)
{
	Redblack node = drb_first(rb), next;

	BUG_ON(!issentinel(rb));

	while (node != rb) {
		next = drb_next(node);
		if (keys && !isint)
			kfree(node->k.key);
		if (items)
			kfree(node->item);
		/*	printk("freeing node 0x%x\n", node); */
		drb_delete(node,isint);
		node = next;
	}

#ifdef RB_LINK
	rb->next = rb->prev =
#endif
		rb->left = rb->right = rb;
}


/*
 * drb_destroy
 *
 * Free all entries in the redblack tree and the tree itself.  If the
 * flags <keys> and/or <items> are true, free them too.
 *
 */
void drb_destroy(Redblack rb, uint32_t keys,int items, bool isint)
{
	drb_clear(rb, keys, items,isint);
	kfree(rb);
}


/*
 * drb_count
 *
 * Returns the number of nodes in the red-black tree.
 *
 */
int drb_count(Redblack rb)
{
	Redblack x;
	int c = 0;

	BUG_ON(!issentinel(rb));

	for (x = drb_first(rb); x != rb; x = drb_next(x))
		c++;

	return c;
}


/*
 * drb_check
 *
 * Tests the entire red-black tree for violations of the red-black
 * condition.  Returns 1 if the tree is red-black and 0 if not.  If
 * the tree fails, a message explaining which condition it failed is
 * printed to stderr.  It will (should) always return 1.
 *
 */
int drb_check(Redblack rb)
{
	Redblack node, tmp;
	int blacks = 0, count, good = TRUE, initialized = FALSE;

	BUG_ON(!issentinel(rb));

	drb_traverse(tmp, rb) {
		if (tmp->left == rb && tmp->right == rb) {
			if (!initialized) {
				for (node=tmp; node != rb; node = node->parent)
					if (isblack(node))
						blacks++;
				initialized = TRUE;
			}
			count = 0;
			for (node=tmp; node != rb; node = node->parent) {
				if (isred(node) && isred(node->parent)) {
					printk("tree violates the red constraint\n");
					good = FALSE;
					break;
				}

				if (isblack(node))
					count++;
			}
			if (count != blacks) {
				printk("tree violates the black constraint\n");
				good = FALSE;
			}

			if (!good)
				break;
		}
	}
	return good;
}

/*
 * drb_height
 *
 * Returns the height of the red-black tree.
 *
 */
int drb_height(Redblack rb)
{
	Redblack node, tmp;
	int height=0, count;

	BUG_ON(!issentinel(rb));

	drb_traverse(tmp, rb) {
		if (tmp->left == rb && tmp->right == rb) {
			count = 0;
			for (node = tmp; node != rb; node = node->parent)
				count++;
			if (count > height)
				height = count;
		}
	}
	return height;
}


#ifndef RB_LINK
Redblack drb_first(Redblack rb)
{
	Redblack x;

	for (x = root(rb); x->left != rb; x = x->left)
		;

	return x;
}

Redblack drb_last(Redblack rb)
{
	Redblack x;

	for (x = root(rb); x->right != rb; x = x->right)
		;

	return x;
}

Redblack drb_next(Redblack node)
{
	if (issentinel(node)) {
		node = drb_first(node);
	}
	else if (!issentinel(node->right)) {
		node = node->right;
		while (!issentinel(node->left))
			node = node->left;
	}
	else {
		while (!issentinel(node->parent) && isright(node))
			node = node->parent;
		node = node->parent;
	}
	return node;
}

Redblack drb_prev(Redblack node)
{
	if (issentinel(node)) {
		node = drb_last(node);
	}
	else if (!issentinel(node->left)) {
		node = node->left;
		while (!issentinel(node->right))
			node = node->right;
	}
	else {
		while (!issentinel(node->parent) && isleft(node))
			node = node->parent;
		node = node->parent;
	}
	return node;
}
#endif

const char *
drb_rcsid(void)
{
	return rcsid;
}
