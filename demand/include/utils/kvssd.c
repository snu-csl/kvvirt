#include "../settings.h"
#ifdef KVSSD

#include <linux/slab.h>
char* kvssd_tostring(KEYT key){
	/*
	char temp1[255]={0,};
	memcpy(temp1,key.key,key.len);*/
	return key.key;
}

void kvssd_cpy_key(KEYT *des, KEYT *key){
	des->key=(char*)kzalloc(sizeof(char)*key->len, GFP_KERNEL);
	des->len=key->len;
	memcpy(des->key,key->key,key->len);
}
void kvssd_free_key(KEYT *des){
	kfree(des->key);
	kfree(des);
}
#endif
