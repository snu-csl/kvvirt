#include "FS.h"
#include "container.h"
#ifdef bdbm_drv
extern lower_info memio_info;
#endif
int F_kzalloc(void **ptr, int size,int rw){
	int dmatag=0;
	if(rw!=FS_SET_T && rw!=FS_GET_T){
		printk("type error! in F_MALLOC\n");
        printk("Should have aborted here!\n");
	}
#ifdef bdbm_drv
	dmatag=memio_info.lower_alloc(rw,(char**)ptr);
#elif linux_aio
	if(size%(4*K)){
		(*ptr)=kzalloc(size);
	}else{
		int res;
		void *target;
		res=posix_memalign(&target,4*K,size);
		memset(target,0,size);

		if(res){
			printk("failed to allocate memory:%d\n",errno);
		}
		*ptr=target;
	}
#else
	(*ptr)=kzalloc(size, GFP_KERNEL);
#endif	
	if(rw==FS_MALLOC_R){
	//	printk("alloc tag:%d\n",dmatag);
	}
	return dmatag;
}
void F_free(void *ptr,int tag,int rw){
#ifdef bdbm_drv
	memio_info.lower_kfree(rw,tag);
#else 
	kfree(ptr);
#endif
	return;
}
