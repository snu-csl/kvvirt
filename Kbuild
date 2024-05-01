# Select one of the targets to build
#CONFIG_NVMEVIRT_NVM := y
#CONFIG_NVMEVIRT_SSD := y
#CONFIG_NVMEVIRT_ZNS := y
#CONFIG_NVMEVIRT_KV := y
CONFIG_NVMEVIRT_HASH_DFTL := y
#CONFIG_PAGE_MAPPED := y

obj-m   := nvmev.o
nvmev-objs := main.o pci.o admin.o io.o dma.o
ccflags-y += -Wno-unused-variable -Wno-unused-function -g

ccflags-$(CONFIG_NVMEVIRT_NVM) += -DBASE_SSD=INTEL_OPTANE
nvmev-$(CONFIG_NVMEVIRT_NVM) += simple_ftl.o

ccflags-$(CONFIG_NVMEVIRT_SSD) += -DBASE_SSD=SAMSUNG_970PRO
nvmev-$(CONFIG_NVMEVIRT_SSD) += ssd.o conv_ftl.o pqueue/pqueue.o channel_model.o

ccflags-$(CONFIG_NVMEVIRT_ZNS) += -DBASE_SSD=WD_ZN540
#ccflags-$(CONFIG_NVMEVIRT_ZNS) += -DBASE_SSD=ZNS_PROTOTYPE
ccflags-$(CONFIG_NVMEVIRT_ZNS) += -Wno-implicit-fallthrough
nvmev-$(CONFIG_NVMEVIRT_ZNS) += ssd.o zns_ftl.o zns_read_write.o zns_mgmt_send.o zns_mgmt_recv.o channel_model.o

ccflags-$(CONFIG_NVMEVIRT_KV) += -DBASE_SSD=KV_PROTOTYPE
nvmev-$(CONFIG_NVMEVIRT_KV) += kv_ftl.o append_only.o bitmap.o

ccflags-$(CONFIG_NVMEVIRT_HASH_DFTL) += -Wno-declaration-after-statement -std=gnu99 -Werror
ccflags-$(CONFIG_NVMEVIRT_HASH_DFTL) += -DBASE_SSD=SAMSUNG_970PRO_HASH_DFTL

ccflags-y += -I/lib/modules/$(shell uname -r)/build/include
nvmev-$(CONFIG_NVMEVIRT_HASH_DFTL) += ssd.o demand_ftl.o pqueue/pqueue.o channel_model.o cache.o city.o fifo.o hashset.o twolevel.o
