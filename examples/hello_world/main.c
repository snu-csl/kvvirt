#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <unistd.h>

#include "linux_nvme_ioctl.h"
#include "kv_nvme.h"

int main(int argc, char** argv)
{
    char* dev;
    char *buf;
    char* key;
    char* val;
    int fd, nsid, space_id, offset, key_len, ret;
    struct nvme_passthru_kv_cmd cmd;
    enum nvme_kv_store_option store_option;
    enum nvme_kv_retrieve_option retrieve_option;

    dev = NULL;
    buf = NULL;
    key = (char*) "HelloKey";
    val = (char*) "Hello KVSSD!";
    fd = -1;
    nsid = 0;
    space_id = 0;
    offset = 0;
    key_len = strlen(key);
    ret = 0;
    store_option = STORE_OPTION_NOTHING;
    retrieve_option = RETRIEVE_OPTION_NOTHING;

    if(argc != 2) {
        printf("Usage: ./hello_world /dev/nvmeXnX.\n");
        exit(1);
    }

    dev = argv[1];

    fd = open(dev, O_RDWR);
    if (fd < 0) {
        printf("Failed to open device %s.\n", dev);
        exit(1);
    }

    printf("Opened %s!\n", dev);

    memset(&cmd, 0x0, sizeof(cmd));
    buf = malloc(4096);

	cmd.opcode = nvme_cmd_kv_store;
	cmd.nsid = nsid;
    /*
     * KVSSDs can support multiple key spaces. We don't use them right now
     * in kvvirt.
     */
    cmd.cdw3 = space_id;
    cmd.cdw4 = store_option;
	cmd.cdw5 = offset;
	cmd.data_addr = (__u64)buf;
	cmd.data_length = strlen(val);
	cmd.key_length = key_len;
	if (key_len > KVCMD_INLINE_KEY_MAX) {
		cmd.key_addr = (__u64)key;
	} else {
		memcpy(cmd.key, key, key_len);
	}

	ret = ioctl(fd, NVME_IOCTL_IO_KV_CMD, &cmd);
    if(ret) {
        printf("Store failed with error %d.\n", ret);
        close(fd);
        exit(1);
    }

    printf("Wrote %s to the KVSSD with key %s!\n", val, key);

    memset(buf, 0x0, 4096);
    cmd.opcode = nvme_cmd_kv_retrieve;
    cmd.cdw4 = retrieve_option;

	ret = ioctl(fd, NVME_IOCTL_IO_KV_CMD, &cmd);
    if(ret) {
        printf("Retrieve failed with error %d.\n", ret);
        close(fd);
        exit(1);
    }

    printf("Got %s vlen %u from the KVSSD with key %s!\n", val, cmd.result, key);
    close(fd);
}
