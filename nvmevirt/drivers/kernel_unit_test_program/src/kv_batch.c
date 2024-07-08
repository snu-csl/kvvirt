#include <string.h>
#include <sys/ioctl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <errno.h>
#include <unistd.h>
#include <limits.h>
#include <stdint.h>

#include "linux_nvme_ioctl.h"
#include "kv_nvme.h"

void usage(void)
{
    printf("[Usage] kv_store -d device_path -k key_string [-v value_pattern -l offset -s value_size -z space_id -i(idempotent flag)]\n");
}

#define DEFAULT_BUF_SIZE        (4096)

int main(int argc, char *argv[])
{
    int ret = 0;
    int fd = -1;
    int opt = 0;
    char *dev = NULL;
    char *key = NULL;
    int key_len = 0;
    enum nvme_kv_store_option option = STORE_OPTION_NOTHING;
    unsigned int nsid = 0;
    char *buf = NULL;
    int space_id = 0;
    while((opt = getopt(argc, argv, "d:k:v:l:s:z:i")) != -1) {
        switch(opt) {
            case 'd':
                dev = optarg;
            break;
            default:
                usage();
                ret = -EINVAL;
                goto exit;
            break;
        }
    }

    fd = open(dev, O_RDWR);
    if (fd < 0) {
        printf("fail to open device %s\n", dev);
        goto exit;
    }

    nsid = ioctl(fd, NVME_IOCTL_ID);
    if (nsid == (unsigned) -1) {
        printf("fail to get nsid for %s\n", dev);
        goto exit;
    }

    buf = aligned_alloc(4096, 4096);
    memset(buf, 0x0, 4096);

    uint8_t klen;
    uint32_t vlen;
    uint32_t off = 0;
    uint64_t v;

    vlen = sizeof(v);

    for(int i = 0; i < 9; i++) {
        klen = 8;

        memcpy(buf + off, &klen, sizeof(klen));
        off += sizeof(uint8_t);

        memcpy(buf + off, "hello123", 8);
        off += 8;
        buf[off - 1] = '0' + i;

        v = i * 100;
        memcpy(buf + off, &vlen, sizeof(uint32_t));
        off += sizeof(uint32_t);

        memcpy(buf + off, &v, sizeof(v));
        off += sizeof(v);
    }

    key = malloc(sizeof(uint64_t));
    memcpy(key, &v, sizeof(uint64_t));
    key_len = sizeof(uint64_t);

    ret = nvme_kv_batch(space_id, fd, nsid, key, key_len, buf, (int)off, (int)0, option);
    if (ret) {
        printf("fail to store for %s\n", dev);
    }
exit:
    if (buf) free(buf);
    if (fd >= 0) {
        close(fd);
    }
    return ret;
}
