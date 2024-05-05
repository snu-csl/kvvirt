#include "KVSSD.h"

int KVSSD::fd_ = -100;
std::mutex KVSSD::lock;
uint32_t KVSSD::kvssds_open;
std::atomic<uint8_t> KVSSD::poller_stop;
uint8_t KVSSD::pollers_started;
std::vector<std::thread> KVSSD::pollers;
std::vector<int> KVSSD::efds;
std::vector<struct nvme_aioctx> KVSSD::aioctxs;
struct KVSSD::req* KVSSD::reqs[num_pollers];
moodycamel::ConcurrentQueue<uint64_t> KVSSD::req_ids[num_pollers];
int KVSSD::fds_[num_pollers];

int KVSSD::Open() {
    lock.lock();
    if(!pollers_started) {
        for(int i = 0; i < num_pollers; i++) {
            reqs[i] = (struct req*)malloc(sizeof(struct req) * 64);
        }

        fd_ = open(path_.c_str(), O_RDWR);
        if(fd_ < 0) {
            printf("ERROR: failed to open KVSSD %s\n", path_.c_str());
            return 1;
        }

        nsid_ = ioctl(fd_, NVME_IOCTL_ID);
        if(nsid_ == (unsigned) -1) {
            printf("ERROR: failed to get nsid for %s\n", path_.c_str());
            return 1;
        }

        for(int i = 0; i < num_pollers; i++) {
            fds_[i] = open(path_.c_str(), O_RDWR);
            if(fds_[i] < 0) {
                printf("ERROR: failed to open KVSSD %s\n", path_.c_str());
                return 1;
            }

            nsid_ = ioctl(fds_[i], NVME_IOCTL_ID);
            if(nsid_ == (unsigned) -1) {
                printf("ERROR: failed to get nsid for %s\n", path_.c_str());
                return 1;
            }

            struct nvme_aioctx aioctx;

            int efd_ = eventfd(0, EFD_NONBLOCK);
            if (efd_ < 0) {
                printf("ERROR: failed to open eventfd %s\n", path_.c_str());
                return 1;
            }

            memset(&aioevents, 0, sizeof(aioevents));
            memset(&aioctx, 0, sizeof(aioctx));

            for(int j = 0; j < 64; j++) {
                req_ids[i].enqueue(j);
            }

            aioctx.eventfd = efd_;
            if (ioctl(fds_[i], NVME_IOCTL_SET_AIOCTX, &aioctx) < 0) {
                printf("ERROR: failed to set aioctx for %s\n", path_.c_str());
                assert(0);
                return 1;
            }

            efds.push_back(efd_);
            aioctxs.push_back(aioctx);
            pollers.push_back(std::thread(&KVSSD::Poller, this, i,
                        aioctxs.back(), efds.back()));
        }
        pollers_started = 1;
    }
    kvssds_open++;
    lock.unlock();

    return 0;
}

int KVSSD::Close() {
    if(fd_ < 0) {
        printf("WARNING: tried to close non-existent KVSSD.\n");
        return 1;
    }

    lock.lock();
    if(--kvssds_open == 0) {
        poller_stop = 1;

        for(int i = 0; i < pollers.size(); i++) {
            auto &p = pollers.at(i);
            p.join();

            auto efd = efds.at(i);
            close(efd);

            auto ctx = aioctxs.at(i);
            if (ioctl(fd_, NVME_IOCTL_DEL_AIOCTX, &ctx) < 0) {
                printf("ERROR: failed to close aioctx for %s %s\n", path_.c_str(),
                        strerror(errno));
                //return 1;
            }

            free(reqs[i]);
        }

        efds.clear();
        aioctxs.clear();
        pollers.clear();
        close(fd_);
        fd_ = -1;
        pollers_started = 0;
        poller_stop = 0;
    }
    lock.unlock();

    return 0;
}

int KVSSD::Store(std::string key, const char* in, int vlen) {
    if(fd_ < 0) {
        printf("ERROR: tried to called Store without an open KVSSD.\n");
	assert(0);
        return 1;
    }

    if(vlen == 0) {
        printf("WARNING: Store called with 0 length value.\n");
	assert(0);
    }

    int ret = 0;
    struct nvme_passthru_kv_cmd cmd;

    memset(&cmd, 0, sizeof (struct nvme_passthru_kv_cmd));
    cmd.opcode = nvme_cmd_kv_store;
    cmd.nsid = nsid_;
    cmd.cdw3 = space_id_;
    cmd.cdw4 = STORE_OPTION_NOTHING;
    cmd.cdw5 = 0;
    cmd.data_addr = (__u64)in;
    cmd.data_length = vlen;
    cmd.cdw10 = vlen >> 2;
    cmd.key_length = key.length();
    //if (cmd.key_length > KVCMD_INLINE_KEY_MAX) {
    //cmd.key_addr = (__u64)&key;
    //} else {
    memcpy(cmd.key, key.c_str(), key.length());
    //}
    ret = ioctl(fd_, NVME_IOCTL_IO_KV_CMD, &cmd);

    if(ret) {
        printf("ERROR: failed to store key %s value %s err %d\n",
                key.c_str(), in, ret);
        return 1;
    }

    return 0;
}

int KVSSD::Store(uint64_t key, const char* in, int vlen, bool append) {
    if(fd_ < 0) {
        printf("ERROR: tried to called Store without an open KVSSD.\n");
        return 1;
    }

    assert(vlen > 0);

    int ret = 0;
    struct nvme_passthru_kv_cmd cmd;

    memset(&cmd, 0, sizeof (struct nvme_passthru_kv_cmd));
    cmd.opcode = append ? nvme_cmd_kv_append : nvme_cmd_kv_store;
    cmd.nsid = nsid_;
    cmd.cdw3 = space_id_;
    cmd.cdw4 = STORE_OPTION_NOTHING;
    cmd.cdw5 = 0;
    cmd.data_addr = (__u64)in;
    cmd.data_length = vlen;
    cmd.cdw10 = vlen >> 2;
    cmd.key_length = sizeof(key);
    //if (cmd.key_length > KVCMD_INLINE_KEY_MAX) {
    //cmd.key_addr = (__u64)&key;
    //} else {
    memcpy(cmd.key, &key, cmd.key_length);
    //}
    ret = ioctl(fd_, NVME_IOCTL_IO_KV_CMD, &cmd);

    if(vlen == 0) {
        printf("Vlen 0 in store for %d\n", vlen);
    }

    if(ret) {
        printf("ERROR: failed to store key %lu value %s err %d\n",
                key, in, ret);
        return 1;
    }

    return 0;
}

int KVSSD::StoreAsync(std::string key, const char *in, int vlen,
        void (*cb)(void*, char*, int), void *args) {
    if(fd_ < 0) {
        printf("ERROR: tried to called Store without an open KVSSD.\n");
        return 1;
    }

    assert(in);
    assert(vlen > 0);

    int ret = 0;
    struct nvme_passthru_kv_cmd cmd;

    uint64_t reqid = UINT64_MAX;
    uint64_t poller = (*(uint64_t*) key.c_str() >> 32) % num_pollers;

    while(!req_ids[poller].try_dequeue(reqid)) {}
    assert(key.length() <= 16);

    memset(&cmd, 0, sizeof (struct nvme_passthru_kv_cmd));
    cmd.opcode = nvme_cmd_kv_store;
    cmd.nsid = nsid_;
    cmd.cdw3 = space_id_;
    cmd.cdw4 = STORE_OPTION_NOTHING;
    cmd.cdw5 = 0;
    cmd.data_addr = (__u64)in;
    cmd.data_length = vlen;
    cmd.cdw10 = vlen >> 2;
    cmd.key_length = key.length();
    cmd.reqid = reqid;
    cmd.ctxid = aioctxs.at(poller).ctxid;
    memcpy(cmd.key, key.c_str(), key.length());
    reqs[poller][reqid].cb = cb;
    reqs[poller][reqid].out = (char*) in;
    reqs[poller][reqid].args = args;
    ret = ioctl(fds_[poller], NVME_IOCTL_AIO_CMD, &cmd);

    //printf("Store for %lu\n", key);

    if(vlen == 0) {
        printf("Vlen 0 in store for %d\n", vlen);
    }

    if(ret) {
        printf("ERROR: failed to store key %s value %s err %d\n",
                key.c_str(), in, ret);
        return 1;
    }

    return 0;
}

int KVSSD::StoreAsync(uint64_t key, const char *in, int vlen,
        void (*cb)(void*, char*, int), void *args) {
    if(fd_ < 0) {
        printf("ERROR: tried to called Store without an open KVSSD.\n");
        return 1;
    }

    assert(in);
    assert(vlen > 0);

    int ret = 0;
    struct nvme_passthru_kv_cmd cmd;

    uint64_t reqid = UINT64_MAX;
    uint64_t poller = (key >> 32) % num_pollers;

    while(!req_ids[poller].try_dequeue(reqid)) {}

    memset(&cmd, 0, sizeof (struct nvme_passthru_kv_cmd));
    cmd.opcode = nvme_cmd_kv_store;
    cmd.nsid = nsid_;
    cmd.cdw3 = space_id_;
    cmd.cdw4 = STORE_OPTION_NOTHING;
    cmd.cdw5 = 0;
    cmd.data_addr = (__u64)in;
    cmd.data_length = vlen;
    cmd.cdw10 = vlen >> 2;
    cmd.key_length = sizeof(key);
    cmd.reqid = reqid;
    cmd.ctxid = aioctxs.at(poller).ctxid;
    memcpy(cmd.key, &key, cmd.key_length);
    reqs[poller][reqid].cb = cb;
    reqs[poller][reqid].out = (char*) in;
    reqs[poller][reqid].args = args;
    ret = ioctl(fds_[poller], NVME_IOCTL_AIO_CMD, &cmd);

    //printf("Store for %lu\n", key);

    if(vlen == 0) {
        printf("Vlen 0 in store for %d\n", vlen);
    }

    if(ret) {
        printf("ERROR: failed to store key %lu value %s err %d\n",
                key, in, ret);
        return 1;
    }

    return 0;
}

int KVSSD::Retrieve(std::string key, char *out, uint64_t *vlen_out) {
    if(fd_ < 0) {
        printf("ERROR: tried to called Retrieve without an open KVSSD.\n");
        return 1;
    }

    if(!out || !vlen_out) {
        printf("WARNING: retrieve called with one or more NULL parameters.\n");
    }

    int ret = 0;
    struct nvme_passthru_kv_cmd cmd;

    memset(&cmd, 0, sizeof (struct nvme_passthru_kv_cmd));
    cmd.opcode = nvme_cmd_kv_retrieve;
    cmd.nsid = nsid_;
    cmd.cdw3 = space_id_;
    cmd.cdw4 = RETRIEVE_OPTION_NOTHING;
    cmd.cdw5 = 0;
    cmd.data_addr = (__u64)out;
    cmd.data_length = _buffer_len;
    cmd.key_length = key.length();
    //if (cmd.key_length > KVCMD_INLINE_KEY_MAX) {
    //cmd.key_addr = (__u64)&key;
    //} else {
    memcpy(cmd.key, key.c_str(), key.length());
    //}
    ret = ioctl(fd_, NVME_IOCTL_IO_KV_CMD, &cmd);

    if(ret || cmd.result > 1024) {
        fflush(stdout);

        if(vlen_out) {
            *vlen_out = 0;
        }
        return 1;
    }

    if(vlen_out) {
        *vlen_out = cmd.result;
    }

    return 0;
}


int KVSSD::Retrieve(uint64_t key, char *out, uint64_t *vlen_out) {
    if(fd_ < 0) {
        printf("ERROR: tried to called Retrieve without an open KVSSD.\n");
        return 1;
    }

    if(!out || !vlen_out) {
        printf("ERROR: retrieve called with one or more NULL parameters.\n");
        return 1;
    }

    int ret = 0;
    struct nvme_passthru_kv_cmd cmd;

    memset(&cmd, 0, sizeof (struct nvme_passthru_kv_cmd));
    cmd.opcode = nvme_cmd_kv_retrieve;
    cmd.nsid = nsid_;
    cmd.cdw3 = space_id_;
    cmd.cdw4 = RETRIEVE_OPTION_NOTHING;
    cmd.cdw5 = 0;
    cmd.data_addr = (__u64)out;
    cmd.data_length = _buffer_len;
    cmd.key_length = sizeof(key);
    //if (cmd.key_length > KVCMD_INLINE_KEY_MAX) {
    //cmd.key_addr = (__u64)&key;
    //} else {
    memcpy(cmd.key, &key, cmd.key_length);
    //}
    ret = ioctl(fd_, NVME_IOCTL_IO_KV_CMD, &cmd);

    //printf("Retrieve for %lu\n", key);

    if(ret) {
        //printf("ERROR: failed to retrieve key %lu %d\n", key, ret);
        fflush(stdout);
        *vlen_out = 0;
        return 1;
    }

    *vlen_out = cmd.result;
    return 0;
}

struct req {
    void (*cb)(void*, char*, int);
    char *out;
    void *args;
};

int KVSSD::RetrieveAsync(uint64_t key, char *out, uint64_t *vlen_out,
        void (*cb)(void*, char*, int), void *args) {
    //Retrieve(key, out, (uint64_t*) vlen_out);
    //cb(args, out, *vlen_out);
    //return 0;
    if(fd_ < 0) {
        printf("ERROR: tried to called Retrieve without an open KVSSD.\n");
        return 1;
    }

    if(!out || !vlen_out) {
        printf("ERROR: retrieve called with one or more NULL parameters.\n");
        return 1;
    }

    int ret = 0;
    struct nvme_passthru_kv_cmd cmd;
    uint64_t reqid = UINT64_MAX;
    uint64_t poller = (key >> 32) % num_pollers;

    while(!req_ids[poller].try_dequeue(reqid)) {}

    memset(&cmd, 0, sizeof (struct nvme_passthru_kv_cmd));
    cmd.opcode = nvme_cmd_kv_retrieve;
    cmd.nsid = nsid_;
    cmd.cdw3 = space_id_;
    cmd.cdw4 = RETRIEVE_OPTION_NOTHING;
    cmd.cdw5 = 0;
    cmd.data_addr = (__u64)out;
    cmd.data_length = _buffer_len;
    cmd.key_length = sizeof(key);
    cmd.reqid = reqid;
    cmd.ctxid = aioctxs.at(poller).ctxid;
    memcpy(cmd.key, &key, cmd.key_length);
    reqs[poller][reqid].cb = cb;
    reqs[poller][reqid].out = out;
    reqs[poller][reqid].args = args;
    ret = ioctl(fds_[poller], NVME_IOCTL_AIO_CMD, &cmd);

    //printf("Async retrieve for %lu went to %lu\n", key, key % num_pollers);

    if(ret) {
        printf("ERROR: failed to retrieve key %lu %d\n", key, ret);
        fflush(stdout);
        *vlen_out = 0;
        return 1;
    }

    *vlen_out = cmd.result;
    return 0;
}

int KVSSD::Delete(std::string key) {
    if(fd_ < 0) {
        printf("ERROR: tried to called Delete without an open KVSSD.\n");
        return 1;
    }

    int ret = 0;
    struct nvme_passthru_kv_cmd cmd;

    memset(&cmd, 0, sizeof (struct nvme_passthru_kv_cmd));
    cmd.opcode = nvme_cmd_kv_delete;
    cmd.nsid = nsid_;
    cmd.cdw3 = space_id_;
    cmd.cdw4 = check_deletes ? DELETE_OPTION_CHECK_KEY_EXIST :
        DELETE_OPTION_NOTHING;
    cmd.key_length = sizeof(key);
    //if (cmd.key_length > KVCMD_INLINE_KEY_MAX) {
    //cmd.key_addr = (__u64)&key;
    //} else {
    memcpy(cmd.key, key.c_str(), key.length());
    //}
    ret = ioctl(fd_, NVME_IOCTL_IO_KV_CMD, &cmd);

    //printf("Delete for %lu\n", key);

    if(ret && check_deletes) {
        printf("ERROR: failed to delete key %s %d. "
                "Check deletes was %s.\n",
                key.c_str(), ret, check_deletes ? "ON" : "OFF");
        return 1;
    }

    return 0;
}

int KVSSD::Delete(uint64_t key) {
    if(fd_ < 0) {
        printf("ERROR: tried to called Delete without an open KVSSD.\n");
        return 1;
    }

    int ret = 0;
    struct nvme_passthru_kv_cmd cmd;

    memset(&cmd, 0, sizeof (struct nvme_passthru_kv_cmd));
    cmd.opcode = nvme_cmd_kv_delete;
    cmd.nsid = nsid_;
    cmd.cdw3 = space_id_;
    cmd.cdw4 = check_deletes ? DELETE_OPTION_CHECK_KEY_EXIST :
        DELETE_OPTION_NOTHING;
    cmd.key_length = sizeof(key);
    //if (cmd.key_length > KVCMD_INLINE_KEY_MAX) {
    //cmd.key_addr = (__u64)&key;
    //} else {
    memcpy(cmd.key, &key, cmd.key_length);
    //}
    ret = ioctl(fd_, NVME_IOCTL_IO_KV_CMD, &cmd);

    //printf("Delete for %lu\n", key);

    if(ret && check_deletes) {
        printf("ERROR: failed to delete key %lu %d. "
                "Check deletes was %s.\n",
                key, ret, check_deletes ? "ON" : "OFF");
        return 1;
    }

    return 0;
}

int KVSSD::DeleteAsync(uint64_t key, void (*cb)(void*, char*, int), void *args) {
    if(fd_ < 0) {
        printf("ERROR: tried to called Delete without an open KVSSD.\n");
        return 1;
    }

    int ret = 0;
    struct nvme_passthru_kv_cmd cmd;

    uint64_t reqid = UINT64_MAX;
    uint64_t poller = (key >> 32) % num_pollers;

    while(!req_ids[poller].try_dequeue(reqid)) {}

    memset(&cmd, 0, sizeof (struct nvme_passthru_kv_cmd));
    cmd.opcode = nvme_cmd_kv_delete;
    cmd.nsid = nsid_;
    cmd.cdw3 = space_id_;
    cmd.cdw4 = check_deletes ? DELETE_OPTION_CHECK_KEY_EXIST :
        DELETE_OPTION_NOTHING;
    cmd.key_length = sizeof(key);
    cmd.reqid = reqid;
    cmd.ctxid = aioctxs.at(poller).ctxid;
    memcpy(cmd.key, &key, cmd.key_length);
    reqs[poller][reqid].cb = cb;
    reqs[poller][reqid].out = NULL;
    reqs[poller][reqid].args = args;

    ret = ioctl(fds_[poller], NVME_IOCTL_AIO_CMD, &cmd);

    if(ret && check_deletes) {
        printf("ERROR: failed to delete key %lu %d. "
                "Check deletes was %s.\n",
                key, ret, check_deletes ? "ON" : "OFF");
        return 1;
    }

    return 0;
}

uint64_t KVSSD::GetBytesUsed() {
    char *data = (char *)calloc(1, 4096);

    struct nvme_passthru_cmd cmd;
    memset(&cmd, 0, sizeof(struct nvme_passthru_cmd));
    cmd.opcode = nvme_cmd_admin_identify;
    cmd.nsid = nsid_;
    cmd.addr = (__u64)data;
    cmd.data_len = 4096;
    cmd.cdw10 = 0;

    if (ioctl(fd_, NVME_IOCTL_ADMIN_CMD, &cmd) < 0)
    {
        if (data){
            free(data);
            data = NULL;
        }
        return UINT64_MAX;
    }

    const __u64 namespace_utilization = *((__u64 *)&data[16]);
    free(data);
    return namespace_utilization * 512;
}

void KVSSD::ToggleDeleteCheck(bool check_) {
    check_deletes = check_;
}

void KVSSD::SetBufferLen(uint64_t len) {
    _buffer_len = len;
}
