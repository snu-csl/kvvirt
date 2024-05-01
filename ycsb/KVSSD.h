#pragma once

#include <atomic>
#include <cstring>
#include <fcntl.h>
#include <mutex>
#include <string>
#include <sys/eventfd.h>
#include <sys/ioctl.h>
#include <thread>
#include <unistd.h>
#include <vector>

#include "blockingconcurrentqueue.h"
#include "concurrentqueue.h"
#include "kv_nvme.h"
#include "linux_nvme_ioctl.h"

class KVSSD {
    public:
        KVSSD(std::string path) {
            path_ = path;
        }

        int Open();
        int Close();
        int Store(std::string key, const char* in, int vlen);
        int Store(uint64_t key, const char* in, int vlen);
        int StoreAsync(std::string key, const char *in, int vlen,
                       void (*cb)(void*, char*, int), void *args);
        int StoreAsync(uint64_t key, const char *in, int vlen,
                       void (*cb)(void*, char*, int), void *args);
        int Retrieve(std::string key, char *out, uint64_t *vlen_out);
        int Retrieve(uint64_t key, char *out, uint64_t *vlen_out);
        struct req {
            void (*cb)(void*, char*, int);
            char *out;
            void *args;
        };

        int RetrieveAsync(uint64_t key, char *out, uint64_t *vlen_out,
                          void (*cb)(void*, char*, int), void *args);
        int Delete(uint64_t key);
    int Delete(std::string key);
        int DeleteAsync(uint64_t key, void (*cb)(void*, char*, int), void *args);
        uint64_t GetBytesUsed();
        void ToggleDeleteCheck(bool check_);
        void SetBufferLen(uint64_t len);
    private:
        static const uint32_t num_pollers = 3;
        static int fd_;
        static int fds_[num_pollers];
        int nsid_;
        int space_id_ = 1;
        std::string path_;
        char readBuffer_[4096];
        bool check_deletes = false;
        uint64_t _buffer_len = 4096;

        static uint32_t kvssds_open;
        static std::mutex lock;
        static std::atomic<uint8_t> poller_stop;
        static uint8_t pollers_started;
        static std::vector<std::thread> pollers;
        std::atomic<uint32_t> open_count;

        struct nvme_aioevents aioevents;
        static std::vector<struct nvme_aioctx> aioctxs;
        static std::vector<int> efds;

        static struct req *reqs[num_pollers];
        static moodycamel::ConcurrentQueue<uint64_t> req_ids[num_pollers];

        void Poller(int id, struct nvme_aioctx aioctx, int efd_) {
            struct nvme_aioevents aioevents;
            int nr_changedfds;
            fd_set rfds;
            struct timeval timeout{0, 0};
            unsigned long long eftd_ctx = 0;
            unsigned int reqid = 0;
            unsigned int check_nr = 0;
            int read_s = 0;

            char name[16];
            sprintf(name, "poller%d", id);
            pthread_setname_np(pthread_self(), name);

            while(1) {
                nr_changedfds = select(efd_ + 1, &rfds, NULL, NULL, &timeout);
                if (nr_changedfds == 1 || nr_changedfds == 0) {
                    read_s = read(efd_, &eftd_ctx, sizeof(unsigned long long));
                    while (eftd_ctx) {
                        check_nr = eftd_ctx;
                        if (check_nr > 64) {
                            assert(0);
                            check_nr = 64;
                        }
                        aioevents.nr = check_nr;
                        aioevents.ctxid = aioctx.ctxid;
                        if (ioctl(fd_, NVME_IOCTL_GET_AIOEVENT, &aioevents) < 0) {
                            fprintf(stderr, "fail to read IOEVETS \n");
                            assert(0);
                        }
                        eftd_ctx -= check_nr;
                        for (int i = 0; i < aioevents.nr; i++) {
                            int reqid = aioevents.events[i].reqid;
                            struct req req = reqs[id][reqid];

                            if(req.cb) {
                                req.cb(req.args, (char*) req.out, aioevents.events[i].result);
                            }
                            memset(&reqs[id][reqid], 0x0, sizeof(req));
                            req_ids[id].enqueue(reqid);
                        }
                    }
                } if(poller_stop && req_ids[id].size_approx() == 64) {
                    while(req_ids[id].try_dequeue(reqid)) {}
                    break;
                }
            }
        }
};
