#include <algorithm>
#include <assert.h>
#include <atomic>
#include <bitset>
#include <gflags/gflags.h>
#include <hdr/hdr_histogram.h>
#include <map>
#include <mutex>
#include <random>
#include <set>
#include <shared_mutex>
#include <sstream>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <thread>
#include <unistd.h>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "latest-generator.h"
#include "ycsb_common.h"
#include "zipf.h"

FILE *m_log_fp = NULL;
#define lprintf(...) {   \
    printf(__VA_ARGS__); \
    if (m_log_fp) { fprintf(m_log_fp, __VA_ARGS__); fflush(m_log_fp); }} \

DEFINE_int64(warmup_time, 5, "Warmup time in seconds.");
DEFINE_bool(warm_cache, false, "Read the top (CACHE_SIZE / VLEN) from the zipf output first.");
DEFINE_int64(w_pct, 50, "To use with --benchmarks=custom.");
DEFINE_double(zipf_coef, 0.99, "Zipf coefficient");
DEFINE_string(store_name, "NONE.", "What store are we testing?");
DEFINE_string(benchmarks, "a,b,c", "Comma separated list of benchmarks to use.");
DEFINE_bool(uniform, true, "Uniform or zipf.");
DEFINE_bool(pop, true, "Populate or not.");
DEFINE_string(dir, "/tmp/db", "Database directory.");
DEFINE_int64(chunk_len, 4096, "Chunk length for range writes in NewStore.");
DEFINE_int64(vlen, 1024, "Value length.");
DEFINE_int64(threads, 40, "Value length.");
DEFINE_int64(num_ops, 250000, "Value length.");
DEFINE_int64(duration, 1800, "Value length.");
DEFINE_int64(num_pairs, 10000000, "Value length.");
DEFINE_int64(wal_write_size, 16384, "Value length.");
DEFINE_int64(wal_size_mb, 16384, "WAL size in NewStore.");
DEFINE_int64(cache_size_mb, 16384, "Cache size in MB.");
DEFINE_int64(iops_limit, 0, "IOPS limit. 0 means none.");
DEFINE_bool(do_inserts, false, "Inserts instead of updates.");
DEFINE_bool(append, false, "Append instead of store. Appends go to to one global KV pair.");

std::atomic<uint64_t> writes_done;
std::atomic<uint64_t> reads_done;
std::atomic<uint64_t> read_f;

std::atomic<uint64_t> written_during_warmup;
std::atomic<uint64_t> read_during_warmup;
std::atomic<uint64_t> total_written;
std::atomic<uint64_t> total_read;

std::atomic<uint32_t> elapsed;
std::atomic<uint64_t> done;
std::atomic<uint64_t> stats_stop;

std::thread stats_thread;


struct hdr_histogram* read_hist = NULL;

bool pop_only = false;
std::string cur_bench = "";
bool zipf = false;
bool need_population = true;
uint32_t w_pct = 50;

void relaxed_add(std::atomic<uint64_t> &stat, uint64_t add) {
    stat.fetch_add(add, std::memory_order_relaxed);
}

std::string exec(const char* cmd) {
    std::array<char, 128> buffer;
    std::string result;
    std::unique_ptr<FILE, decltype(&pclose)> pipe(popen(cmd, "r"), pclose);
    if (!pipe) {
        return std::string("FAIL");
    }
    while (fgets(buffer.data(), buffer.size(), pipe.get()) != nullptr) {
        result += buffer.data();
    }
    result.erase(std::remove(result.begin(), result.end(), '\n'), result.cend());
    return result;
}

uint64_t NowMicros() {
    struct timeval tv;
    gettimeofday(&tv, nullptr);
    return static_cast<uint64_t>(tv.tv_sec) * 1000000 + tv.tv_usec;
}

std::atomic<bool> warmup;
void reset_stats() {
    done = 0;
    total_written = 0;
    total_read = 0;
    reads_done = 0;
    writes_done = 0;
    elapsed = 1;
    warmup = true;
}

#define BUF_SZ (1 << 20)
std::atomic<uint64_t> slot{BUF_SZ};
std::atomic<uint64_t> cur_append_key{1};

std::atomic<uint64_t> next_to_insert;
std::atomic<uint64_t> num_started{0};
std::atomic<bool> go;
uint32_t buf_size;

int worker(const int id, const bool fill, const bool warm, const std::vector<uint64_t> *keys, 
           const uint64_t start, uint64_t end, const bool do_scan, const bool latest,
           const bool do_rmw, const bool sample, const bool append) {
    char name[16];
    sprintf(name, "worker%d", id);
    pthread_setname_np(pthread_self(), name);

    open(FLAGS_dir.c_str(), buf_size, FLAGS_cache_size_mb, fill);

    if(num_started++ < (FLAGS_threads - 1)) {
        while(!go) {
            usleep(1);
        }
    } else {
        go = true;
    }

    const uint64_t range_from  = 0;
    const uint64_t range_to    = FLAGS_num_pairs - 1;
    std::random_device rand_dev;
    std::mt19937 generator(rand_dev());
    std::uniform_int_distribution<uint64_t>  distr(range_from, range_to);

    const uint64_t op_range_from  = 0;
    const uint64_t op_range_to    = 100;
    std::random_device op_rand_dev;
    std::mt19937 op_generator(op_rand_dev());
    std::uniform_int_distribution<uint64_t>  distr_op(op_range_from, op_range_to);

    int seed = time(NULL);

    char *v = (char*)malloc(FLAGS_vlen);
    char *out = NULL;
    int ret = posix_memalign((void**) &out, 4096, buf_size);
    (void)ret;
    assert(out);
    uint64_t vlen_out = UINT64_MAX;
    uint64_t scan_len = 0;
    uint8_t key_len = sizeof(uint64_t);

    uint64_t k = UINT64_MAX;
    if(warm && !fill) {
        printf("Warming cache...\n");
        for(uint64_t i = start; i < end; i++) {
            k = keys->at(i);
            get(k, out, &vlen_out);
        }
        printf("Warming done.\n");
    }

    if(FLAGS_duration > 0 && !fill) {
        end = UINT64_MAX;
    }

    reset_stats();

    uint64_t orig_start = start;
    uint64_t orig_end = end;

    if(warmup && !fill) {
        end = UINT64_MAX;
    }

    for(uint64_t i = start; i < end; i++) {
        auto op = distr_op(op_generator);

again:
        if(append) {
            k = std::atomic_fetch_add(&slot, FLAGS_vlen) / BUF_SZ;
            //k = cur_append_key;
        } else if(fill) {
            k = keys->at(i);
        } else if(FLAGS_do_inserts && op < w_pct) {
            k = next_to_insert++;
        } else if(latest) {
            k = next_value_latestgen(next_to_insert - 1);
        } else if(zipf) {
            k = zipf_next();
        } else {
            k = distr(generator);
        }

        if(k == 0) {
            k = 1;
        }

        //printf("Putting key %lu (%lu out of %lu)\n", k, i - start, end - start);

        memcpy(v, &key_len, sizeof(uint8_t));
        memcpy(v + sizeof(uint8_t), &k, key_len);

        if(fill || (op < w_pct)) {
            if(!do_rmw || fill) {
                if(put(k, v, FLAGS_vlen, append)) {
                    /*
                     * Reached end of this append buffer.
                     */
                    //if(cur_append_key.compare_exchange_strong(k, k + 1)) {
                    printf("Full! New append key %lu\n", k + 1);
                    //}
                    goto again;
                }
            } else {
                rmw(k, FLAGS_vlen);
                relaxed_add(total_read, FLAGS_vlen);
            }

            relaxed_add(writes_done, 1);
            relaxed_add(done, 1);
            total_written += FLAGS_vlen;
        } else { 
            uint64_t start, end;
            if(sample && !do_scan) {
                start = NowMicros();
            }

            if(do_scan) {
                scan_len = rand_r((unsigned int*) &seed) % 100;
                if(scan_len == 0) {
                    scan_len = 1;
                }
                scan(k, scan_len, out);
                relaxed_add(total_read, scan_len * (sizeof(k) + FLAGS_vlen));
                relaxed_add(done, scan_len);
            } else {
                if(get(k, out, &vlen_out)) {
                    read_f++;
                }
                relaxed_add(total_read, sizeof(k) + FLAGS_vlen);
                relaxed_add(done, 1);
            }

            relaxed_add(reads_done, 1);
            if(sample) {
                end = NowMicros();
                if(read_hist) {
                    hdr_record_value(read_hist, end - start);
                }
            }
        }

        if((i % 10000 == 0) && (!fill && FLAGS_duration > 0 && elapsed >= FLAGS_duration)) {
            break;
        }

        if((i % 10000 == 0) && !warmup && !fill && end == UINT64_MAX) {
            i = orig_start;
            end = orig_end;
        }
    }

    close();

    if(num_started-- > 1) {
        while(go) {
            usleep(1);
        }
    } else {
        go = false;
    }

    free(out);
    free(v);
    return 0;
}

int ycsb(bool pop, bool warm, bool scan, bool latest, bool rmw) {
    std::vector<uint64_t> keys;

    if(pop) {
        for(int i = 0; i < FLAGS_num_pairs; i++) {
            keys.push_back(i);
        }
        //auto rng = std::default_random_engine {};
        //rng.seed(NowMicros());
        //printf("Shuffling keys...\n");
        //std::shuffle(std::begin(keys), std::end(keys), rng);
    } 
    
    reset_stats();
    empty_cache();

    std::thread threads[FLAGS_threads];
    if(pop) {
        for(int i = 0; i < FLAGS_threads; i++) {
            threads[i] = std::thread(worker, i, true, false, &keys, 
                                     FLAGS_num_pairs / FLAGS_threads * i,
                                     i == FLAGS_threads - 1 ? FLAGS_num_pairs : 
                                     FLAGS_num_pairs / FLAGS_threads * (i + 1), false, false, false,
                                     false, FLAGS_append);
        }

        for(int i = 0; i < FLAGS_threads; i++) {
            threads[i].join();
        }

        reset_stats();
        empty_cache();

        lprintf("FILL DONE\n");
    }

    if(pop_only) {
        return 0;
    }

    if(warm && !FLAGS_uniform) {
        std::unordered_set<uint64_t> working_set;
        while(working_set.size() < FLAGS_num_pairs * 0.15 && elapsed < 20) {
            if(!latest) {
                working_set.insert(zipf_next());
            } else {
                working_set.insert(next_value_latestgen(next_to_insert - 1));
            }
        }

        keys.clear();
        for(auto k : working_set) {
            keys.push_back(k);
        }
    }

    for(int i = 0; i < FLAGS_threads; i++) {
        uint64_t start = 0;
        uint64_t end = FLAGS_num_ops;

        if(warm && !FLAGS_uniform) {
            start = keys.size() / FLAGS_threads * i;
            end = i == FLAGS_threads - 1 ? keys.size(): 
                keys.size() / FLAGS_threads * (i + 1);
        }

        threads[i] = std::thread(worker, i, false, warm, &keys, start, end, scan, latest, rmw,
                                 i == 0 ? true : false, FLAGS_append);
    }

    for(int i = 0; i < FLAGS_threads; i++) {
        threads[i].join();
    }

    if(read_hist) {
	    hdr_percentiles_print(
			    read_hist,
			    m_log_fp,  // File to write to
			    5,  // Granularity of printed values
			    1.0,  // Multiplier for results
			    CLASSIC);  // Format CLASSIC/CSV supported.
        fflush(m_log_fp);
    }

    lprintf("%s %s DONE %lu ops/s. Writes done %lu Reads done %lu\n", zipf ? "ZIPF" : "UNIFORM", 
            cur_bench.c_str(),
            FLAGS_duration > 0 ?
            done / FLAGS_duration :
            (FLAGS_num_ops * FLAGS_threads) / elapsed,
            writes_done.load(), reads_done.load(std::memory_order_relaxed));

    reset_stats();
    empty_cache();

    free(read_hist);
    hdr_init(
            1,  // Minimum value
            INT64_C(3600000000),  // Maximum value
            2,  // Number of significant figures
            &read_hist);  // Pointer to initialise

    return 0;
}

void stats() {
    uint32_t count = 0;

warmup:
    printf("Not printing stats during %ld sec warmup...\n", FLAGS_warmup_time);
    while(!stats_stop) {
        sleep(1);
        count++;
        elapsed++;

        if(count >= FLAGS_warmup_time) {
            break;
        }
    }

    reset_stats();
    warmup = false; 

    hdr_init(
            1,  // Minimum value
            INT64_C(3600000000),  // Maximum value
            2,  // Number of significant figures
            &read_hist);  // Pointer to initialise

    uint64_t last = 0;
    while(!stats_stop) {
        uint64_t d = done.load();

        lprintf("Read fails %lu reads done %lu. ",
                read_f.load(), reads_done.load());
        lprintf("Done %lu. %.2f ops/s (%lu interval)\n", 
                d, (double) d / elapsed.load(), d - last);
        fflush(stdout);
        last = d;
        sleep(1);

        elapsed++;

        if(warmup) {
            count = 0;
            goto warmup;
        }
    }
}

int main(int argc, char** argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    char filename[64];
    memset(filename, 0x0, 32);
    sprintf(filename, "log_%s_%ld_%s_%lu", FLAGS_uniform ? "uniform" : "zipf",
                                           FLAGS_vlen, FLAGS_benchmarks.c_str(), NowMicros());
    m_log_fp = fopen(filename, "w");
    if(!m_log_fp) {
        assert(0);
    }

    lprintf("STORE %s.\n", FLAGS_store_name.c_str());
    lprintf("VLEN %lu.\n", FLAGS_vlen);
    lprintf("FLAGS_num_pairs %lu.\n", FLAGS_num_pairs);
    lprintf("FLAGS_duration %lu.\n", FLAGS_duration);
    lprintf("FLAGS_benchmarks %s.\n", FLAGS_benchmarks.c_str());

    std::thread stats_thread = std::thread(stats);

    std::stringstream ss(FLAGS_benchmarks);
    std::vector<std::string> benchmarks;

    hdr_init(
		    1,  // Minimum value
		    INT64_C(3600000000),  // Maximum value
		    3,  // Number of significant figures
		    &read_hist);  // Pointer to initialise

    while(ss.good()) {
        std::string substr;
        getline(ss, substr, ',' );
        benchmarks.push_back(substr);
    }

    if(!FLAGS_uniform) {
        init_zipf_generator(0, FLAGS_num_pairs, FLAGS_zipf_coef);
    }

    next_to_insert = FLAGS_num_pairs;

    buf_size = FLAGS_vlen + (4096 - (FLAGS_vlen % 4096));

    bool do_warm = FLAGS_warm_cache;
    bool do_pop = FLAGS_pop;
    bool do_scan = false;
    bool do_rmw = false;
    bool latest = false;

    for(auto bench : benchmarks) {
        cur_bench = bench;
        do_scan = false;
        do_rmw = false;
        latest = false;

        if(FLAGS_uniform) {
            zipf = false;
        } else {
            zipf = true;
        }

        if(bench == "a") {
            w_pct = 50;
        } else if(bench == "b") {
            w_pct = 5;
        } else if(bench == "c") {
            w_pct = 0;
        } else if(bench == "d") {
            if(zipf) {
                init_latestgen(FLAGS_num_pairs);
                latest = true;
            }
            w_pct = 5;
        } else if(bench == "e") {
            w_pct = 5;
            do_scan = true;
        } else if(bench == "f") {
            w_pct = 50;
            do_rmw = true;
        } else if(bench == "o") {
            w_pct = 1000;
        } else if(bench == "custom") {
            w_pct = FLAGS_w_pct;
        } else {
            pop_only = true;
        }

        ycsb(do_pop, do_warm, do_scan, latest, do_rmw);
        do_pop = false;
    }

    stats_stop = 1;
    stats_thread.join();
}
