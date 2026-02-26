#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdbool.h>
#include <stdatomic.h>
#include <math.h>
#include <time.h>
#include <sys/time.h>

#ifdef __APPLE__
#include <sys/sysctl.h>
#include <mach/mach.h>
#include <arm_neon.h>
#include <arm_acle.h>
#define CACHE_LINE_SIZE 128
#else
#define CACHE_LINE_SIZE 64
#endif

typedef struct {
    _Atomic uint32_t hash;
    _Atomic uint16_t key_len;
    _Atomic uint16_t value_len;
    _Atomic uint32_t version;
    _Atomic bool occupied;
    char padding[2];
    char data[56];
} __attribute__((aligned(128))) HashEntry;

#define HASH_ENTRY_DATA_SIZE 56
#define MAX_KEY_SIZE 24
#define MAX_VALUE_SIZE 24

typedef struct {
    HashEntry *table;
    atomic_size_t table_size;
    atomic_size_t count;
    atomic_uint_fast64_t ops_count;
    atomic_bool resizing;
    atomic_uint_fast64_t collisions;
    
    uint64_t *bloom_bits;
    atomic_uint_fast64_t bloom_saves;
    
    atomic_uint_fast64_t total_sets;
    atomic_uint_fast64_t total_gets;
    atomic_uint_fast64_t total_exists;
    atomic_uint_fast64_t total_dels;
    
    atomic_uint_fast64_t verify_ok;
    atomic_uint_fast64_t verify_fail;
    
    bool use_neon;
    bool has_crc32;
} Soldb;

typedef struct {
    const char *name;
    bool (*init)(Soldb *db);
    uint32_t (*hash)(const char *key, size_t len);
    int (*compare)(const char *a, const char *b, size_t len);
} HashImpl;

static inline uint32_t hash_crc32c_fallback(const char *key, size_t len) {
    uint32_t crc = 0xFFFFFFFF;
    const char *p = key;
    while (len >= 4) {
        uint32_t data;
        memcpy(&data, p, 4);
        crc ^= data;
        for (int i = 0; i < 32; i++) {
            crc = (crc >> 1) ^ (0xEDB88320 & -(crc & 1));
        }
        p += 4;
        len -= 4;
    }
    while (len > 0) {
        crc ^= *p++;
        for (int i = 0; i < 8; i++) {
            crc = (crc >> 1) ^ (0xEDB88320 & -(crc & 1));
        }
        len--;
    }
    return ~crc;
}

#ifdef __APPLE__
static inline uint32_t hash_crc32c_arm(const char *key, size_t len) {
    uint32_t crc = 0xFFFFFFFF;
    const char *p = key;
    while (len >= 8) {
        uint64_t data;
        memcpy(&data, p, 8);
        crc = __crc32cd(crc, data);
        p += 8;
        len -= 8;
    }
    while (len >= 4) {
        uint32_t data;
        memcpy(&data, p, 4);
        crc = __crc32cw(crc, data);
        p += 4;
        len -= 4;
    }
    while (len > 0) {
        crc = __crc32cb(crc, *p++);
        len--;
    }
    return ~crc;
}
#endif

static int compare_generic(const char *a, const char *b, size_t len) {
    return memcmp(a, b, len);
}

#ifdef __APPLE__
static int compare_neon(const char *a, const char *b, size_t len) {
    while (len >= 16) {
        uint8x16_t va = vld1q_u8((const uint8_t *)a);
        uint8x16_t vb = vld1q_u8((const uint8_t *)b);
        uint8x16_t diff = veorq_u8(va, vb);
        if (vmaxvq_u8(diff) != 0) {
            return memcmp(a, b, len);
        }
        a += 16;
        b += 16;
        len -= 16;
    }
    if (len > 0) {
        return memcmp(a, b, len);
    }
    return 0;
}
#endif

static bool detect_cpu_features(Soldb *db) {
    db->use_neon = false;
    db->has_crc32 = false;
    
#ifdef __APPLE__
    int ret;
    size_t size = sizeof(ret);
    
    if (sysctlbyname("hw.optional.arm.FEAT_CRC32", &ret, &size, NULL, 0) == 0 && ret == 1) {
        db->has_crc32 = true;
        printf("[HW] CRC32C hardware acceleration: ENABLED\n");
    }
    
    if (sysctlbyname("hw.optional.neon", &ret, &size, NULL, 0) == 0 && ret == 1) {
        db->use_neon = true;
        printf("[HW] NEON SIMD: ENABLED\n");
    }
    
    int cores = 0;
    size = sizeof(cores);
    sysctlbyname("hw.perflevel0.physicalcpu", &cores, &size, NULL, 0);
    if (cores == 0) sysctlbyname("hw.physicalcpu", &cores, &size, NULL, 0);
    printf("[HW] CPU cores: %d\n", cores);
    
    uint64_t mem = 0;
    size = sizeof(mem);
    sysctlbyname("hw.memsize", &mem, &size, NULL, 0);
    printf("[HW] Memory: %lu GB\n", mem / (1024 * 1024 * 1024));
#else
    printf("[HW] Running on non-Apple platform, using software fallback\n");
#endif
    
    return true;
}

static uint32_t soldb_hash(Soldb *db, const char *key, size_t len) {
#ifdef __APPLE__
    if (db->has_crc32) {
        return hash_crc32c_arm(key, len);
    }
#endif
    return hash_crc32c_fallback(key, len);
}

static inline size_t hash_to_index(uint32_t hash, size_t size) {
    return hash & (size - 1);
}

static bool soldb_find(Soldb *db, uint32_t hash, const char *key, size_t key_len, 
                        size_t *idx_out, bool *found_out) {
    size_t size = atomic_load_explicit(&db->table_size, memory_order_acquire);
    size_t idx = hash_to_index(hash, size);
    int probes = 0;
    
    while (probes < 128) {
        HashEntry *e = &db->table[idx];
        
        bool occupied = atomic_load_explicit(&e->occupied, memory_order_acquire);
        
        if (!occupied) {
            *found_out = false;
            *idx_out = idx;
            return true;
        }
        
        uint32_t stored_hash = atomic_load_explicit(&e->hash, memory_order_relaxed);
        uint16_t stored_key_len = atomic_load_explicit(&e->key_len, memory_order_relaxed);
        
        if (stored_hash == hash && stored_key_len == key_len) {
            int cmp;
#ifdef __APPLE__
            if (db->use_neon) {
                cmp = compare_neon(e->data, key, key_len);
            } else {
                cmp = compare_generic(e->data, key, key_len);
            }
#else
            cmp = compare_generic(e->data, key, key_len);
#endif
            if (cmp == 0) {
                *found_out = true;
                *idx_out = idx;
                return true;
            }
        }
        
        if (probes > 0) {
            atomic_fetch_add_explicit(&db->collisions, 1, memory_order_relaxed);
        }
        
        idx = (idx + 1) & (size - 1);
        probes++;
    }
    
    *found_out = false;
    *idx_out = idx;
    return false;
}

static void soldb_resize(Soldb *db, size_t new_size) {
    HashEntry *old_table = db->table;
    size_t old_size = atomic_load_explicit(&db->table_size, memory_order_relaxed);
    
    HashEntry *new_table = (HashEntry *)aligned_alloc(128, new_size * sizeof(HashEntry));
    if (!new_table) return;
    
    memset(new_table, 0, new_size * sizeof(HashEntry));
    
    for (size_t i = 0; i < old_size; i++) {
        HashEntry *old_entry = &old_table[i];
        
        if (atomic_load_explicit(&old_entry->occupied, memory_order_relaxed)) {
            uint32_t hash = atomic_load_explicit(&old_entry->hash, memory_order_relaxed);
            uint16_t klen = atomic_load_explicit(&old_entry->key_len, memory_order_relaxed);
            uint16_t vlen = atomic_load_explicit(&old_entry->value_len, memory_order_relaxed);
            
            size_t idx = hash_to_index(hash, new_size);
            int probes = 0;
            
            while (probes < 128) {
                HashEntry *new_entry = &new_table[idx];
                
                if (!atomic_load_explicit(&new_entry->occupied, memory_order_relaxed)) {
                    memcpy(new_entry->data, old_entry->data, klen + vlen);
                    atomic_store_explicit(&new_entry->hash, hash, memory_order_relaxed);
                    atomic_store_explicit(&new_entry->key_len, klen, memory_order_relaxed);
                    atomic_store_explicit(&new_entry->value_len, vlen, memory_order_relaxed);
                    atomic_store_explicit(&new_entry->occupied, true, memory_order_relaxed);
                    break;
                }
                
                idx = (idx + 1) & (new_size - 1);
                probes++;
            }
        }
    }
    
    db->table = new_table;
    atomic_store_explicit(&db->table_size, new_size, memory_order_release);
    free(old_table);
}

static void soldb_maybe_resize(Soldb *db) {
    size_t count = atomic_load_explicit(&db->count, memory_order_relaxed);
    size_t size = atomic_load_explicit(&db->table_size, memory_order_relaxed);
    
    double load = (double)count / (double)size;
    
    if (load > 0.75) {
        bool expected = false;
        if (atomic_compare_exchange_strong_explicit(&db->resizing, &expected, true,
                                                     memory_order_acquire, memory_order_relaxed)) {
            soldb_resize(db, size * 2);
            atomic_store_explicit(&db->resizing, false, memory_order_release);
        }
    }
}

static void soldb_init(Soldb *db) {
    size_t initial_size = 65536;
    
    db->table = (HashEntry *)aligned_alloc(128, initial_size * sizeof(HashEntry));
    if (!db->table) {
        fprintf(stderr, "Failed to allocate memory\n");
        exit(1);
    }
    
    memset(db->table, 0, initial_size * sizeof(HashEntry));
    
    atomic_init(&db->table_size, initial_size);
    atomic_init(&db->count, 0);
    atomic_init(&db->ops_count, 0);
    atomic_init(&db->resizing, false);
    atomic_init(&db->collisions, 0);
    atomic_init(&db->bloom_saves, 0);
    atomic_init(&db->total_sets, 0);
    atomic_init(&db->total_gets, 0);
    atomic_init(&db->total_exists, 0);
    atomic_init(&db->total_dels, 0);
    atomic_init(&db->verify_ok, 0);
    atomic_init(&db->verify_fail, 0);
    
    size_t bloom_size = 16384;
    db->bloom_bits = (uint64_t *)calloc(bloom_size / 64, sizeof(uint64_t));
    
    detect_cpu_features(db);
    
    printf("[DB] Initialized with %zu slots\n", initial_size);
}

static bool soldb_get(Soldb *db, const char *key, size_t key_len, char *value_out, size_t *value_len_out);

static bool soldb_set(Soldb *db, const char *key, size_t key_len, const char *value, size_t value_len) {
    if (key_len == 0 || key_len > MAX_KEY_SIZE) return false;
    if (value_len == 0 || value_len > MAX_VALUE_SIZE) return false;
    if (key_len + value_len > HASH_ENTRY_DATA_SIZE) return false;
    
    uint32_t hash = soldb_hash(db, key, key_len);
    size_t idx;
    bool found;
    
    if (!soldb_find(db, hash, key, key_len, &idx, &found)) return false;
    
    HashEntry *e = &db->table[idx];
    
    memcpy(e->data, key, key_len);
    memcpy(e->data + key_len, value, value_len);
    
    atomic_store_explicit(&e->hash, hash, memory_order_relaxed);
    atomic_store_explicit(&e->key_len, (uint16_t)key_len, memory_order_relaxed);
    atomic_store_explicit(&e->value_len, (uint16_t)value_len, memory_order_relaxed);
    
    atomic_thread_fence(memory_order_release);
    
    if (!found) {
        atomic_store_explicit(&e->occupied, true, memory_order_release);
        atomic_fetch_add_explicit(&db->count, 1, memory_order_relaxed);
        soldb_maybe_resize(db);
    } else {
        atomic_fetch_add_explicit(&e->version, 1, memory_order_relaxed);
    }
    
    size_t bloom_idx = (hash + 0) % 16384;
    db->bloom_bits[bloom_idx / 64] |= (1ULL << (bloom_idx % 64));
    
    atomic_fetch_add_explicit(&db->ops_count, 1, memory_order_relaxed);
    atomic_fetch_add_explicit(&db->total_sets, 1, memory_order_relaxed);
    
    char verify_buf[MAX_VALUE_SIZE];
    size_t verify_len;
    if (soldb_get(db, key, key_len, verify_buf, &verify_len)) {
        if (verify_len == value_len && memcmp(verify_buf, value, value_len) == 0) {
            atomic_fetch_add_explicit(&db->verify_ok, 1, memory_order_relaxed);
        } else {
            atomic_fetch_add_explicit(&db->verify_fail, 1, memory_order_relaxed);
        }
    } else {
        atomic_fetch_add_explicit(&db->verify_fail, 1, memory_order_relaxed);
    }
    
    return true;
}

static bool soldb_get(Soldb *db, const char *key, size_t key_len, char *value_out, size_t *value_len_out) {
    if (key_len == 0 || key_len > MAX_KEY_SIZE) return false;
    
    uint32_t hash = soldb_hash(db, key, key_len);
    
    size_t bloom_idx = (hash + 0) % 16384;
    if (!(db->bloom_bits[bloom_idx / 64] & (1ULL << (bloom_idx % 64)))) {
        atomic_fetch_add_explicit(&db->bloom_saves, 1, memory_order_relaxed);
        atomic_fetch_add_explicit(&db->ops_count, 1, memory_order_relaxed);
        atomic_fetch_add_explicit(&db->total_gets, 1, memory_order_relaxed);
        return false;
    }
    
    size_t idx;
    bool found;
    
    if (!soldb_find(db, hash, key, key_len, &idx, &found)) return false;
    
    if (!found) {
        atomic_fetch_add_explicit(&db->ops_count, 1, memory_order_relaxed);
        atomic_fetch_add_explicit(&db->total_gets, 1, memory_order_relaxed);
        return false;
    }
    
    HashEntry *e = &db->table[idx];
    uint16_t vlen = atomic_load_explicit(&e->value_len, memory_order_relaxed);
    uint16_t klen = atomic_load_explicit(&e->key_len, memory_order_relaxed);
    
    memcpy(value_out, e->data + klen, vlen);
    *value_len_out = vlen;
    
    atomic_fetch_add_explicit(&db->ops_count, 1, memory_order_relaxed);
    atomic_fetch_add_explicit(&db->total_gets, 1, memory_order_relaxed);
    
    return true;
}

static bool soldb_exists(Soldb *db, const char *key, size_t key_len) {
    if (key_len == 0 || key_len > MAX_KEY_SIZE) return false;
    
    uint32_t hash = soldb_hash(db, key, key_len);
    
    size_t bloom_idx = (hash + 0) % 16384;
    if (!(db->bloom_bits[bloom_idx / 64] & (1ULL << (bloom_idx % 64)))) {
        atomic_fetch_add_explicit(&db->bloom_saves, 1, memory_order_relaxed);
        atomic_fetch_add_explicit(&db->ops_count, 1, memory_order_relaxed);
        atomic_fetch_add_explicit(&db->total_exists, 1, memory_order_relaxed);
        return false;
    }
    
    size_t idx;
    bool found;
    
    if (!soldb_find(db, hash, key, key_len, &idx, &found)) return false;
    
    atomic_fetch_add_explicit(&db->ops_count, 1, memory_order_relaxed);
    atomic_fetch_add_explicit(&db->total_exists, 1, memory_order_relaxed);
    
    return found;
}

static bool soldb_del(Soldb *db, const char *key, size_t key_len) {
    if (key_len == 0 || key_len > MAX_KEY_SIZE) return false;
    
    uint32_t hash = soldb_hash(db, key, key_len);
    size_t idx;
    bool found;
    
    if (!soldb_find(db, hash, key, key_len, &idx, &found)) return false;
    
    if (!found) {
        atomic_fetch_add_explicit(&db->ops_count, 1, memory_order_relaxed);
        atomic_fetch_add_explicit(&db->total_dels, 1, memory_order_relaxed);
        return false;
    }
    
    HashEntry *e = &db->table[idx];
    atomic_store_explicit(&e->occupied, false, memory_order_release);
    atomic_fetch_sub_explicit(&db->count, 1, memory_order_relaxed);
    
    atomic_fetch_add_explicit(&db->ops_count, 1, memory_order_relaxed);
    atomic_fetch_add_explicit(&db->total_dels, 1, memory_order_relaxed);
    
    return true;
}

static inline uint64_t get_time_ms(void) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (uint64_t)tv.tv_sec * 1000 + tv.tv_usec / 1000;
}

typedef struct {
    char *key;
    char *value;
} CSVEntry;

static int load_csv(const char *filename, CSVEntry **entries_out, size_t *count_out) {
    FILE *f = fopen(filename, "r");
    if (!f) {
        fprintf(stderr, "Cannot open %s\n", filename);
        return -1;
    }
    
    char line[256];
    fgets(line, sizeof(line), f);
    
    size_t capacity = 1000000;
    CSVEntry *entries = (CSVEntry *)malloc(capacity * sizeof(CSVEntry));
    size_t count = 0;
    
    while (fgets(line, sizeof(line), f) && count < capacity) {
        size_t len = strlen(line);
        if (len > 0 && line[len-1] == '\n') line[len-1] = 0;
        
        char *comma = strchr(line, ',');
        if (!comma) continue;
        
        *comma = 0;
        char *key = line;
        char *value = comma + 1;
        
        size_t key_len = strlen(key);
        size_t value_len = strlen(value);
        
        if (key_len >= MAX_KEY_SIZE || value_len >= MAX_VALUE_SIZE) continue;
        
        entries[count].key = strdup(key);
        entries[count].value = strdup(value);
        count++;
    }
    
    fclose(f);
    
    *entries_out = entries;
    *count_out = count;
    
    return 0;
}

static void free_csv(CSVEntry *entries, size_t count) {
    for (size_t i = 0; i < count; i++) {
        free(entries[i].key);
        free(entries[i].value);
    }
    free(entries);
}

#include <curses.h>
#include <unistd.h>
#include <pthread.h>

typedef struct {
    WINDOW *win;
    char title[32];
    int color;
    int row, col;
    int height, width;
} Panel;

typedef struct {
    Soldb *db;
    CSVEntry *entries;
    size_t entry_count;
    size_t pos;
    bool *running;
    
    Panel *panels;
    int panel_count;
    
    pthread_mutex_t display_mutex;
    
    uint64_t last_set, last_get, last_exists, last_del;
    uint64_t rate_set, rate_get, rate_exists, rate_del;
    uint64_t last_display_time;
    
    char set_buf[20][128];
    char get_buf[20][128];
    char exists_buf[20][128];
    char del_buf[20][128];
    char db_buf[20][128];
    int buf_idx;
} TUIContext;

static void init_panel(Panel *p, const char *title, int color, int row, int col, int h, int w) {
    p->win = newwin(h, w, row, col);
    strncpy(p->title, title, sizeof(p->title) - 1);
    p->color = color;
    p->row = row;
    p->col = col;
    p->height = h;
    p->width = w;
}

static void draw_panel(Panel *p) {
    if (!p->win) return;
    werase(p->win);
    wattron(p->win, COLOR_PAIR(p->color) | A_BOLD);
    box(p->win, 0, 0);
    mvwprintw(p->win, 0, 2, "[%s]", p->title);
    wattroff(p->win, COLOR_PAIR(p->color) | A_BOLD);
}

static void show_buffer(WINDOW *win, char buf[][128], int count, int start_row) {
    if (!win) return;
    for (int i = 0; i < count; i++) {
        int idx = (start_row + i) % 20;
        if (strlen(buf[idx]) > 0) {
            mvwprintw(win, start_row + i, 1, "%.90s", buf[idx]);
        }
    }
}

static void *ops_thread(void *arg) {
    TUIContext *ctx = (TUIContext *)arg;
    Soldb *db = ctx->db;
    CSVEntry *entries = ctx->entries;
    size_t count = ctx->entry_count;
    
    uint64_t start = get_time_ms();
    ctx->last_display_time = start;
    
    size_t display_interval = 500;
    int buf_count = 0;
    
    while (*ctx->running) {
        for (int batch = 0; batch < 5; batch++) {
            for (size_t i = 0; i < 50000 && ctx->pos < count; i++) {
                CSVEntry *e = &entries[ctx->pos];
                
                soldb_set(db, e->key, strlen(e->key), e->value, strlen(e->value));
                
                if (i % display_interval == 0) {
                    snprintf(ctx->set_buf[buf_count], sizeof(ctx->set_buf[0]), 
                             "SET: %s -> %s", e->key, e->value);
                    snprintf(ctx->db_buf[buf_count], sizeof(ctx->db_buf[0]), 
                             "%s -> %s", e->key, e->value);
                    buf_count = (buf_count + 1) % 20;
                }
                
                ctx->pos++;
                
                if (i % 50 == 0 && ctx->pos > 0) {
                    size_t rnd_idx = rand() % ctx->pos;
                    CSVEntry *ge = &entries[rnd_idx];
                    char buf[256];
                    size_t len;
                    
                    if (soldb_get(db, ge->key, strlen(ge->key), buf, &len)) {
                        if (i % display_interval == 0) {
                            snprintf(ctx->get_buf[buf_count % 20], sizeof(ctx->get_buf[0]),
                                     "GET: %.*s", (int)len, buf);
                            buf_count = (buf_count + 1) % 20;
                        }
                    }
                    
                    bool ex = soldb_exists(db, ge->key, strlen(ge->key));
                    if (i % display_interval == 0) {
                        snprintf(ctx->exists_buf[buf_count % 20], sizeof(ctx->exists_buf[0]),
                                 "EXISTS: %s = %s", ge->key, ex ? "YES" : "NO");
                        buf_count = (buf_count + 1) % 20;
                    }
                    
                    size_t neg_idx = count - 1 - (rand() % 10000);
                    if (neg_idx < count) {
                        soldb_exists(db, entries[neg_idx].key, strlen(entries[neg_idx].key));
                    }
                }
                
                if (i % 100 == 25 && ctx->pos > 100) {
                    size_t rnd_idx = rand() % ctx->pos;
                    CSVEntry *de = &entries[rnd_idx];
                    if (soldb_del(db, de->key, strlen(de->key))) {
                        snprintf(ctx->del_buf[buf_count % 20], sizeof(ctx->del_buf[0]),
                                 "DEL: %s", de->key);
                        buf_count = (buf_count + 1) % 20;
                    }
                }
            }
            
            if (ctx->pos >= count) {
                ctx->pos = 0;
            }
        }
        
        uint64_t now = get_time_ms();
        if (now - ctx->last_display_time >= 100) {
            pthread_mutex_lock(&ctx->display_mutex);
            
            ctx->rate_set = atomic_load(&db->total_sets) - ctx->last_set;
            ctx->rate_get = atomic_load(&db->total_gets) - ctx->last_get;
            ctx->rate_exists = atomic_load(&db->total_exists) - ctx->last_exists;
            ctx->rate_del = atomic_load(&db->total_dels) - ctx->last_del;
            
            ctx->last_set = atomic_load(&db->total_sets);
            ctx->last_get = atomic_load(&db->total_gets);
            ctx->last_exists = atomic_load(&db->total_exists);
            ctx->last_del = atomic_load(&db->total_dels);
            
            ctx->last_display_time = now;
            pthread_mutex_unlock(&ctx->display_mutex);
        }
        
        usleep(50000);
    }
    
    return NULL;
}

static void run_tui(TUIContext *ctx) {
    initscr();
    cbreak();
    noecho();
    curs_set(0);
    
    start_color();
    init_pair(1, COLOR_GREEN, COLOR_BLACK);
    init_pair(2, COLOR_YELLOW, COLOR_BLACK);
    init_pair(3, COLOR_CYAN, COLOR_BLACK);
    init_pair(4, COLOR_MAGENTA, COLOR_BLACK);
    init_pair(5, COLOR_RED, COLOR_BLACK);
    init_pair(6, COLOR_WHITE, COLOR_BLUE);
    init_pair(7, COLOR_BLACK, COLOR_WHITE);
    
    int R, C;
    getmaxyx(stdscr, R, C);
    
    if (R < 24 || C < 80) {
        endwin();
        fprintf(stderr, "Need 24x80, got %dx%d\n", R, C);
        return;
    }
    
    int h = (R - 3) / 2;
    int w = C / 3;
    
    Panel panels[6];
    init_panel(&panels[0], "SET", 1, 1, 0, h, w);
    init_panel(&panels[1], "DATABASE", 2, 1, w, h, w);
    init_panel(&panels[2], "GET", 3, 1, w * 2, h, w);
    init_panel(&panels[3], "EXISTS", 4, 1 + h, 0, h, w);
    init_panel(&panels[4], "DELETE", 5, 1 + h, w, h, w);
    init_panel(&panels[5], "OPS/SEC", 6, 1 + h, w * 2, h, w);
    
    for (int i = 0; i < 6; i++) {
        if (panels[i].win) scrollok(panels[i].win, true);
    }
    
    pthread_mutex_init(&ctx->display_mutex, NULL);
    
    pthread_t ops;
    pthread_create(&ops, NULL, ops_thread, ctx);
    
    uint64_t start_time = get_time_ms();
    
    while (*ctx->running) {
        pthread_mutex_lock(&ctx->display_mutex);
        
        for (int i = 0; i < 5; i++) {
            draw_panel(&panels[i]);
        }
        
        show_buffer(panels[0].win, ctx->set_buf, 20, 1);
        show_buffer(panels[1].win, ctx->db_buf, 20, 1);
        show_buffer(panels[2].win, ctx->get_buf, 20, 1);
        show_buffer(panels[3].win, ctx->exists_buf, 20, 1);
        show_buffer(panels[4].win, ctx->del_buf, 20, 1);
        
        WINDOW *rate_win = panels[5].win;
        werase(rate_win);
        wattron(rate_win, COLOR_PAIR(6) | A_BOLD);
        box(rate_win, 0, 0);
        mvwprintw(rate_win, 0, 2, "[OPS/SEC]");
        
        uint64_t ops = ctx->rate_set * 10;
        uint64_t ops_get = ctx->rate_get * 10;
        uint64_t ops_exists = ctx->rate_exists * 10;
        uint64_t ops_del = ctx->rate_del * 10;
        
        mvwprintw(rate_win, 1, 1, "SET:   %6lu/s", (unsigned long)ops);
        mvwprintw(rate_win, 2, 1, "GET:   %6lu/s", (unsigned long)ops_get);
        mvwprintw(rate_win, 3, 1, "EXISTS:%6lu/s", (unsigned long)ops_exists);
        mvwprintw(rate_win, 4, 1, "DELETE:%6lu/s", (unsigned long)ops_del);
        mvwprintw(rate_win, 5, 1, "TOTAL: %6lu/s", (unsigned long)(ops + ops_get + ops_exists + ops_del));
        
        uint64_t total_sets = atomic_load(&ctx->db->total_sets);
        uint64_t total_gets = atomic_load(&ctx->db->total_gets);
        uint64_t total_exists = atomic_load(&ctx->db->total_exists);
        uint64_t total_dels = atomic_load(&ctx->db->total_dels);
        size_t db_count = atomic_load(&ctx->db->count);
        size_t db_capacity = atomic_load(&ctx->db->table_size);
        
        int elapsed = (int)((get_time_ms() - start_time) / 1000);
        
        wattroff(rate_win, COLOR_PAIR(6) | A_BOLD);
        
        for (int i = 0; i < 6; i++) {
            wrefresh(panels[i].win);
        }
        
        attron(COLOR_PAIR(6));
        uint64_t verify_ok = atomic_load(&ctx->db->verify_ok);
        uint64_t verify_fail = atomic_load(&ctx->db->verify_fail);
        mvprintw(R - 1, 0, "SET:%lu GET:%lu EXISTS:%lu DEL:%lu | DB:%zu/%zu | VFY:%lu/%lu | %02ds | %s",
                 (unsigned long)total_sets, (unsigned long)total_gets,
                 (unsigned long)total_exists, (unsigned long)total_dels,
                 db_count, db_capacity, verify_ok, verify_fail, elapsed,
                 ctx->db->has_crc32 ? "CRC32-HW" : "CRC32-SW");
        attroff(COLOR_PAIR(6));
        
        refresh();
        doupdate();
        
        pthread_mutex_unlock(&ctx->display_mutex);
        
        if (elapsed >= 20) {
            *ctx->running = false;
        }
        
        usleep(50000);
    }
    
    pthread_join(ops, NULL);
    
    sleep(2);
    
    for (int i = 0; i < 6; i++) {
        if (panels[i].win) delwin(panels[i].win);
    }
    
    endwin();
    
    printf("\n=== FINAL STATS ===\n");
    printf("SET:   %lu\n", (unsigned long)atomic_load(&ctx->db->total_sets));
    printf("GET:   %lu\n", (unsigned long)atomic_load(&ctx->db->total_gets));
    printf("EXISTS:%lu\n", (unsigned long)atomic_load(&ctx->db->total_exists));
    printf("DELETE:%lu\n", (unsigned long)atomic_load(&ctx->db->total_dels));
    printf("DB size: %zu\n", (size_t)atomic_load(&ctx->db->count));
    printf("Collisions: %lu\n", (unsigned long)atomic_load(&ctx->db->collisions));
    printf("Bloom saves: %lu\n", (unsigned long)atomic_load(&ctx->db->bloom_saves));
    printf("Verify OK:   %lu\n", (unsigned long)atomic_load(&ctx->db->verify_ok));
    printf("Verify FAIL: %lu\n", (unsigned long)atomic_load(&ctx->db->verify_fail));
}

int main(int argc, char *argv[]) {
    if (argc < 2) {
        fprintf(stderr, "Usage: %s <csv_file>\n", argv[0]);
        return 1;
    }
    
    printf("=== M1-Optimized Redis Clone ===\n");
    printf("Hardware-accelerated key-value store\n\n");
    
    Soldb db;
    soldb_init(&db);
    
    CSVEntry *entries = NULL;
    size_t count = 0;
    
    if (load_csv(argv[1], &entries, &count) < 0) {
        return 1;
    }
    
    printf("Loaded %zu entries from CSV\n\n", count);
    
    bool running = true;
    
    TUIContext ctx = {0};
    ctx.db = &db;
    ctx.entries = entries;
    ctx.entry_count = count;
    ctx.running = &running;
    ctx.pos = 0;
    
    memset(ctx.set_buf, 0, sizeof(ctx.set_buf));
    memset(ctx.get_buf, 0, sizeof(ctx.get_buf));
    memset(ctx.exists_buf, 0, sizeof(ctx.exists_buf));
    memset(ctx.del_buf, 0, sizeof(ctx.del_buf));
    memset(ctx.db_buf, 0, sizeof(ctx.db_buf));
    
    run_tui(&ctx);
    
    free_csv(entries, count);
    
    return 0;
}
