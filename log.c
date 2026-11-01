#include <stdlib.h>
#include <string.h>
#include "request.h"
#include "log.h"

#define INITIAL_LOG_CAPACITY 1024


// Opaque struct definition
struct Server_Log {
    char *data;
    int size;
    int capacity;

    pthread_mutex_t lock;
    pthread_cond_t can_read;
    pthread_cond_t can_write;

    int active_readers;
    int active_writers;
    int waiting_writers;
    double debug_sleep_time; // Store debug sleep configuration
};

static void ensure_capacity(server_log log, int extra_len)
{
    int needed = log->size + extra_len + 2; // + '#' + '\0'

    if (needed <= log->capacity)
        return;

    while (log->capacity < needed)
        log->capacity *= 2;

    log->data = realloc(log->data, log->capacity);
    if (log->data == NULL)
        unix_error("Failed to reallocate server log.");
}

// Creates a new server log instance (stub)
server_log create_log(double debug_sleep_time) {
    // TODO: Allocate and initialize internal log structure
    server_log log = malloc(sizeof(*log));
    if (log == NULL)
        unix_error("Failed to allocate server log.");

    log->data = malloc(INITIAL_LOG_CAPACITY);
    if (log->data == NULL)
        unix_error("Failed to allocate server log data.");
    log->debug_sleep_time = debug_sleep_time;
    log->data[0] = '\0';
    log->size = 0;
    log->capacity = INITIAL_LOG_CAPACITY;

    log->active_readers = 0;
    log->active_writers = 0;
    log->waiting_writers = 0;

    int rc = pthread_mutex_init(&log->lock, NULL);
    if (rc != 0)
        posix_error(rc, "pthread_mutex_init failed");

    rc = pthread_cond_init(&log->can_read, NULL);
    if (rc != 0)
        posix_error(rc, "pthread_cond_init failed");

    rc = pthread_cond_init(&log->can_write, NULL);
    if (rc != 0)
        posix_error(rc, "pthread_cond_init failed");

    return log;
}

// Destroys and frees the log (stub)
void destroy_log(server_log log) {
    // TODO: Free all internal resources used by the log
    if (log == NULL)
        return;

    int rc = pthread_mutex_destroy(&log->lock);
    if (rc != 0)
        posix_error(rc, "pthread_mutex_destroy failed");

    rc = pthread_cond_destroy(&log->can_read);
    if (rc != 0)
        posix_error(rc, "pthread_cond_destroy failed");

    rc = pthread_cond_destroy(&log->can_write);
    if (rc != 0)
        posix_error(rc, "pthread_cond_destroy failed");

    free(log->data);
    free(log);
}

// Returns dummy log content as string (stub)
// Corrected get_log in log_4.c
int get_log(server_log log, char** dst, time_stats* out_times) {
    if (gettimeofday(&out_times->log_enter, NULL) < 0) unix_error("gettimeofday failed");

    pthread_mutex_lock(&log->lock); // Acquire lock[cite: 14]

    while (log->active_writers > 0 || log->waiting_writers > 0) {
        pthread_cond_wait(&log->can_read, &log->lock);
    }
    log->active_readers++;

    if (log->debug_sleep_time > 0) {
        usleep((useconds_t)(log->debug_sleep_time * 1e6));
    }
    if (gettimeofday(&out_times->log_exit, NULL) < 0) unix_error("gettimeofday failed");

    // Perform copy while protected[cite: 2]
    *dst = malloc(log->size + 1);
    memcpy(*dst, log->data, log->size + 1);
    int copied_size = log->size;

    log->active_readers--;
    if (log->active_readers == 0)
        pthread_cond_signal(&log->can_write);

    pthread_mutex_unlock(&log->lock); // Unlock only after copy[cite: 14]
    return copied_size;
}

// Appends a new entry to the log (no-op stub)
void add_to_log(server_log log, const char* data, int data_len, time_stats* out_times) {
    if (gettimeofday(&out_times->log_enter, NULL) < 0)
        unix_error("gettimeofday failed");
    int rc = pthread_mutex_lock(&log->lock);
    if (rc != 0)
        posix_error(rc, "pthread_mutex_lock failed");

    log->waiting_writers++;

    while (log->active_writers > 0 || log->active_readers > 0) {
        rc = pthread_cond_wait(&log->can_write, &log->lock);
        if (rc != 0)
            posix_error(rc, "pthread_cond_wait failed");
    }

    log->waiting_writers--;
    log->active_writers = 1;
    if (log->debug_sleep_time > 0) {
        usleep((useconds_t)(log->debug_sleep_time * 1e6));
    }

    if (gettimeofday(&out_times->log_exit, NULL) < 0)
        unix_error("gettimeofday failed");

    ensure_capacity(log, data_len + 1);

    memcpy(log->data + log->size, data, data_len);
    log->size += data_len;

    log->data[log->size] = '#';
    log->size++;
    log->data[log->size] = '\0';

    log->active_writers = 0;
    if (log->waiting_writers > 0) {
        rc = pthread_cond_signal(&log->can_write);
    } else {
        rc = pthread_cond_broadcast(&log->can_read);
    }
    if (rc != 0)
        posix_error(rc, "pthread_cond_signal/broadcast failed");

    rc = pthread_mutex_unlock(&log->lock);
    if (rc != 0)
        posix_error(rc, "pthread_mutex_unlock failed");
}
