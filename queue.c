#include "queue.h"

request_queue *queue_create(int capacity)
{
    request_queue *q = malloc(sizeof(*q));
    if (q == NULL)
        unix_error("Failed to allocate request queue.");

    q->buffer = malloc(sizeof(request_t) * capacity);
    if (q->buffer == NULL)
        unix_error("Failed to allocate request queue buffer.");

    q->capacity = capacity;
    q->size = 0;
    q->head = 0;
    q->tail = 0;

    int rc = pthread_mutex_init(&q->mutex, NULL);
    if (rc != 0)
        posix_error(rc, "pthread_mutex_init failed");

    rc = pthread_cond_init(&q->not_empty, NULL);
    if (rc != 0)
        posix_error(rc, "pthread_cond_init failed");

    rc = pthread_cond_init(&q->not_full, NULL);
    if (rc != 0)
        posix_error(rc, "pthread_cond_init failed");

    return q;
}

void queue_destroy(request_queue *q)
{
    if (q == NULL)
        return;

    int rc = pthread_mutex_destroy(&q->mutex);
    if (rc != 0)
        posix_error(rc, "pthread_mutex_destroy failed");

    rc = pthread_cond_destroy(&q->not_empty);
    if (rc != 0)
        posix_error(rc, "pthread_cond_destroy failed");

    rc = pthread_cond_destroy(&q->not_full);
    if (rc != 0)
        posix_error(rc, "pthread_cond_destroy failed");

    free(q->buffer);
    free(q);
}

void queue_push(request_queue *q, request_t req)
{
    int rc = pthread_mutex_lock(&q->mutex);
    if (rc != 0)
        posix_error(rc, "pthread_mutex_lock failed");

    while (q->size == q->capacity) {
        rc = pthread_cond_wait(&q->not_full, &q->mutex);
        if (rc != 0)
            posix_error(rc, "pthread_cond_wait failed");
    }

    q->buffer[q->tail] = req;
    q->tail = (q->tail + 1) % q->capacity;
    q->size++;

    rc = pthread_cond_signal(&q->not_empty);
    if (rc != 0)
        posix_error(rc, "pthread_cond_signal failed");

    rc = pthread_mutex_unlock(&q->mutex);
    if (rc != 0)
        posix_error(rc, "pthread_mutex_unlock failed");
}

request_t queue_pop(request_queue *q)
{
    request_t req;
    pthread_mutex_lock(&q->mutex);

    while (q->size == 0) {
        pthread_cond_wait(&q->not_empty, &q->mutex);

        if (q->size == 0) {
            req.connfd = -1;
            pthread_mutex_unlock(&q->mutex);
            return req;
        }
    }

    req = q->buffer[q->head];
    q->head = (q->head + 1) % q->capacity;
    q->size--;

    pthread_cond_signal(&q->not_full);
    pthread_mutex_unlock(&q->mutex);

    return req;
}