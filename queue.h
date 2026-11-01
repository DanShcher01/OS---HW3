#ifndef REQUEST_QUEUE_H
#define REQUEST_QUEUE_H

#include "segel.h"
#include "request.h"

typedef struct {
    int connfd;
    time_stats times;
} request_t;

typedef struct RequestQueue {
    request_t *buffer;
    int capacity;
    int size;
    int head;
    int tail;

    pthread_mutex_t mutex;
    pthread_cond_t not_empty;
    pthread_cond_t not_full;
} request_queue;

request_queue *queue_create(int capacity);
void queue_destroy(request_queue *q);

void queue_push(request_queue *q, request_t req);
request_t queue_pop(request_queue *q);

#endif