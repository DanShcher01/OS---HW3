#include "segel.h"
#include "request.h"
#include "log.h"
#include "queue.h"

//
// server.c: A very, very simple web server
//
// To run:
//  ./server <portnum (above 2000)>
//
// Repeatedly handles HTTP requests sent to this port number.
// Most of the work is done within routines written in request.c
//
int total_connections = 0;
pthread_cond_t can_accept = PTHREAD_COND_INITIALIZER;
// Parses command-line arguments
void getargs(int *tcp_port, int *udp_port, int *threads,
             int *queue_size, double *debug_sleep_time,
             int argc, char *argv[])
{
    if (argc != 6)
        app_error("Usage: ./server <tcp_port> <udp_port> <threads> <queue_size> <debug_sleep_time>");

    *tcp_port = atoi(argv[1]);
    *udp_port = atoi(argv[2]);
    *threads = atoi(argv[3]);
    *queue_size = atoi(argv[4]);
    *debug_sleep_time = atof(argv[5]);

    if (*tcp_port <= 1024 || *tcp_port > 65535) {
        fprintf(stderr, "Invalid TCP port.\n");
        exit(1);
    }

    if (*udp_port <= 1024 || *udp_port > 65535) {
        fprintf(stderr, "Invalid UDP port.\n");
        exit(1);
    }

    if (*tcp_port == *udp_port) {
        fprintf(stderr, "TCP and UDP ports must be different.\n");
        exit(1);
    }

    if (*threads <= 0) {
        fprintf(stderr, "Number of worker threads must be positive.\n");
        exit(1);
    }

    if (*queue_size <= 0) {
        fprintf(stderr, "Queue size must be positive.\n");
        exit(1);
    }
}

// TODO: HW3 — Task 4: Add the UDP channel (see the UDP_* wrappers in segel.c).
typedef struct UdpNode {
    struct sockaddr_in addr;
    struct UdpNode *next;
} UdpNode;

typedef struct {
    UdpNode *head;
    UdpNode *tail;
    pthread_mutex_t lock;
    pthread_cond_t cond; // Added condition variable
} UdpQueue;

int udp_fd;
UdpQueue *udp_queues;

void flush_udp_requests(int thread_id, threads_stats stats) {
    int idx = thread_id - 1;
    while (1) {
        UdpNode *node = NULL;

        // Safely dequeue a UDP request
        pthread_mutex_lock(&udp_queues[idx].lock);
        if (udp_queues[idx].head) {
            node = udp_queues[idx].head;
            udp_queues[idx].head = node->next;
            if (!udp_queues[idx].head) {
                udp_queues[idx].tail = NULL;
            }
        }
        pthread_mutex_unlock(&udp_queues[idx].lock);

        // If the queue is empty, break out and process TCP
        if (!node) break;

        // Formulate the thread statistics and send back via UDP
        char stats_buf[MAXBUF] = {0};
        append_thread_log(stats_buf, stats);
        UDP_Write(udp_fd, &node->addr, stats_buf, strlen(stats_buf));

        free(node);
    }
}


// TODO: HW3 — Task 1: Initialize the thread pool and request queue.

typedef struct {
    request_queue *queue;
    server_log log;
    threads_stats stats;
} worker_args;

// Correct worker loop pattern:
void *worker(void *arg) {
    worker_args *wargs = (worker_args *)arg;
    while (1) {
        // 1. Process TCP requests (Blocking)
        request_t req = queue_pop(wargs->queue);

        if (req.connfd != -1) {
            requestHandle(req.connfd, req.times, wargs->stats, wargs->log);
            Close(req.connfd);

            // 2. Capacity management
            pthread_mutex_lock(&wargs->queue->mutex);
            total_connections--;
            pthread_cond_signal(&can_accept);
            pthread_mutex_unlock(&wargs->queue->mutex);
        }

        // 3. Process UDP (Non-blocking)
        flush_udp_requests(wargs->stats->id, wargs->stats);
    }
}


// TODO: HW3 — Extend getargs() to parse the full argument list.

int main(int argc, char *argv[])
{
    int listenfd, connfd, tcp_port, clientlen, udp_port, threads, queue_size;
    double debug_sleep_time;
    struct sockaddr_in clientaddr;
    getargs(&tcp_port, &udp_port, &threads, &queue_size, &debug_sleep_time, argc, argv);
    server_log log = create_log(debug_sleep_time);
    // Task 1 
    request_queue *request_q = queue_create(queue_size);
    // Task 4
    udp_queues = malloc(sizeof(UdpQueue) * threads);
    for (int i = 0; i < threads; i++) {
        udp_queues[i].head = NULL;
        udp_queues[i].tail = NULL;
        pthread_mutex_init(&udp_queues[i].lock, NULL);
        pthread_cond_init(&udp_queues[i].cond, NULL);
    }
    pthread_t *workers = malloc(sizeof(*workers) * threads);
    if (workers == NULL)
        unix_error("Failed to allocate worker threads.");

    worker_args *wargs = malloc(sizeof(*wargs) * threads);
    if (wargs == NULL)
        unix_error("Failed to allocate worker arguments.");

    for (int i = 0; i < threads; i++) {
        wargs[i].queue = request_q;
        wargs[i].log = log;
        wargs[i].stats = malloc(sizeof(struct Threads_stats));

        if (wargs[i].stats == NULL)
            unix_error("Failed to allocate thread statistics.");
        wargs[i].stats->id = i + 1;
        wargs[i].stats->stat_req = 0;
        wargs[i].stats->dynm_req = 0;
        wargs[i].stats->post_req = 0;
        wargs[i].stats->total_req = 0;

        int rc = pthread_create(&workers[i], NULL, worker, &wargs[i]);
        if (rc != 0)
            posix_error(rc, "pthread_create failed");
    }
    listenfd = Open_listenfd(tcp_port);
    udp_fd = UDP_Open(udp_port);

    fd_set read_fds;
    int max_fd = (listenfd > udp_fd) ? listenfd : udp_fd;

    while (1) {
        FD_ZERO(&read_fds);
        FD_SET(listenfd, &read_fds);
        FD_SET(udp_fd, &read_fds);

        Select(max_fd + 1, &read_fds, NULL, NULL, NULL);

        if (FD_ISSET(udp_fd, &read_fds)) {
            struct sockaddr_in clientaddr;
            char recv_buf[MAXLINE] = {0};
            int n = UDP_Read(udp_fd, &clientaddr, recv_buf, MAXLINE - 1);

            if (n > 0) {
                int target_thread_id = atoi(recv_buf);
                if (target_thread_id >= 1 && target_thread_id <= threads) {
                    UdpNode *new_node = malloc(sizeof(UdpNode));
                    new_node->addr = clientaddr;
                    new_node->next = NULL;

                    int idx = target_thread_id - 1;
                    pthread_mutex_lock(&udp_queues[idx].lock);
                    if (udp_queues[idx].tail) {
                        udp_queues[idx].tail->next = new_node;
                    } else {
                        udp_queues[idx].head = new_node;
                    }
                    udp_queues[idx].tail = new_node;
                    pthread_mutex_unlock(&udp_queues[idx].lock);

                    pthread_mutex_lock(&request_q->mutex);
                    pthread_cond_broadcast(&request_q->not_empty);
                    pthread_mutex_unlock(&request_q->mutex);
                }
            }

        }
        if (FD_ISSET(listenfd, &read_fds)) {
            struct sockaddr_in clientaddr;
            int clientlen = sizeof(clientaddr);
            int connfd = Accept(listenfd, (SA *)&clientaddr, (socklen_t *)&clientlen);

            pthread_mutex_lock(&request_q->mutex);
            while (total_connections >= queue_size) {
                pthread_cond_wait(&can_accept, &request_q->mutex);
            }
            total_connections++;
            pthread_mutex_unlock(&request_q->mutex);

            request_t req;
            req.connfd = connfd;
            gettimeofday(&req.times.task_arrival, NULL);
            queue_push(request_q, req);
        }
    }
    // TODO: HW3 — Add cleanup code for the thread pool and queue.
    destroy_log(log);
    queue_destroy(request_q);
    return 0;

}
