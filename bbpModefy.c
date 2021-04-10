#include <math.h>
#include <stdio.h>
#include <errno.h>
#include <pthread.h>
#include <stdlib.h>
#include <time.h>
#include <stddef.h>

#define PRECISION 100 /* upper bound in BPP sum */

typedef struct __tpool_future *tpool_future_t;
typedef struct __threadpool *tpool_t;

enum __future_flags {
    __FUTURE_RUNNING = 01,
    __FUTURE_FINISHED = 02,
    __FUTURE_TIMEOUT = 04,
    __FUTURE_CANCELLED = 010,
    __FUTURE_DESTROYED = 020,
};

typedef struct __threadtask {
    void *(*func)(void *);
    void *arg;
    struct __tpool_future *future;
    struct __threadtask *next;
    struct __threadtask *prev;
} threadtask_t;

typedef struct __jobqueue {
    threadtask_t *head, *tail;
    pthread_cond_t cond_nonempty;
    pthread_mutex_t rwlock;
} jobqueue_t;

struct __tpool_future {
    int flag;
    void *result;
    pthread_mutex_t mutex;
    //pthread_cond_t cond_finished;
};

struct __threadpool {
    size_t count;
    pthread_t *workers;
    jobqueue_t *jobqueue;
    jobqueue_t *readqueue;
};

/**
 * Create a thread pool containing specified number of threads.
 * If successful, the thread pool is returned. Otherwise, it
 * returns NULL.
 */
tpool_t tpool_create(size_t count);

/**
 * Schedules the specific function to be executed.
 * If successful, a future object representing the execution of
 * the task is returned. Otherwise, it returns NULL.
 */
tpool_future_t tpool_apply(tpool_t pool, void *(*func)(void *), void *arg);

/**
 * Wait for all pending tasks to complete before destroying the thread pool.
 */
int tpool_join(tpool_t pool);

/**
 * Return the result when it becomes available.
 * If @seconds is non-zero and the result does not arrive within specified time,
 * NULL is returned. Each tpool_future_get() resets the timeout status on
 * @future.
 */
void *tpool_future_get(tpool_future_t future, unsigned int seconds);

/**
 * Destroy the future object and free resources once it is no longer used.
 * It is an error to refer to a destroyed future object. Note that destroying
 * a future object does not prevent a pending task from being executed.
 */
int tpool_future_destroy(tpool_future_t future);

static void readqueue_push(jobqueue_t *readqueue, threadtask_t *new_head);

static struct __tpool_future *tpool_future_create(void)
{
    struct __tpool_future *future = malloc(sizeof(struct __tpool_future));
    if (future) {
        future->flag = 0;
        future->result = NULL;
        pthread_mutex_init(&future->mutex, NULL);
        //pthread_condattr_t attr;
        //pthread_condattr_init(&attr);
        //pthread_cond_init(&future->cond_finished, &attr);
        //pthread_condattr_destroy(&attr);
    }
    return future;
}

int tpool_future_destroy(struct __tpool_future *future)
{
    if (future) {
        pthread_mutex_lock(&future->mutex);
        if (future->flag & __FUTURE_FINISHED ||
            future->flag & __FUTURE_CANCELLED) {
            pthread_mutex_unlock(&future->mutex);
            pthread_mutex_destroy(&future->mutex);
            //pthread_cond_destroy(&future->cond_finished);
            free(future);
        } else {
            future->flag |= __FUTURE_DESTROYED;
            pthread_mutex_unlock(&future->mutex);
        }
    }
    return 0;
}

// void *tpool_future_get(struct __tpool_future *future, unsigned int seconds)
// {
//     pthread_mutex_lock(&future->mutex);
//     /* turn off the timeout bit set previously */
//     future->flag &= ~__FUTURE_TIMEOUT;
//     while ((future->flag & __FUTURE_FINISHED) == 0) {
//         if (seconds) {
//             struct timespec expire_time;
//             clock_gettime(CLOCK_MONOTONIC, &expire_time);
//             expire_time.tv_sec += seconds;
//             int status = pthread_cond_timedwait(&future->cond_finished,
//                                                 &future->mutex, &expire_time);
//             if (status == ETIMEDOUT) {
//                 future->flag |= __FUTURE_TIMEOUT;
//                 pthread_mutex_unlock(&future->mutex);
//                 return NULL;
//             }
//         } else
//             pthread_cond_wait(&future->cond_finished, &future->mutex);
//     }

//     pthread_mutex_unlock(&future->mutex);
//     return future->result;
// }

static jobqueue_t *jobqueue_create(void)
{
    jobqueue_t *jobqueue = malloc(sizeof(jobqueue_t));
    if (jobqueue) {
        jobqueue->head = jobqueue->tail = NULL;
        pthread_cond_init(&jobqueue->cond_nonempty, NULL);
        pthread_mutex_init(&jobqueue->rwlock, NULL);
    }
    return jobqueue;
}

static void jobqueue_destroy(jobqueue_t *jobqueue)
{
    threadtask_t *tmp = jobqueue->head;
    while (tmp) {
        jobqueue->head = jobqueue->head->next;
        pthread_mutex_lock(&tmp->future->mutex);
        if (tmp->future->flag & __FUTURE_DESTROYED) {
            pthread_mutex_unlock(&tmp->future->mutex);
            pthread_mutex_destroy(&tmp->future->mutex);
            //pthread_cond_destroy(&tmp->future->cond_finished);
            free(tmp->future);
        } else {
            tmp->future->flag |= __FUTURE_CANCELLED;
            pthread_mutex_unlock(&tmp->future->mutex);
        }
        free(tmp);
        tmp = jobqueue->head;
    }

    pthread_mutex_destroy(&jobqueue->rwlock);
    pthread_cond_destroy(&jobqueue->cond_nonempty);
    free(jobqueue);
}

static void __jobqueue_fetch_cleanup(void *arg)
{
    pthread_mutex_t *mutex = (pthread_mutex_t *) arg;
    pthread_mutex_unlock(mutex);
}

static void *jobqueue_fetch(void *tpool)
{
    struct __threadpool *pool = (struct __threadpool *)tpool;
    jobqueue_t *jobqueue = pool->jobqueue;
    jobqueue_t *readqueue = pool->readqueue;
    threadtask_t *task;
    int old_state;

    pthread_cleanup_push(__jobqueue_fetch_cleanup, (void *) &jobqueue->rwlock);

    while (1) {
        pthread_mutex_lock(&jobqueue->rwlock);
        pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, &old_state);
        pthread_testcancel();

        while (!jobqueue->tail)
            pthread_cond_wait(&jobqueue->cond_nonempty, &jobqueue->rwlock);

        pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, &old_state);
        //pthread fetch a task from tail of jobqueue
        if (jobqueue->head == jobqueue->tail) {
            task = jobqueue->tail;
            jobqueue->head = jobqueue->tail = NULL;
        } else {
            threadtask_t *tmp = jobqueue->tail->prev;
            task = tmp->next;
            tmp->next = NULL;
            task->prev = NULL;
            jobqueue->tail = tmp;
        }
        pthread_mutex_unlock(&jobqueue->rwlock);

        if (task->func) {
            pthread_mutex_lock(&task->future->mutex);
            if (task->future->flag & __FUTURE_CANCELLED) {
                pthread_mutex_unlock(&task->future->mutex);
                free(task);
                continue;
            } else {
                task->future->flag |= __FUTURE_RUNNING;
                pthread_mutex_unlock(&task->future->mutex);
            }

            void *ret_value = task->func(task->arg);
            pthread_mutex_lock(&task->future->mutex);
            if (task->future->flag & __FUTURE_DESTROYED) {
                pthread_mutex_unlock(&task->future->mutex);
                pthread_mutex_destroy(&task->future->mutex);
                //pthread_cond_destroy(&task->future->cond_finished);
                free(task->future);
            } else {
                task->future->flag |= __FUTURE_FINISHED;
                task->future->result = ret_value;
                readqueue_push(readqueue, task);/*When the work is done , push this task into readqueue*/
                //pthread_cond_broadcast(&task->future->cond_finished);
                pthread_mutex_unlock(&task->future->mutex);
            }
        } else {
            pthread_mutex_destroy(&task->future->mutex);
           //pthread_cond_destroy(&task->future->cond_finished);
            free(task->future);
            free(task);
            break;
        }
    }
    pthread_cleanup_pop(0);
    pthread_exit(NULL);
}

double readqueue_fetch(struct __threadpool *tpool, int max_term)
{
    jobqueue_t *readqueue = tpool->readqueue;
    threadtask_t *task;
    int old_state;
    double retval = 0.0;
    int count = 0;

    while (count < max_term) {
        pthread_mutex_lock(&readqueue->rwlock);
        while (!readqueue->tail){
            pthread_cond_wait(&readqueue->cond_nonempty, &readqueue->rwlock);
        }
        /*Fetch new task from tail*/   
        if (readqueue->head == readqueue->tail) {
            task = readqueue->tail;
            readqueue->head = readqueue->tail = NULL;
        } else {
            threadtask_t *tmp = readqueue->tail->prev;
            task = tmp->next;
            tmp->next = NULL;
            readqueue->tail = tmp;
        }
        count++;
        if (task->future) {
            pthread_mutex_lock(&task->future->mutex);
            retval += *(double *)task->future->result;
            pthread_mutex_unlock(&task->future->mutex);
            pthread_mutex_destroy(&task->future->mutex);
            //pthread_cond_destroy(&task->future->cond_finished);
            free(task->future);
            free(task);
        } else {
            pthread_mutex_unlock(&task->future->mutex);
            pthread_mutex_destroy(&task->future->mutex);
            //pthread_cond_destroy(&task->future->cond_finished);
            free(task->future);
            free(task);
            break;
        }
    pthread_mutex_unlock(&readqueue->rwlock);    
    }
    return retval;
}

static void readqueue_push(jobqueue_t *readqueue, threadtask_t *new_head)
{
    pthread_mutex_lock(&readqueue->rwlock);
    if (new_head) {
        if (readqueue->head) {
            readqueue->head->prev = new_head;
            new_head->next = readqueue->head;
            readqueue->head = new_head;
        } else {
            readqueue->head = readqueue->tail = new_head;
            pthread_cond_broadcast(&readqueue->cond_nonempty);
        }

        int tasknum = *(int *)new_head->arg;
        pthread_mutex_unlock(&readqueue->rwlock);
    }
    else{
        printf("Error : readqueue task doesn't exist\n");
    }
}

struct __threadpool *tpool_create(size_t count)
{
    jobqueue_t *jobqueue = jobqueue_create();
    jobqueue_t *readqueue = jobqueue_create();
    struct __threadpool *pool = malloc(sizeof(struct __threadpool));
    if (!jobqueue || !pool ||!readqueue) {
        if (jobqueue)
            jobqueue_destroy(jobqueue);
        if (readqueue)
            jobqueue_destroy(readqueue);
        free(pool);
        return NULL;
    }
    pool->count = count, pool->jobqueue = jobqueue,pool->readqueue = readqueue;;
    if ((pool->workers = malloc(count * sizeof(pthread_t)))) {
        for (int i = 0; i < count; i++) {
            if (pthread_create(&pool->workers[i], NULL, jobqueue_fetch,
                               (void *) pool)) {
                for (int j = 0; j < i; j++)
                    pthread_cancel(pool->workers[j]);
                for (int j = 0; j < i; j++)
                    pthread_join(pool->workers[j], NULL);
                free(pool->workers);
                jobqueue_destroy(jobqueue);
                jobqueue_destroy(readqueue);
                free(pool);
                return NULL;
            }
        }
        return pool;
    }
    jobqueue_destroy(readqueue);
    jobqueue_destroy(jobqueue);
    free(pool);
    return NULL;
}

struct __tpool_future *tpool_apply(struct __threadpool *pool,
                                   void *(*func)(void *),
                                   void *arg)
{
    jobqueue_t *jobqueue = pool->jobqueue;
    threadtask_t *new_head = malloc(sizeof(threadtask_t));
    struct __tpool_future *future = tpool_future_create();
    if (new_head && future) {
        new_head->func = func, new_head->arg = arg, new_head->future = future;
        pthread_mutex_lock(&jobqueue->rwlock);
        if (jobqueue->head) {
            jobqueue->head->prev = new_head;
            new_head->next = jobqueue->head;
            jobqueue->head = new_head;
        } else {
            jobqueue->head = jobqueue->tail = new_head;
            pthread_cond_broadcast(&jobqueue->cond_nonempty);;
        }
        pthread_mutex_unlock(&jobqueue->rwlock);
    } else if (new_head) {
        free(new_head);
        return NULL;
    } else if (future) {
        tpool_future_destroy(future);
        return NULL;
    }
    return future;
}

int tpool_join(struct __threadpool *pool)
{
    size_t num_threads = pool->count;
    for (int i = 0; i < num_threads; i++)
        tpool_apply(pool, NULL, NULL);
    for (int i = 0; i < num_threads; i++)
        pthread_join(pool->workers[i], NULL);
    free(pool->workers);
    jobqueue_destroy(pool->jobqueue);
    free(pool);
    return 0;
}

/* Use Bailey–Borwein–Plouffe formula to approximate PI */
static void *bpp(void *arg)
{
    int k = *(int *) arg;
    double sum = (4.0 / ((k << 3)+ 1)) - (2.0 / ((k << 3) + 4)) -
                 (1.0 / ((k << 3) + 5)) - (1.0 / ((k << 3) + 6));
    double *product = malloc(sizeof(double));
    if (product)
        *product = 1 / pow(16, k) * sum;
    return (void *) product;
}

int main()
{
    int bpp_args[PRECISION + 1];
    int sum_count = 0;
    double bpp_sum = 0;
    tpool_t pool = tpool_create(6);
    tpool_future_t futures[PRECISION + 1];

    for (int i = 0; i <= PRECISION; i++) {
        bpp_args[i] = i;
        futures[i] = tpool_apply(pool, bpp, (void *) &bpp_args[i]);
    }
    

    /*When readqueue is available , read value from the head . If  readqueue is empty , wait for broadcast*/

    bpp_sum = readqueue_fetch(pool, PRECISION + 1);



    // for (int i = 0; i <= PRECISION; i++) {
    //     double *result = tpool_future_get(futures[i], 0);
    //     bpp_sum += *result;
    //     tpool_future_destroy(futures[i]);
    //     free(result);
    // }


    tpool_join(pool);
    printf("PI calculated with %d terms: %.15f\n", PRECISION + 1, bpp_sum);
    return 0;
}