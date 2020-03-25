#ifndef _DB_UTILS_THREAD_POOL_H_
#define _DB_UTILS_THREAD_POOL_H_

#include "task.h"

class ThreadPool {
 public:
  virtual ~ThreadPool() {}

  // Causes 'task' to be scheduled for background by a thread in the threadpool.
  virtual void RunTask(Task* task) = 0;

  // Returns the number of active physical pthreads currently consituting the
  // threadpool.
  virtual int ThreadCount() = 0;
};

#endif  // _DB_UTILS_THREAD_POOL_H_
