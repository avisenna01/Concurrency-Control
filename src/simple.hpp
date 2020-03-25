// Interface for simple locking (Exclusive only) lock managers.

#ifndef _SIMPLE_LOCKING_SCHEDULER_H_
#define _SIMPLE_LOCKING_SCHEDULER_H_

#include <tr1/unordered_map>
#include <deque>
#include <map>
#include <vector>

#include "utils/atomic.h"
#include "utils/static_thread_pool.h"
#include "utils/mutex.h"
#include "utils/condition.h"
#include "utils/txn.h"

#include "common.hpp"

using std::map;
using std::deque;
using std::vector;
using std::tr1::unordered_map;

#define THREAD_COUNT 2

class Txn;

// This interface supports locks being held in exclusive modes.
enum LockMode {
  UNLOCKED = 0,
  EXCLUSIVE = 1,
};

class SimpleLockManager {
 public:
  SimpleLockManager(deque<Txn*>* ready_txns) {
    ready_txns_ = ready_txns;
  }

  ~SimpleLockManager() {
    for (auto it = lock_table_.begin(); it != lock_table_.end(); it++) {
      delete it->second;
    }
  }

  // Attempts to grant a read lock to the specified transaction, enqueueing
  // request in lock table. Returns true if lock is immediately granted, else
  // returns false.
  bool ReadLock(Txn* txn, const Key& key) {
    return WriteLock(txn, key);
  }

  // Attempts to grant a write lock to the specified transaction, enqueueing
  // request in lock table. Returns true if lock is immediately granted, else
  // returns false.
  bool WriteLock(Txn* txn, const Key& key) {
    bool empty = true;
    LockRequest rq(EXCLUSIVE, txn);
    deque<LockRequest> *dq = _getLockQueue(key);

    empty = dq->empty();
    dq->push_back(rq);

    if (!empty) { // Add to wait list, doesn't own lock.
      txn_waits_[txn]++;
    }
    return empty;
  }

  // Releases lock held by 'txn' on 'key', or cancels any pending request for
  // a lock on 'key' by 'txn'
  void Release(Txn* txn, const Key& key) {
    deque<LockRequest> *queue = _getLockQueue(key);
    bool removedOwner = true; // Is the lock removed the lock owner?

    // Delete the txn's exclusive lock.
    for (auto it = queue->begin(); it < queue->end(); it++) {
      if (it->txn_ == txn) { // TODO is it ok to just compare by address?
          queue->erase(it);
          break;
      }
      removedOwner = false;
    }

    if (!queue->empty() && removedOwner) {
      // Give the next transaction the lock
      LockRequest next = queue->front();

      if (--txn_waits_[next.txn_] == 0) {
          ready_txns_->push_back(next.txn_);
          txn_waits_.erase(next.txn_);
      }
    }
  }

  // Sets '*owners' to contain the txn IDs of all txns holding the lock, and
  // returns the current LockMode of the lock: UNLOCKED if it is not currently held
  LockMode Status(const Key& key, vector<Txn*>* owners) {
    deque<LockRequest> *dq = _getLockQueue(key);
    if (dq->empty()) {
      return UNLOCKED;
    } else {
      vector<Txn*> _owners;
      _owners.push_back(dq->front().txn_);
      *owners = _owners;
      return EXCLUSIVE;
    }
  }

 protected:
  struct LockRequest {
    LockRequest(LockMode m, Txn* t) : txn_(t), mode_(m) {}
    Txn* txn_;       // Pointer to txn requesting the lock.
    LockMode mode_;  // Specifies whether this is a read or write lock request.
  };
  unordered_map<Key, deque<LockRequest>*> lock_table_;

  deque<Txn*>* ready_txns_;

  unordered_map<Txn*, int> txn_waits_;

  deque<LockRequest>* _getLockQueue(const Key& key) {
    deque<LockRequest> *dq = lock_table_[key];
    if (!dq) {
      dq = new deque<LockRequest>();
      lock_table_[key] = dq;
    }
    return dq;
  }
};

class SimpleStorage {
 public:
  // If there exists a record for the specified key, sets '*result' equal to
  // the value associated with the key and returns true, else returns false;
  // Note that the third parameter is only used for MVCC, the default vaule is 0.
  bool Read(Key key, Value* result, int txn_unique_id = 0) {
    if (data_.count(key)) {
      *result = data_[key];
      return true;
    } else {
      return false;
    }
  }

  // Inserts the record <key, value>, replacing any previous record with the
  // same key.
  // Note that the third parameter is only used for MVCC, the default vaule is 0.
  void Write(Key key, Value value, int txn_unique_id = 0) {
    data_[key] = value;
    timestamps_[key] = GetTime();
  }

  // Returns the timestamp at which the record with the specified key was last
  // updated (returns 0 if the record has never been updated). This is used for OCC.
  double Timestamp(Key key) {
    if (timestamps_.count(key) == 0) {
      return 0;
    }
    return timestamps_[key];
  }
  
  // Init storage
  void InitStorage() {
    for (int i = 0; i < 1000000;i++) {
      Write(i, 0, 0);
    } 
  }
  
  ~SimpleStorage() {}
   
 private:
 
   // Collection of <key, value> pairs. Use this for single-version storage
   unordered_map<Key, Value> data_;
  
   // Timestamps at which each key was last updated.
   unordered_map<Key, double> timestamps_;
};

class SimpleLockingScheduler {
 public:
  SimpleLockingScheduler() : tp_(THREAD_COUNT), next_unique_id_(1) {
    lm_ = new SimpleLockManager(&ready_txns_);
    storage_ = new SimpleStorage();
    storage_->InitStorage();

    cpu_set_t cpuset;
    pthread_attr_t attr;
    pthread_attr_init(&attr);
    CPU_ZERO(&cpuset);
    CPU_SET(0, &cpuset);
    CPU_SET(1, &cpuset);
    CPU_SET(2, &cpuset);
    CPU_SET(3, &cpuset);
    CPU_SET(4, &cpuset);
    CPU_SET(5, &cpuset);
    CPU_SET(6, &cpuset);
    pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpuset);
    pthread_t scheduler_;
    pthread_create(&scheduler_, &attr, StartScheduler, reinterpret_cast<void*>(this));
  }

  ~SimpleLockingScheduler() {
    delete lm_;
    delete storage_;
  }

  void NewTxnRequest(Txn* txn) {
    // Atomically assign the txn a new number and add it to the incoming txn
    // requests queue.
    mutex_.Lock();
    txn->unique_id_ = next_unique_id_;
    next_unique_id_++;
    txn_requests_.Push(txn);
    mutex_.Unlock();
  }

  Txn* GetTxnResult() {
    Txn* txn;
    while (!txn_results_.Pop(&txn)) {
      // No result yet. Wait a bit before trying again (to reduce contention on
      // atomic queues).
      sleep(0.000001);
    }
    return txn;
  }

 private:
  static void* StartScheduler(void *arg) {
    reinterpret_cast<SimpleLockingScheduler *>(arg)->RunScheduler();
    return NULL;
  }

  void RunScheduler() {
    Txn* txn;
    while (tp_.Active()) {
      // Start processing the next incoming transaction request.
      if (txn_requests_.Pop(&txn)) {
        bool blocked = false;
        // Request read locks.
        for (set<Key>::iterator it = txn->readset_.begin();
            it != txn->readset_.end(); ++it) {
          if (!lm_->ReadLock(txn, *it)) {
            blocked = true;
            // If readset_.size() + writeset_.size() > 1, and blocked, just abort
            if (txn->readset_.size() + txn->writeset_.size() > 1) {
              // Release all locks that already acquired
              for (set<Key>::iterator it_reads = txn->readset_.begin(); true; ++it_reads) {
                lm_->Release(txn, *it_reads);
                if (it_reads == it) {
                  break;
                }
              }
              break;
            }
          }
        }

        if (blocked == false) {
          // Request write locks.
          for (set<Key>::iterator it = txn->writeset_.begin();
              it != txn->writeset_.end(); ++it) {
            if (!lm_->WriteLock(txn, *it)) {
              blocked = true;
              // If readset_.size() + writeset_.size() > 1, and blocked, just abort
              if (txn->readset_.size() + txn->writeset_.size() > 1) {
                // Release all read locks that already acquired
                for (set<Key>::iterator it_reads = txn->readset_.begin(); it_reads != txn->readset_.end(); ++it_reads) {
                  lm_->Release(txn, *it_reads);
                }
                // Release all write locks that already acquired
                for (set<Key>::iterator it_writes = txn->writeset_.begin(); true; ++it_writes) {
                  lm_->Release(txn, *it_writes);
                  if (it_writes == it) {
                    break;
                  }
                }
                break;
              }
            }
          }
        }

        // If all read and write locks were immediately acquired, this txn is
        // ready to be executed. Else, just restart the txn
        if (blocked == false) {
          ready_txns_.push_back(txn);
        } else if (blocked == true && (txn->writeset_.size() + txn->readset_.size() > 1)){
          mutex_.Lock();
          txn->unique_id_ = next_unique_id_;
          next_unique_id_++;
          txn_requests_.Push(txn);
          mutex_.Unlock();
        }
      }

      // Process and commit all transactions that have finished running.
      while (completed_txns_.Pop(&txn)) {
        // Commit/abort txn according to program logic's commit/abort decision.
        if (txn->Status() == COMPLETED_C) {
          ApplyWrites(txn);
          txn->status_ = COMMITTED;
        } else if (txn->Status() == COMPLETED_A) {
          txn->status_ = ABORTED;
        } else {
          // Invalid TxnStatus!
          DIE("Completed Txn has invalid TxnStatus: " << txn->Status());
        }

        // Release read locks.
        for (set<Key>::iterator it = txn->readset_.begin();
            it != txn->readset_.end(); ++it) {
          lm_->Release(txn, *it);
        }
        // Release write locks.
        for (set<Key>::iterator it = txn->writeset_.begin();
            it != txn->writeset_.end(); ++it) {
          lm_->Release(txn, *it);
        }

        // Return result to client.
        txn_results_.Push(txn);
      }

      // Start executing all transactions that have newly acquired all their
      // locks.
      while (ready_txns_.size()) {
        // Get next ready txn from the queue.
        txn = ready_txns_.front();
        ready_txns_.pop_front();

        // Start txn running in its own thread.
        tp_.RunTask(new Method<SimpleLockingScheduler, void, Txn*>(
              this,
              &SimpleLockingScheduler::ExecuteTxn,
              txn));

      }
    }
  }

  void ExecuteTxn(Txn* txn) {
    // Get the start time
    txn->occ_start_time_ = GetTime();

    // Read everything in from readset.
    for (set<Key>::iterator it = txn->readset_.begin();
        it != txn->readset_.end(); ++it) {
      // Save each read result iff record exists in storage.
      Value result;
      if (storage_->Read(*it, &result))
        txn->reads_[*it] = result;
    }

    // Also read everything in from writeset.
    for (set<Key>::iterator it = txn->writeset_.begin();
        it != txn->writeset_.end(); ++it) {
      // Save each read result iff record exists in storage.
      Value result;
      if (storage_->Read(*it, &result))
        txn->reads_[*it] = result;
    }

    // Execute txn's program logic.
    txn->Run();

    // Hand the txn back to the RunScheduler thread.
    completed_txns_.Push(txn);
  }

  void ApplyWrites(Txn* txn) {
    // Write buffered writes out to storage.
    for (map<Key, Value>::iterator it = txn->writes_.begin();
        it != txn->writes_.end(); ++it) {
      storage_->Write(it->first, it->second, txn->unique_id_);
    }
  }

  StaticThreadPool tp_;

  int next_unique_id_;
  Mutex mutex_;

  AtomicQueue<Txn*> txn_requests_;
  deque<Txn*> ready_txns_;
  AtomicQueue<Txn*> completed_txns_;

  // Queue of transaction results (already committed or aborted) to be returned
  // to client.
  AtomicQueue<Txn*> txn_results_;

  // Set of transactions that are currently in the process of parallel
  // validation.
  AtomicSet<Txn*> active_set_;

  // Used it for critical section in parallel occ.
  // Lock Manager used for LOCKING concurrency implementations.
  SimpleLockManager* lm_;

  SimpleStorage* storage_;
};

#endif  // SIMPLE_LOCKING_SCHEDULER_H_

