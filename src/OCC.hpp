// Interface for Optimistic Concurrency Control (OCC) lock managers.

#ifndef _OCC_H_
#define _OCC_H_
#define THREAD_COUNT 2

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

class Txn;

class OCCStorage {
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
    timestamps_[key] 
    = 
    GetTime();
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
  
  ~OCCStorage() {}
   
 private:
 
   friend class TxnProcessor;
   
   // Collection of <key, value> pairs. Use this for single-version storage
   unordered_map<Key, Value> data_;
  
   // Timestamps at which each key was last updated.
   unordered_map<Key, double> timestamps_;
};

class OCCScheduler {
 public:
  OCCScheduler() :  tp_(THREAD_COUNT), next_unique_id_(1){
    storage_ = new OCCStorage();

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



  ~OCCScheduler() {
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
    reinterpret_cast<OCCScheduler *>(arg)->RunScheduler();
    return NULL;
  }

  bool OCCValidateTransaction(const Txn &txn) const {
  // Check
  for (auto&& key : txn.readset_) {
    if (txn.occ_start_time_ < storage_->Timestamp(key))
      return false;
  }

  for (auto&& key : txn.writeset_) {
    if (txn.occ_start_time_ < storage_->Timestamp(key))
      return false;
  }

  return true;
}

void ApplyWrites(Txn* txn) {
  // Write buffered writes out to storage.
  for (map<Key, Value>::iterator it = txn->writes_.begin();
       it != txn->writes_.end(); ++it) {
    storage_->Write(it->first, it->second, txn->unique_id_);
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

  void RunScheduler() {

    bool valid;

    // Fetch transaction requests, and immediately begin executing them.
    while (tp_.Active()) {
      Txn *transaction;
      if (txn_requests_.Pop(&transaction)) {

        // Start transaction running in its own thread.
        tp_.RunTask(new Method<OCCScheduler, void, Txn*>(
                    this,
                    &OCCScheduler::ExecuteTxn,
                    transaction));
      }

      // Validate completed transactions
      Txn *completed;
      while (completed_txns_.Pop(&completed)) {
        if (completed->Status() == COMPLETED_A) {
          completed->status_ = ABORTED;
        } else {
          valid = OCCValidateTransaction(*completed);
          if (valid) {
              // Commit the transaction
            ApplyWrites(completed);
            transaction->status_ = COMMITTED;
          } else {
             // Cleanup and restart
            completed->reads_.empty();
            completed->writes_.empty();
            completed->status_ = INCOMPLETE;

            mutex_.Lock();
            transaction->unique_id_ = next_unique_id_;
            next_unique_id_++;
            txn_requests_.Push(completed);
            mutex_.Unlock();
          }
        }

        txn_results_.Push(completed);
      }
    }
  }

  // Queue of completed (but not yet committed/aborted) transactions.
  AtomicQueue<Txn*> completed_txns_;

  // Data storage
  OCCStorage* storage_;

  // Thread pool managing all threads used by TxnProcessor.
  StaticThreadPool tp_;

  // Next valid unique_id, and a mutex to guard incoming transaction requests.
  int next_unique_id_;
  Mutex mutex_;

  // Queue of incoming transaction requests.
  AtomicQueue<Txn*> txn_requests_;

  deque<Txn*> ready_txns_;

  AtomicQueue<Txn*> txn_results_;

  AtomicSet<Txn*> active_set_;

  Mutex active_set_mutex_;


};

#endif  // OCC_H_