
#ifndef _MVCC_SCHEDULER_H_
#define _MVCC_SCHEDULER_H_

#include <limits.h>
#include <tr1/unordered_map>
#include <deque>
#include <map>

#include "common.hpp"
#include "utils/atomic.h"
#include "utils/static_thread_pool.h"
#include "utils/condition.h"
#include "utils/txn.h"
#include "utils/mutex.h"

#include "common.hpp"

using std::tr1::unordered_map;
using std::deque;
using std::map;

#define THREAD_COUNT 2

class Txn;

// MVCC 'version' structure
struct Version {
  Value value_;      // The value of this version
  int max_read_id_;  // Largest timestamp of a transaction that read the version
  int version_id_;   // Timestamp of the transaction that created(wrote) the version
};

// MVCC storage
class MVCCStorage {
 public:
  // If there exists a record for the specified key, sets '*result' equal to
  // the value associated with the key and returns true, else returns false;
  // The third parameter is the txn_unique_id(txn timestamp), which is used for MVCC.
  bool Read(Key key, Value* result, int txn_unique_id = 0) {
    deque<Version *> *dqVersion = mvcc_data_[key];
    
    Version *latestVer = nullptr;
    for (auto it = dqVersion->begin(); it != dqVersion->end(); it++) {
      if (latestVer == nullptr) {
        latestVer = *it;
      } else {
        if (((*it)->version_id_ > latestVer->version_id_) && ((*it)->version_id_ <= txn_unique_id)) {
          latestVer = *it;
        }
      }
    }

    if (latestVer != nullptr) {
      *result = latestVer->value_;
      latestVer->max_read_id_ = txn_unique_id;
      return true;
    }
    
    return false;
  }

  // Inserts a new version with key and value
  // The third parameter is the txn_unique_id(txn timestamp), which is used for MVCC.
  void Write(Key key, Value value, int txn_unique_id = 0) {
    struct Version *newVersion = (struct Version*) malloc(sizeof(struct Version));
    newVersion->value_ = value;
    newVersion->max_read_id_ = txn_unique_id;
    newVersion->version_id_ = txn_unique_id;

    if (mvcc_data_.count(key)) {  // key already exist in mvcc_data_
      deque<Version *> *dqVersion = mvcc_data_[key];
      Version *latestVer = nullptr;
      for (auto it = dqVersion->begin(); it != dqVersion->end(); it++) {
        if (latestVer == nullptr) {
          latestVer = *it;
        } else {
          if (((*it)->version_id_ > latestVer->version_id_) && ((*it)->version_id_ <= txn_unique_id)) {
            latestVer = *it;
          }
        }
      }

      if (txn_unique_id == latestVer->version_id_) {
        // if TS(Ti) == W-timestamp(Qk), overwrite the content
        latestVer->value_ = value;
      } else {
        // else create a new version
        mvcc_data_[key]->push_back(newVersion);
      }
    } else {
      deque<Version *> *newDqVersion = new deque<Version *>();
      newDqVersion->push_back(newVersion);
      
      mvcc_data_[key] = newDqVersion;
    }
  }

  // Returns the timestamp at which the record with the specified key was last
  // updated (returns 0 if the record has never been updated). This is used for OCC.
  double Timestamp(Key key) {
    return 0;
  }
  
  // Init storage
  void InitStorage() {
    for (int i = 0; i < 1000000;i++) {
      Write(i, 0, 0);
      Mutex* key_mutex = new Mutex();
      mutexs_[i] = key_mutex;
    }
  }
  
  // Lock the version_list of key
  void Lock(Key key) {
    mutexs_[key]->Lock();
  }
  
  // Unlock the version_list of key
  void Unlock(Key key) {
    mutexs_[key]->Unlock();
  }
  
  // Check whether apply or abort the write
  bool CheckWrite (Key key, int txn_unique_id) {
    deque<Version *> *dqVersion = mvcc_data_[key];

    if (!dqVersion) {
      return false;
    }
    // Find Qk, the version whose write timestamp is the largest timestamp less than or equal to TS(Ti)
    Version *latestVer = nullptr;
    for (auto it = dqVersion->begin(); it != dqVersion->end(); it++) {
      if (latestVer == nullptr) {
        latestVer = *it;
      } else {
        if (((*it)->version_id_ > latestVer->version_id_) && ((*it)->version_id_ <= txn_unique_id)) {
          latestVer = *it;
        }
      }
    }
    
    if (txn_unique_id < latestVer->max_read_id_) {
      // TS(Ti) < R-timestamp(Qk)
      return false;
    }

    return true;
  }
  
  ~MVCCStorage() {
    for (unordered_map<Key, deque<Version*>*>::iterator it = mvcc_data_.begin();
        it != mvcc_data_.end(); ++it) {
      delete it->second;          
    }
    
    mvcc_data_.clear();
    
    for (unordered_map<Key, Mutex*>::iterator it = mutexs_.begin();
        it != mutexs_.end(); ++it) {
      delete it->second;          
    }
    
    mutexs_.clear();
  }

 private:
 
  friend class MVCCScheduler;
  
  // Storage for MVCC, each key has a linklist of versions
  unordered_map<Key, deque<Version*>*> mvcc_data_;
  
  // Mutexs for each key
  unordered_map<Key, Mutex*> mutexs_;
};

class MVCCScheduler {
 public:
  MVCCScheduler() : tp_(THREAD_COUNT), next_unique_id_(1) {
    storage_ = new MVCCStorage();
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


  ~MVCCScheduler() {
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
    reinterpret_cast<MVCCScheduler *>(arg)->RunScheduler();
    return NULL;
  }

  void RunScheduler() {
    Txn *txn;
    while (tp_.Active()) {
      if (txn_requests_.Pop(&txn)) {
        tp_.RunTask(new Method<MVCCScheduler, void, Txn*>(
                    this,
                    &MVCCScheduler::ExecuteTxn, txn));
      }
    }
  }

  void ExecuteTxn(Txn* txn) {
    // Read all necessary data for this transaction from storage
    for (set<Key>::iterator it = txn->readset_.begin();
        it != txn->readset_.end(); ++it) {
      Value result;
      storage_->Lock(*it);
      if (storage_->Read(*it, &result, txn->unique_id_))
        txn->reads_[*it] = result;
      storage_->Unlock(*it);
    }
    for (set<Key>::iterator it = txn->writeset_.begin();
        it != txn->writeset_.end(); ++it) {
      Value result;
      storage_->Lock(*it);
      if (storage_->Read(*it, &result, txn->unique_id_))
        txn->writes_[*it] = result;
      storage_->Unlock(*it);
    }

    txn->Run();

    for (set<Key>::iterator it = txn->writeset_.begin(); it != txn->writeset_.end(); it++) {
      // if (!lm_->WriteLock(txn, *it)) {
      //   allPassed = false;
      // }
      storage_->Lock(*it);
    }
    
    bool allPassed = true;
    for (set<Key>::iterator it = txn->writeset_.begin(); it != txn->writeset_.end(); it++) {
      if (!storage_->CheckWrite(*it, txn->unique_id_)) {
        allPassed = false;
      }
    }

    if (allPassed) {
      ApplyWrites(txn);
      for (set<Key>::iterator it_writes = txn->writeset_.begin(); it_writes != txn->writeset_.end(); ++it_writes) {
        storage_->Unlock(*it_writes);
      }
      txn->status_ = COMMITTED;
      txn_results_.Push(txn);
    } else {
      for (set<Key>::iterator it_writes = txn->writeset_.begin(); it_writes != txn->writeset_.end(); ++it_writes) {
        storage_->Unlock(*it_writes);
      }
      // cleanup transaction
      txn->reads_.clear();
      txn->writes_.clear();
      txn->status_ = INCOMPLETE;

      // restart txn
      mutex_.Lock();
      txn->unique_id_ = next_unique_id_;
      next_unique_id_++;
      txn_requests_.Push(txn);
      mutex_.Unlock(); 
    }
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

  MVCCStorage* storage_;
};

#endif  // MVCC_SCHEDULER_H_
