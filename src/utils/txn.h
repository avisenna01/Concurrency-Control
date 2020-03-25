#ifndef _TXN_H_
#define _TXN_H_

#include <map>
#include <set>
#include <vector>

#include "../common.hpp"

using std::map;
using std::set;
using std::vector;

// Txns can have five distinct status values:
enum TxnStatus {
  INCOMPLETE = 0,   // Not yet executed
  COMPLETED_C = 1,  // Executed (with commit vote)
  COMPLETED_A = 2,  // Executed (with abort vote)
  COMMITTED = 3,    // Committed
  ABORTED = 4,      // Aborted
};

class Txn {
 public:
  // Commit vote defauls to false. Only by calling "commit"
  Txn() : status_(INCOMPLETE) {}
  virtual ~Txn() {}
  virtual Txn * clone() const = 0;    // Virtual constructor (copying)

  // Method containing all the transaction's method logic.
  virtual void Run() = 0;

  // Returns the Txn's current execution status.
  TxnStatus Status() { return status_; }

  // Checks for overlap in read and write sets. If any key appears in both,
  // an error occurs.
  void CheckReadWriteSets() {
    for (set<Key>::iterator it = writeset_.begin();
        it != writeset_.end(); ++it) {
      if (readset_.count(*it) > 0) {
        DIE("Overlapping read/write sets\n.");
      }
    }
  }

 protected:
  // Copies the internals of this txn into a given transaction (i.e.
  // the readset, writeset, and so forth).  Be sure to modify this method
  // to copy any new data structures you create.
  void CopyTxnInternals(Txn* txn) const {
    txn->readset_ = set<Key>(this->readset_);
    txn->writeset_ = set<Key>(this->writeset_);
    txn->reads_ = map<Key, Value>(this->reads_);
    txn->writes_ = map<Key, Value>(this->writes_);
    txn->status_ = this->status_;
    txn->unique_id_ = this->unique_id_;
    txn->occ_start_time_ = this->occ_start_time_;
  }

  friend class SimpleLockingScheduler;
  friend class OCCScheduler;
  friend class MVCCScheduler;

  // Method to be used inside 'Execute()' function when reading records from
  // the database. If record corresponding with specified 'key' exists, sets
  // '*value' equal to the record value and returns true, else returns false.
  //
  // Requires: key appears in readset or writeset
  //
  // Note: Can ONLY be called from inside the 'Execute()' function.
  bool Read(const Key& key, Value* value) {
    // Check that key is in readset/writeset.
    if (readset_.count(key) == 0 && writeset_.count(key) == 0)
      DIE("Invalid read (key not in readset or writeset).");

    // Reads have no effect if we have already aborted or committed.
    if (status_ != INCOMPLETE)
      return false;

    // 'reads_' has already been populated by TxnProcessor, so it should contain
    // the target value iff the record appears in the database.
    if (reads_.count(key)) {
      *value = reads_[key];
      return true;
    } else {
      return false;
    }
  }

  // Method to be used inside 'Execute()' function when writing records to
  // the database.
  //
  // Requires: key appears in writeset
  //
  // Note: Can ONLY be called from inside the 'Execute()' function.
  void Write(const Key& key, const Value& value) {
    // Check that key is in writeset.
    if (writeset_.count(key) == 0)
      DIE("Invalid write to key " << key << " (writeset).");

    // Writes have no effect if we have already aborted or committed.
    if (status_ != INCOMPLETE)
      return;

    // Set key-value pair in write buffer.
    writes_[key] = value;

    // Also set key-value pair in read results in case txn logic requires the
    // record to be re-read.
    reads_[key] = value;
  }

  // Macro to be used inside 'Execute()' function when deciding to COMMIT.
  //
  // Note: Can ONLY be called from inside the 'Execute()' function.
  #define COMMIT \
    do { \
      status_ = COMPLETED_C; \
      return; \
    } while (0)

  // Macro to be used inside 'Execute()' function when deciding to ABORT.
  //
  // Note: Can ONLY be called from inside the 'Execute()' function.
  #define ABORT \
    do { \
      status_ = COMPLETED_A; \
      return; \
    } while (0)

  // Set of all keys that may need to be read in order to execute the
  // transaction.
  set<Key> readset_;

  // Set of all keys that may be updated when executing the transaction.
  set<Key> writeset_;

  // Results of reads performed by the transaction.
  map<Key, Value> reads_;

  // Key, Value pairs WRITTEN by the transaction.
  map<Key, Value> writes_;

  // Transaction's current execution status.
  TxnStatus status_;

  // Unique, monotonically increasing transaction ID, assigned by TxnProcessor.
  uint64 unique_id_;

  // Start time (used for OCC).
  double occ_start_time_;
};

#endif  // _TXN_H_

