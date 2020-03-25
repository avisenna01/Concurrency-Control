#include <stdio.h>
#include "simple.hpp"

#include "utils/testing.h"
#include "utils/load_gen.h"

using std::set;

TEST(AlgorithmTesting1) {
  deque<Txn*> ready_txns;
  SimpleLockManager lm(&ready_txns);
  vector<Txn*> owners;

  Txn* t1 = reinterpret_cast<Txn*>(1);
  Txn* t2 = reinterpret_cast<Txn*>(2);
  Txn* t3 = reinterpret_cast<Txn*>(3);

  // Txn 1 acquires read lock.
  lm.ReadLock(t1, 101);
  ready_txns.push_back(t1);   // Txn 1 is ready.
  EXPECT_EQ(EXCLUSIVE, lm.Status(101, &owners));
  EXPECT_EQ(1, owners.size());
  EXPECT_EQ(t1, owners[0]);
  EXPECT_EQ(1, ready_txns.size());
  EXPECT_EQ(t1, ready_txns.at(0));

  // Txn 2 requests write lock. Not granted.
  lm.WriteLock(t2, 101);
  EXPECT_EQ(EXCLUSIVE, lm.Status(101, &owners));
  EXPECT_EQ(1, owners.size());
  EXPECT_EQ(t1, owners[0]);
  EXPECT_EQ(1, ready_txns.size());

  // Txn 3 requests read lock. Not granted.
  lm.ReadLock(t3, 101);
  EXPECT_EQ(EXCLUSIVE, lm.Status(101, &owners));
  EXPECT_EQ(1, owners.size());
  EXPECT_EQ(t1, owners[0]);
  EXPECT_EQ(1, ready_txns.size());

  // Txn 1 releases lock.  Txn 2 is granted write lock.
  lm.Release(t1, 101);
  EXPECT_EQ(EXCLUSIVE, lm.Status(101, &owners));
  EXPECT_EQ(1, owners.size());
  EXPECT_EQ(t2, owners[0]);
  EXPECT_EQ(2, ready_txns.size());
  EXPECT_EQ(t2, ready_txns.at(1));

  // Txn 2 releases lock.  Txn 3 is granted read lock.
  lm.Release(t2, 101);
  EXPECT_EQ(EXCLUSIVE, lm.Status(101, &owners));
  EXPECT_EQ(1, owners.size());
  EXPECT_EQ(t3, owners[0]);
  EXPECT_EQ(3, ready_txns.size());
  EXPECT_EQ(t3, ready_txns.at(2));

  END;
}

TEST(AlgorithmTesting2) {
  deque<Txn*> ready_txns;
  SimpleLockManager lm(&ready_txns);
  vector<Txn*> owners;

  Txn* t1 = reinterpret_cast<Txn*>(1);
  Txn* t2 = reinterpret_cast<Txn*>(2);
  Txn* t3 = reinterpret_cast<Txn*>(3);
  Txn* t4 = reinterpret_cast<Txn*>(4);

  lm.ReadLock(t1, 101);   // Txn 1 acquires read lock.
  ready_txns.push_back(t1);  // Txn 1 is ready.
  lm.WriteLock(t2, 101);  // Txn 2 requests write lock. Not granted.
  lm.ReadLock(t3, 101);   // Txn 3 requests read lock. Not granted.
  lm.ReadLock(t4, 101);   // Txn 4 requests read lock. Not granted.

  lm.Release(t2, 101);    // Txn 2 cancels write lock request.

  // Txn 1 should now have a read lock and Txns 3 and 4 should be next in line.
  EXPECT_EQ(EXCLUSIVE, lm.Status(101, &owners));
  EXPECT_EQ(1, owners.size());
  EXPECT_EQ(t1, owners[0]);

  // Txn 1 releases lock.  Txn 3 is granted read lock.
  lm.Release(t1, 101);
  EXPECT_EQ(EXCLUSIVE, lm.Status(101, &owners));
  EXPECT_EQ(1, owners.size());
  EXPECT_EQ(t3, owners[0]);
  EXPECT_EQ(2, ready_txns.size());
  EXPECT_EQ(t3, ready_txns.at(1));

  // Txn 3 releases lock.  Txn 4 is granted read lock.
  lm.Release(t3, 101);
  EXPECT_EQ(EXCLUSIVE, lm.Status(101, &owners));
  EXPECT_EQ(1, owners.size());
  EXPECT_EQ(t4, owners[0]);
  EXPECT_EQ(3, ready_txns.size());
  EXPECT_EQ(t4, ready_txns.at(2));

  END;
}

void Benchmark(const vector<LoadGen*>& lg) {
  // Number of transaction requests that can be active at any given time.
  int active_txns = 100;
  deque<Txn*> doneTxns;

  // For each experiment, run 3 times and get the average.
  for (uint32 exp = 0; exp < lg.size(); exp++) {
    double throughput[3];
    for (uint32 round = 0; round < 3; round++) {

      int txn_count = 0;

      // Create Scheduler in next mode.
      SimpleLockingScheduler* p = new SimpleLockingScheduler();

      // Record start time.
      double start = GetTime();

      // Start specified number of txns running.
      for (int i = 0; i < active_txns; i++)
        p->NewTxnRequest(lg[exp]->NewTxn());

      // Keep 100 active txns at all times for the first full second.
      while (GetTime() < start + 1) {
        Txn* txn = p->GetTxnResult();
        doneTxns.push_back(txn);
        txn_count++;
        p->NewTxnRequest(lg[exp]->NewTxn());
      }

      // Wait for all of them to finish.
      for (int i = 0; i < active_txns; i++) {
        Txn* txn = p->GetTxnResult();
        doneTxns.push_back(txn);
        txn_count++;
      }

      // Record end time.
      double end = GetTime();
    
      throughput[round] = txn_count / (end-start);

      doneTxns.clear();
      delete p;
    }
    
    // Print throughput
    cout << "\t" << (throughput[0] + throughput[1] + throughput[2]) / 3 << "\t" << flush;
  }

  cout << endl;
}

void PerformanceTesting() {
  cout << "\t\t\t\t\t\tAverage\t\tTransaction\tDuration" << endl;
  cout << "\t\t\t\t\t\t0.1ms\t\t1ms\t\t10ms" << endl << flush;

  cpu_set_t cs;
  CPU_ZERO(&cs);
  CPU_SET(2, &cs);
  int ret = sched_setaffinity(0, sizeof(cs), &cs);
  if (ret) {
    perror("sched_setaffinity");
    assert(false);
  }

  vector<LoadGen*> lg;

  cout << "'Low contention' Read only (5 records)\t" << flush;
  lg.push_back(new RMWLoadGen(1000000, 5, 0, 0.0001));
  lg.push_back(new RMWLoadGen(1000000, 5, 0, 0.001));
  lg.push_back(new RMWLoadGen(1000000, 5, 0, 0.01));

  Benchmark(lg);

  for (uint32 i = 0; i < lg.size(); i++)
    delete lg[i];
  lg.clear();
  
  cout << "'Low contention' Read only (20 records) " << flush;
  lg.push_back(new RMWLoadGen(1000000, 20, 0, 0.0001));
  lg.push_back(new RMWLoadGen(1000000, 20, 0, 0.001));
  lg.push_back(new RMWLoadGen(1000000, 20, 0, 0.01));

  Benchmark(lg);

  for (uint32 i = 0; i < lg.size(); i++)
    delete lg[i];
  lg.clear();
  
  cout << "'High contention' Read only (5 records)\t" << flush;
  lg.push_back(new RMWLoadGen(100, 5, 0, 0.0001));
  lg.push_back(new RMWLoadGen(100, 5, 0, 0.001));
  lg.push_back(new RMWLoadGen(100, 5, 0, 0.01));

  Benchmark(lg);

  for (uint32 i = 0; i < lg.size(); i++)
    delete lg[i];
  lg.clear();

  cout << "'High contention' Read only (20 records)" << flush;
  lg.push_back(new RMWLoadGen(100, 20, 0, 0.0001));
  lg.push_back(new RMWLoadGen(100, 20, 0, 0.001));
  lg.push_back(new RMWLoadGen(100, 20, 0, 0.01));

  Benchmark(lg);

  for (uint32 i = 0; i < lg.size(); i++)
    delete lg[i];
  lg.clear();
  
  cout << "Low contention read-write (5 records)\t" << flush;
  lg.push_back(new RMWLoadGen(1000000, 0, 5, 0.0001));
  lg.push_back(new RMWLoadGen(1000000, 0, 5, 0.001));
  lg.push_back(new RMWLoadGen(1000000, 0, 5, 0.01));

  Benchmark(lg);

  for (uint32 i = 0; i < lg.size(); i++)
    delete lg[i];
  lg.clear();
  
  cout << "Low contention read-write (10 records)\t" << flush;
  lg.push_back(new RMWLoadGen(1000000, 0, 10, 0.0001));
  lg.push_back(new RMWLoadGen(1000000, 0, 10, 0.001));
  lg.push_back(new RMWLoadGen(1000000, 0, 10, 0.01));

  Benchmark(lg);

  for (uint32 i = 0; i < lg.size(); i++)
    delete lg[i];
  lg.clear();
}

int main() {
  // Algorithm testing
  /* Simple Locking (exclusive locks) */
  AlgorithmTesting1();
  AlgorithmTesting2();

  // Performance testing
  /* Simple Locking (exclusive locks) */
  PerformanceTesting();

  return 0;
}