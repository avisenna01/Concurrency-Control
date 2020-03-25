#include <stdio.h>
#include "OCC.hpp"

#include "utils/testing.h"
#include "utils/load_gen.h"
using namespace std;
using std::set;

void Benchmark(const vector<LoadGen*>& lg) {
  // Number of transaction requests thZat can be active at any given time.
  int active_txns = 100;
  deque<Txn*> doneTxns;

  // For each experiment, run 3 times and get the average.
  for (uint32 exp = 0; exp < lg.size(); exp++) {
    double throughput[3];
    for (uint32 round = 0; round < 3; round++) {

      int txn_count = 0;

      // Create Scheduler in next mode.
      OCCScheduler* p = new OCCScheduler();

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
  cout << "\t\t\t\t\t\t0.1ms\t\t1ms\t\t10ms";
  cout << endl;

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
  
  cout << "'Low contention' Read only (20 records)\t" << flush;
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

int main(int argc, char **argv) {
  // OCC Performance testing
  PerformanceTesting();

  return 0;
}