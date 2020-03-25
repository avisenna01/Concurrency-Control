#include <stdio.h>

#include "MVCC.hpp"
#include "common.hpp"
#include "utils/load_gen.h"

using namespace std;

#define DATA_COUNT 3

unordered_map<int, deque<Version*>*> mvcc_data_;

bool Read(Key key, Value *result, int txn_unique_id) {
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

// Check whether apply or abort the write
bool CheckWrite(Key key, int txn_unique_id) {
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

void Write(Key key, Value value, int txn_unique_id) {
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

// Print all versions of spesific data.
void print_deque(Key key) {
  deque<Version*>* deque_ = mvcc_data_[key];

	int version = 0;
	std::cout << "Data with Key: " << key << std::endl;
	for (deque<Version*>::iterator it = deque_->begin(); it != deque_->end(); ++it) {
		std::cout << "	Version: " << version+1 << std::endl;
		std::cout << "	Value: " <<  (*it)->value_ << std::endl;
		std::cout << "	R-timestamp: " << (*it)->max_read_id_ << std::endl;
		std::cout << "	W-timestamp: " << (*it)->version_id_ << std::endl << std::endl;
		version++;
	}
}

void AlgorithmTesting() {
  // Insert transactions to key.
	for (int i = 1; i <= DATA_COUNT; i++) {
		Write(i, 0, 0);
	}

  std::cout << "Please input your test case:" << std::endl << std::endl;

  for (int i=0; i<13; i++) {
    char type;
		Key key = 0;
		Value value = 0;
		int ts_txn = 0;

		std::cin >> type;
		std::cin >> key;
		std::cin >> value;
		std::cin >> ts_txn;
		if (type == 'r') {
			bool check = Read(key, &value, ts_txn);	
			std::cout << "VALUE READ: " <<  value << std::endl << std::endl;
			if (check) {
				print_deque(key);
			}
		} else if (type == 'w') {
			if (CheckWrite(key, ts_txn)) {
				Write(key, value, ts_txn);
				std::cout << "VALUE WRITTEN: " << value << std::endl << std::endl;
				print_deque(key);
			} else {
				std::cout << "Transaction " << ts_txn << " is ROLLED BACK" << std::endl;
				std::cout << "Exiting.." << std::endl;
			}
		}
  }
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
      MVCCScheduler* p = new MVCCScheduler();

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


int main(int argc, char **argv) {
  if (argc < 2) {
    cout << "Usage: ./MVCC <mode>" << endl;
    cout << "<mode> = {algo, performance}" << endl;
    return (1);
  }

  std::string mode = argv[1];

  if (mode.compare("algo") == 0) {
    // Algorithm testing
    /* Multiversion Timestamp Ordering Concurrency Control */
    AlgorithmTesting();
  } else if (mode.compare("performance") == 0) {
    // Testing Performance
    /* Multiversion Timestamp Ordering Concurrency Control */
    PerformanceTesting();
  } else {
    cout << "mode not available" << endl;
    cout << "Usage: ./MVCC <mode>" << endl;
    cout << "<mode> = {algo, performance}" << endl;
  }

  return 0;
}