#include "timing.hpp"
#include "utils.hpp"
#include <algorithm>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <mpi.h>
#include <omp.h>
#include <unistd.h>
#include <utility>
#include <vector>

const size_t STR_SIZE = 35;

struct Word {
  char word[STR_SIZE];
  size_t count;
};

MPI_Datatype wordDataType;

void initDataTypes() {
  int blocklengths[] = {STR_SIZE, 1};
  MPI_Aint offsets[] = {offsetof(Word, word), offsetof(Word, count)};
  MPI_Datatype types[] = {MPI_CHAR, MPI_UNSIGNED_LONG_LONG};

  MPI_Type_create_struct(2, blocklengths, offsets, types, &wordDataType);
  MPI_Type_commit(&wordDataType);
}

template <typename T> std::vector<T> receiveVector(int source, int tag) {
  MPI_Status status;

  // Probe message
  MPI_Probe(source, tag, MPI_COMM_WORLD, &status);

  // Get source and length of message
  int length;
  MPI_Get_count(&status, wordDataType, &length);

  // Allocate buffer and receive result
  std::vector<T> result(length);
  MPI_Recv(result.data(), length, wordDataType, status.MPI_SOURCE,
           status.MPI_TAG, MPI_COMM_WORLD, &status);
  return result;
}

result_t merge(result_t a, const result_t &b) {
  for (auto [k, v] : b)
    a[k] += v;
  return a;
}

int main(int argc, char **argv) {
  int rank;
  int numProcesses;

  MPI_Init(&argc, &argv);
  MPI_Comm_size(MPI_COMM_WORLD, &numProcesses);
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);

  initDataTypes();

  std::string filePath = "data/gutenberg40.txt";

  std::string method = "mmap";

  for (;;) {
    switch (getopt(argc, argv, "hd:btT:m:")) {
    case 'd':
      filePath = optarg;
      continue;
    case 'm':
      method = optarg;
      continue;
    case 'T': {
      const int numThreads = std::atoi(optarg);
      if (numThreads <= 0) {
        std::cerr << "Invalid value for -T: " << optarg << std::endl;
        std::exit(1);
      }
      omp_set_num_threads(numThreads);
    }
      continue;
    case 'h':
    default:
      std::cerr << "Usage: " << argv[0]
                << " [-h] [-d dataset] [-m method] [-T n]\n";
      std::cerr << " -h          Show this message and exit\n";
      std::cerr << " -d dataset  Use the specified dataset file\n";
      std::cerr << " -T threads  Override OMP_NUM_THREADS\n";
      std::cerr << " -m method   Execution method to use (mmap, iostream, "
                   "iostream_opt, two_steps)\n";
      std::exit(1);
    case -1:
      break;
    }
    break;
  }

  const int numThreads = omp_get_max_threads();

  result_t result;
  if (method == "mmap" || method == "two_steps") {
    result = execute_mmap(rank, numProcesses, numThreads, filePath);
  } else if (method == "iostream") {
    result = execute_iostream(rank, numProcesses, numThreads, filePath);
  } else if (method == "iostream_opt") {
    result = execute_iostream_opt(rank, numProcesses, numThreads, filePath);
  } else {
    std::cerr << "Unsupported method: " << method << "." << std::endl;
    std::cerr << "The supported ones are: mmap, iostream, two_steps."
              << std::endl;
  }

  if (method == "two_steps") {
    // Intermediate step
    // Send intermediate results to other nodes, based on the hash of the word

    // Prepare the buffers to be sent
    std::vector<std::vector<Word>> flatten(numProcesses);
    for (auto [w, c] : result) {
      Word word;
      strncpy(word.word, w.c_str(), STR_SIZE);
      word.word[STR_SIZE - 1] = '\0';
      word.count = c;

      size_t h = std::hash<std::string>{}(w);
      flatten[h % numProcesses].push_back(word);
    }

    // Send the buffers
    std::vector<MPI_Request> requests(numProcesses);
    for (int i = 0; i < numProcesses; i++) {
      if (rank != i) {
        MPI_Isend(flatten[i].data(), flatten[i].size(), wordDataType, i, 1,
                  MPI_COMM_WORLD, &requests[i]);
        fprintf(stderr, "[%2d   ] Process %d has sent %ld records to %d\n",
                rank, rank, flatten[i].size(), i);
      }
    }

    // Receive buffers from others
    std::vector<Word> all = std::move(flatten[rank]);
    for (int i = 0; i < numProcesses; i++) {
      if (rank != i) {
        std::vector<Word> received = receiveVector<Word>(i, 1);
        fprintf(stderr, "[%2d   ] Received intermediate result from %d\n", rank,
                i);
        all.insert(all.begin(), received.begin(), received.end());
      }
    }

    // Merge the results
    result.clear();
    for (Word &w : all) {
      result[w.word] += w.count;
    }

    // Wait on all the ISend, so that the buffers can be safely deallocated
    for (int i = 0; i < numProcesses; i++) {
      if (rank != i) {
        MPI_Wait(&requests[i], MPI_STATUS_IGNORE);
      }
    }
  }

  if (rank != 0) {
    std::vector<Word> flatten;
    for (auto [w, c] : result) {
      Word word;
      strncpy(word.word, w.c_str(), STR_SIZE);
      word.word[STR_SIZE - 1] = '\0';
      word.count = c;

      flatten.push_back(word);
    }
    MPI_Send(flatten.data(), flatten.size(), wordDataType, 0, 2,
             MPI_COMM_WORLD);
    fprintf(stderr, "[%2d   ] Process %d has sent %ld records\n", rank, rank,
            flatten.size());
  } else {
    for (int i = 1; i < numProcesses; i++) {
      std::vector<Word> otherResult = receiveVector<Word>(i, 2);
      for (const auto &w : otherResult) {
        result[w.word] += w.count;
      }
      fprintf(stderr, "[%2d   ] Received fragment %d/%d\n", rank, i,
              numProcesses - 1);
    }
  }

  if (rank == 0) {
#ifndef NDEBUG
    std::vector<std::pair<std::string, size_t>> sorted(result.begin(),
                                                       result.end());
    std::sort(sorted.begin(), sorted.end());
    for (auto &t : sorted) {
      std::cout << t.first << "," << t.second << "\n";
    }
#endif
  }

  MPI_Type_free(&wordDataType);
  MPI_Finalize();
}
