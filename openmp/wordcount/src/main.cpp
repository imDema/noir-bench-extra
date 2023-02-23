#include "utils.hpp"
#include <algorithm>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <omp.h>
#include <unistd.h>
#include <utility>
#include <vector>

const size_t STR_SIZE = 35;

struct Word {
  char word[STR_SIZE];
  size_t count;
};

result_t merge(result_t a, const result_t &b) {
  for (auto [k, v] : b)
    a[k] += v;
  return a;
}

int main(int argc, char **argv) {
  int rank = 0;
  int numProcesses = 1;

  std::string filePath = "/gutenberg.txt";

  std::string method = "iostream";

  for (;;) {
    switch (getopt(argc, argv, "hd:btT:")) {
    case 'd':
      filePath = optarg;
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
      std::exit(1);
    case -1:
      break;
    }
    break;
  }

  const int numThreads = omp_get_max_threads();

  result_t result = execute_iostream(rank, numProcesses, numThreads, filePath);

  std::cout << result.size() << "\n";
}
