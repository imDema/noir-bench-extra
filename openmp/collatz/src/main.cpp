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

typedef struct {
  size_t n, count;
} result_t;

void merge(result_t& omp_out, const result_t& omp_in) {
  if (omp_in.count > omp_out.count) {
    omp_out.n = omp_in.n;
    omp_out.count = omp_in.count;
  }
}
#pragma omp declare reduction(merge : result_t : merge(omp_out, omp_in)) initializer(omp_priv = omp_orig)

int main(int argc, char **argv) {
  size_t n = -1;
  size_t iter = 1000;

  std::string filePath = "/gutenberg.txt";

  std::string method = "iostream";

  for (;;) {
    switch (getopt(argc, argv, "hn:")) {
    case 'n':
      n = std::atoi(optarg);
      continue;
    case 'h':
    default:
      std::cerr << "Usage: " << argv[0]
                << " [-h] [-n NUMBER]\n";
      std::exit(1);
    case -1:
      break;
    }
    break;
  }
  if (n <= 0) {
    std::cerr << "Invalid value for -n: " << optarg << std::endl;
    std::exit(1);
  }

  result_t result {0, 0};

  #pragma omp parallel for schedule(guided) reduction(merge:result)
  for (size_t i = 0; i < n; ++i) {
    size_t c = 0;
    size_t cur = i;
    while (c < iter) {
        if (cur % 2 == 0) {
            cur /= 2;
        } else {
            cur = cur * 3 + 1;
        }
        c += 1;
        if (cur <= 1) {
            break;
        }
    }
    if (c > result.count) {
      result.n = i;
      result.count = c;
    }
  }

  std::cout << result.n << " " << result.count << "\n";
}
