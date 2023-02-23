#include "utils.hpp"
#include <filesystem>
#include <fstream>
#include <iostream>
#include <regex>

result_t execute(std::string datasetPath, size_t start, size_t end) {
  std::ifstream file(datasetPath);
  file.seekg(start);
  if (start != 0) {
    char c;
    file.get(c);
    while (!file.fail() && c != '\n') {
      file.get(c);
    }
  }

  result_t count;
  if (file.fail() || file.tellg() > (ssize_t)end) return count;
  std::string line;
  std::regex re("[^A-Za-z]+");
  while (std::getline(file, line)) {
    line = std::regex_replace(line, re, " ");
    std::string word;
    for (char c : line) {
      if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')) {
        word += std::tolower(c);
      } else {
        if (!word.empty()) {
          count[word]++;
        }
        word = "";
      }
    }
    if (!word.empty()) {
      count[word]++;
    }
    if (file.tellg() > (ssize_t)end) {
      break;
    }
  }

  return count;
}

result_t execute_iostream(size_t rank, size_t numProcesses, size_t numThreads,
                          std::string datasetPath) {
  const size_t datasetSize = std::filesystem::file_size(datasetPath);
  const size_t processChunk = datasetSize;
  const size_t threadChunk = (processChunk + numThreads - 1) / numThreads;
  result_t result;

#pragma omp declare reduction(+ : result_t : omp_out = merge(omp_out, omp_in))
#pragma omp parallel for schedule(static, 1) reduction(+ : result)
  for (size_t th = 0; th < numThreads; th++) {
    size_t start = processChunk * rank + threadChunk * th;
    size_t end = start + threadChunk;

    fprintf(stderr, "[%2ld/%2ld] has interval %9ld - %9ld\n", rank, th, start,
            end);

    result = execute(datasetPath, start, end);

    fprintf(stderr, "[%2ld/%2ld] has interval %9ld - %9ld -- done\n", rank, th,
            start, end);
  }
  return result;
}