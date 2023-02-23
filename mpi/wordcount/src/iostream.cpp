#include "utils.hpp"
#include <filesystem>
#include <fstream>
#include <iostream>
#include <regex>

result_t execute_re(std::string datasetPath, size_t start, size_t end) {
  std::ifstream file;
  char buf[8 << 10];

  file.rdbuf()->pubsetbuf(buf, sizeof buf);
  file.open(datasetPath);

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
  std::regex re("[a-zA-Z]+");
  while (std::getline(file, line)) {
    auto words_begin = 
        std::sregex_iterator(line.cbegin(), line.cend(), re);
    auto words_end = std::sregex_iterator();

    for (std::sregex_iterator i = words_begin; i != words_end; ++i) {
        std::smatch match = *i;
        std::string word = match.str(); 
        if (!word.empty()) {
          std::transform(word.cbegin(), word.cend(), word.begin(), [](unsigned char c){ return std::tolower(c); });
          count[word]++;
        }
    }
    if (file.tellg() > (ssize_t)end) {
      break;
    }
  }

  return count;
}

result_t execute_ascii(std::string datasetPath, size_t start, size_t end) {
  std::ifstream file;
  char buf[8 << 10];

  file.rdbuf()->pubsetbuf(buf, sizeof buf);
  file.open(datasetPath);

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
  std::string word;
  while (std::getline(file, line)) {
    for (char c : line) {
      if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')) {
        word += std::tolower(c);
      } else if (!word.empty()) {
        count[word]++;
        word = "";
      }
    }
    if (!word.empty()) {
      count[word]++;
      word = "";
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
  const size_t processChunk = (datasetSize + numProcesses - 1) / numProcesses;
  const size_t threadChunk = (processChunk + numThreads - 1) / numThreads;
  result_t result;

#pragma omp parallel for schedule(static, 1) reduction(+ : result)
  for (size_t th = 0; th < numThreads; th++) {
    size_t start = processChunk * rank + threadChunk * th;
    size_t end = start + threadChunk;

    fprintf(stderr, "[%2ld/%2ld][iostream_re] has interval %9ld - %9ld\n", rank, th, start,
            end);

    result = execute_re(datasetPath, start, end);

    fprintf(stderr, "[%2ld/%2ld][iostream_re] has interval %9ld - %9ld -- done\n", rank, th,
            start, end);
  }
  return result;
}

result_t execute_iostream_opt(size_t rank, size_t numProcesses, size_t numThreads,
                          std::string datasetPath) {
  const size_t datasetSize = std::filesystem::file_size(datasetPath);
  const size_t processChunk = (datasetSize + numProcesses - 1) / numProcesses;
  const size_t threadChunk = (processChunk + numThreads - 1) / numThreads;
  result_t result;

#pragma omp parallel for schedule(static, 1) reduction(+ : result)
  for (size_t th = 0; th < numThreads; th++) {
    size_t start = processChunk * rank + threadChunk * th;
    size_t end = start + threadChunk;

    fprintf(stderr, "[%2ld/%2ld][iostream_opt] has interval %9ld - %9ld\n", rank, th, start,
            end);

    result = execute_ascii(datasetPath, start, end);

    fprintf(stderr, "[%2ld/%2ld][iostream_opt] has interval %9ld - %9ld -- done (%ld)\n", rank, th,
            start, end, result.size());
  }
  return result;
}