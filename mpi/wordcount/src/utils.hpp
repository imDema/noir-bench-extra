#pragma once

#include <string>
#include <unordered_map>

using result_t = std::unordered_map<std::string, size_t>;

result_t merge(result_t a, const result_t &b);
#pragma omp declare reduction(+ : result_t : omp_out = merge(omp_out, omp_in))

result_t execute_mmap(size_t rank, size_t numProcesses, size_t numThreads,
                      std::string datasetPath);
result_t execute_iostream(size_t rank, size_t numProcesses, size_t numThreads,
                          std::string datasetPath);
result_t execute_iostream_opt(size_t rank, size_t numProcesses, size_t numThreads,
                          std::string datasetPath);