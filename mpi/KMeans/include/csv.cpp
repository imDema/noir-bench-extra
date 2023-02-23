#include "csv.hpp"

#include <fcntl.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <unistd.h>

#include <chrono>
#include <cstring>
#include <filesystem>

CSVReader::CSVReader(std::string path, size_t start, ssize_t end)
    : reader(io::CSVReader<2>(path)) {
  // find the real start of the chunk
  if (start > 0) {
    std::string line;
    std::ifstream tmp(path);
    tmp.seekg(start);
    std::getline(tmp, line);
    start = tmp.tellg();
  }
  // find the real end of the chunk
  if (end != -1) {
    std::string line;
    std::ifstream tmp(path);
    tmp.seekg(end);
    std::getline(tmp, line);
    ssize_t newEnd = tmp.tellg();
    if (newEnd != -1) {
      end = newEnd;
    }
  } else {
    end = std::filesystem::file_size(path);
  }

  reader.set_header("u", "v");
  endPos = end;

  fd = open(path.c_str(), O_RDONLY);
  mmapped = (char *)mmap(NULL, end, PROT_READ, MAP_PRIVATE, fd, 0);
  reader.set_in(path, mmapped + start, mmapped + end);
}

CSVReader::~CSVReader() {
  munmap(mmapped, endPos);
  close(fd);
}

bool CSVReader::nextRow(row_t &row) {
  // start measuring the time of input reading-parsing. The timer is stopped
  // when dropped thanks to RAII.
  return reader.read_row(row.u, row.v);
}
