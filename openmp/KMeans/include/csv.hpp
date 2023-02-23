#pragma once

// Cannot use the AsynchronousReader since it makes io::LineReader non copyable,
// and therefore I cannot use set_in to force reading only a fraction of the
// dataset.
#define CSV_IO_NO_THREAD 1

#include <array>
#include <atomic>
#include <fstream>
#include <map>
#include <string>
#include <vector>

#include "third_party/fast-cpp-csv-parser/csv.h"

// Type for a row of the CSV. The row is a mapping from the column name to
// the corresponding value.
struct row_t {
  double u, v;

  bool operator==(const row_t &o) const { return u == o.u && v == o.v; }
};

// Forward declaration of the CSVReader iterator
class Iterator;

// This class reads and parse a CSV file. It supports reading only a piece of
// it, using fast seeking (seekg).
//
// When an interval of the CSV file is selected, only that part of the file is
// read (+the header). Since the CSV file is row based there is a little
// processing of the file positions, aligning them to the lines:
//
//   ↓ start                  ↓ end
// -------\n--------------\n-------------\n-----------
//          ↑ actual start               ↑ actual end
//
// This way the file can be easily split into chunks of roughly the same number
// of rows assuming the row-length is pretty constant. If the length of the file
// is L, for n chunks you can use:
// - [ 0,    L/n]
// - [ L/n, 2L/n]
// - [2L/n, 3L/n]
// - ...
class CSVReader {
private:
  // actual position of the end of the chunk
  ssize_t endPos;
  // whether the csv has ended
  bool ended = false;
  // mmap-ed file
  char *mmapped;
  // file descriptor
  int fd;

  io::CSVReader<2> reader;

public:
  CSVReader(std::string path, size_t start = 0, ssize_t end = -1);
  ~CSVReader();

  // Read, consume and parse the next line of the CSV file, updating the
  // output-parameter result. This function uses an output-parameter avoiding
  // the reallocation of the map.
  // If the file is ended (or the chunk is ended), nothing is changed.
  bool nextRow(row_t &result);
};
