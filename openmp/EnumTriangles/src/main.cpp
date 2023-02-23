#include <filesystem>
#include <mpi.h>
#include <unistd.h>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "csv.hpp"
#include "timing.hpp"

inline size_t hash(int u, int v) {
  return (static_cast<size_t>(u) * 0x1f1f1f1f) ^ static_cast<size_t>(v);
}

namespace std {
template <> struct hash<row_t> {
  inline size_t operator()(const row_t &x) const { return ::hash(x.u, x.v); }
};
} // namespace std

std::vector<int> mpiRecvVec(int source, int tag) {
  MPI_Status status;
  MPI_Probe(source, tag, MPI_COMM_WORLD, &status);

  int size;
  MPI_Get_count(&status, MPI_INT, &size);

  std::vector<int> buffer(size);
  MPI_Recv(buffer.data(), size, MPI_INT, source, tag, MPI_COMM_WORLD, &status);
  return buffer;
}

std::vector<std::pair<int, std::vector<int>>> mpiRecvAdj(int source) {
  std::vector<int> buffer = mpiRecvVec(source, 0);
  std::vector<std::pair<int, std::vector<int>>> res;

  size_t i = 0;
  while (i < buffer.size()) {
    int u = buffer[i++];
    int sz = buffer[i++];
    std::vector<int> vs(sz);
    for (int j = 0; j < sz; j++) {
      vs[j] = buffer[i++];
    }
    res.emplace_back(u, std::move(vs));
  }
  return res;
}

std::vector<row_t> mpiRecvEdges(int source) {
  std::vector<int> buffer = mpiRecvVec(source, 1);
  std::vector<row_t> res(buffer.size() / 2);
  for (size_t i = 0; i < res.size(); i++) {
    res[i] = {buffer[2 * i], buffer[2 * i + 1]};
  }
  return res;
}

int main(int argc, char **argv) {
  int rank;
  int numProcesses;

  MPI_Init(&argc, &argv);
  MPI_Comm_size(MPI_COMM_WORLD, &numProcesses);
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);

  std::string datasetPath = "";

  for (;;) {
    switch (getopt(argc, argv, "hd:")) {
    case 'd':
      datasetPath = optarg;
      continue;
    case 'h':
    default:
      std::cerr << "Usage: " << argv[0] << " [-h] -d dataset\n";
      std::cerr << " -h          Show this message and exit\n";
      std::cerr << " -d dataset  Use the specified dataset file\n";
      std::exit(1);
    case -1:
      break;
    }
    break;
  }

  if (datasetPath.empty()) {
    std::cerr << "Please provide the path to the dataset with `-d dataset.csv`"
              << std::endl;
    std::exit(1);
  }

  size_t datasetSize = std::filesystem::file_size(datasetPath);
  size_t processChunk = (datasetSize + numProcesses - 1) / numProcesses;

  Timer total("total");
  // compute the interval of the CSV file for the current process
  size_t start = processChunk * rank;
  size_t end = start + processChunk;
  CSVReader csv(datasetPath, start, end);

  fprintf(stderr, "[%2d] has interval %9ld - %9ld\n", rank, start, end);

  fprintf(stderr, "[%2d] Step 1: reading CSV\n", rank);
  std::unordered_map<int, std::vector<int>> adjCsv;
  row_t row;
  while (csv.nextRow(row)) {
    // make row.u <= row.v
    if (row.u > row.v)
      std::swap(row.u, row.v);

    // build adjacency list of each node, associatively
    adjCsv[row.u].push_back(row.v);
  }

  const size_t id = rank;
  const size_t numWorker = numProcesses;

  // group adj by u
  std::vector<std::vector<std::pair<int, std::vector<int>>>> workerAdj(numWorker);
  for (auto &[u, vs] : adjCsv) {
    workerAdj[u % numWorker].emplace_back(u, vs);
  }

  // adj list of nodes I manage
  std::unordered_map<int, std::vector<int>> myAdj;

  // send adjs to others
  fprintf(stderr, "[%2d] Step 2: send adj list to others\n", rank);
  std::vector<std::pair<MPI_Request, std::vector<int>>> sendRequests;
  for (size_t i = 0; i < numWorker; i++) {
    if (i != id) {
      std::vector<int> buffer;
      for (auto &[u, vs] : workerAdj[i]) {
        buffer.push_back(u);
        buffer.push_back(vs.size());
        buffer.insert(buffer.end(), vs.begin(), vs.end());
      }
      MPI_Request req;
      MPI_Isend(buffer.data(), buffer.size(), MPI_INT, i, 0, MPI_COMM_WORLD,
                &req);
      sendRequests.emplace_back(std::move(req), std::move(buffer));
    } else {
      // do not send to myself, just add to myAdj
      for (auto &[u, vs] : workerAdj[i]) {
        myAdj[u].insert(myAdj[u].end(), vs.begin(), vs.end());
      }
    }
  }
  workerAdj.clear();

  fprintf(stderr, "[%2d] Step 3: recv adj list from others\n", rank);
  for (size_t i = 0; i < numWorker; i++) {
    if (i != id) {
      auto adj = mpiRecvAdj(i);
      for (auto &[u, vs] : adj) {
        myAdj[u].insert(myAdj[u].end(), vs.begin(), vs.end());
      }
    }
  }

  // wait for the sends to finish
  for (auto &[req, buffer] : sendRequests) {
    MPI_Wait(&req, MPI_STATUS_IGNORE);
  }
  sendRequests.clear();

  fprintf(stderr, "[%2d] Step 4: build triangles\n", rank);
  std::vector<std::vector<row_t>> edges(numWorker);
  for (auto &[u, vs] : myAdj) {
    for (size_t i = 0; i < vs.size(); i++) {
      for (size_t j = 0; j < i; j++) {
        int v2 = std::min(vs[i], vs[j]);
        int v3 = std::max(vs[i], vs[j]);
        edges[v2 % numWorker].push_back({v2, v3});
      }
    }
  }

  fprintf(stderr, "[%2d] Step 5: send triangles to others\n", rank);
  for (size_t i = 0; i < numWorker; i++) {
    if (i != id) {
      std::vector<int> buffer;
      buffer.reserve(edges[i].size() * 2);
      for (auto &r : edges[i]) {
        buffer.push_back(r.u);
        buffer.push_back(r.v);
      }
      std::vector<row_t>().swap(edges[i]);
      MPI_Request req;
      MPI_Isend(buffer.data(), buffer.size(), MPI_INT, i, 1, MPI_COMM_WORLD,
                &req);
      sendRequests.emplace_back(std::move(req), std::move(buffer));
    }
  }

  // flatten adj lists
  std::unordered_set<row_t> myEdges;
  myEdges.max_load_factor(0.5);
  for (auto &[u, vs] : myAdj) {
    for (int v : vs) {
      myEdges.insert({u, v});
    }
  }

  int count = 0;

  for (auto e : edges[id]) {
    if (myEdges.count(e)) {
      count++;
    }
  }

  fprintf(stderr, "[%2d] Step 6: recv triangles from others\n", rank);
  for (size_t i = 0; i < numWorker; i++) {
    if (i != id) {
      auto data = mpiRecvEdges(i);
      for (auto e : data) {
        if (myEdges.count(e)) {
          count++;
        }
      }
    }
  }

  fprintf(stderr, "[%2d] Step 7: final counts: %d\n", rank, count);
  if (rank != 0) {
    MPI_Send(&count, 1, MPI_INT, 0, 3, MPI_COMM_WORLD);
  } else {
    for (size_t i = 1; i < numWorker; i++) {
      int c;
      MPI_Recv(&c, 1, MPI_INT, i, 3, MPI_COMM_WORLD, NULL);
      count += c;
    }
    std::cout << "Total " << count << std::endl;
  }

  // wait for the sends to finish
  for (auto &[req, buffer] : sendRequests) {
    MPI_Wait(&req, MPI_STATUS_IGNORE);
    buffer.clear();
  }
  sendRequests.clear();

  MPI_Finalize();
  total.stop();
  Timer::printTimes();
}
