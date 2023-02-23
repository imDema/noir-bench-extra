#include <mpi.h>
#include <unistd.h>
#include <unordered_map>
#include <unordered_set>
#include <iterator>
#include <functional>
#include <filesystem>

#include "csv.hpp"

typedef std::unordered_map<int, std::vector<int>> adjlist_t;
typedef std::pair<int, int> edge_t;
typedef std::pair<MPI_Request, std::vector<int>> send_request_t;

inline size_t hash(int u, int v) {
  return (static_cast<size_t>(u) * 0x1f1f1f1f) ^ static_cast<size_t>(v);
}

namespace std {
template <> struct hash<edge_t> {
  inline size_t operator()(const edge_t &x) const { return ::hash(x.first, x.second); }
};
} // namespace std

std::vector<int> mpiRecvVec(int source, int tag)
{
  MPI_Status status;
  MPI_Probe(source, tag, MPI_COMM_WORLD, &status);

  int size;
  MPI_Get_count(&status, MPI_INT, &size);

  std::vector<int> buffer(size);
  MPI_Recv(buffer.data(), size, MPI_INT, source, tag, MPI_COMM_WORLD, &status);
  return buffer;
}

template <typename Iterator>
send_request_t mpiSendEdges(int dest, int tag, Iterator begin, Iterator end)
{
  size_t len = std::distance(begin, end);

  std::vector<int> buffer;
  buffer.reserve(len * 2);
  for (Iterator r = begin; r != end; ++r)
  {
    buffer.push_back(r->first);
    buffer.push_back(r->second);
  }
  // std::vector<edge_t>().swap(edgesEnter[i]);
  MPI_Request req;
  MPI_Isend(buffer.data(), buffer.size(), MPI_INT, dest, tag, MPI_COMM_WORLD,
            &req);
  return std::pair(std::move(req), std::move(buffer));
}

std::vector<edge_t> mpiRecvEdges(int source, int tag)
{
  std::vector<int> buffer = mpiRecvVec(source, tag);
  std::vector<edge_t> res(buffer.size() / 2);
  for (size_t i = 0; i < res.size(); i++)
  {
    res[i] = std::pair(buffer[2 * i], buffer[2 * i + 1]);
  }
  return res;
}

template <typename IntoIterator>
void mpiExchangeEdges(int id, int peers, int tag, std::vector<IntoIterator> edges, std::function<void(edge_t)> processEach) {
  std::vector<send_request_t> requests;
  for (int i = 0; i < peers; ++i) {
    if (i != id) {
      requests.emplace_back(mpiSendEdges(i, tag, edges[i].begin(), edges[i].end()));
    }
  }
  
  for (int i = 0; i < peers; ++i) {
    if (i != id) {
      auto recv = mpiRecvEdges(i, tag);
      for (auto e : recv) {
        processEach(e);
      }
    } else {
      for (auto e : edges[id]) {
        processEach(e);
      }
    }
  }

  // wait for the sends to finish
  for (auto &[req, buffer] : requests)
  {
    MPI_Wait(&req, MPI_STATUS_IGNORE);
  }
  requests.clear();
}

int main(int argc, char **argv)
{
  int rank;
  int numProcesses;

  MPI_Init(&argc, &argv);
  MPI_Comm_size(MPI_COMM_WORLD, &numProcesses);
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);

  std::string input_file = "";
  int numIterations = -1;

  for (;;)
  {
    switch (getopt(argc, argv, "hi:n:d:"))
    {
    case 'i':
      numIterations = std::atoi(optarg);
      continue;
    case 'd':
      input_file = optarg;
      continue;
    case 'h':
    default:
      std::cerr << "Usage: " << argv[0]
                << " [-h] -i iterations -d dataset\n";
      std::cerr << " -h            Show this message and exit\n";
      std::cerr << " -i iterations Number of iterations\n";
      std::cerr << " -d dataset    Use the specified dataset file\n";
      std::exit(1);
    case -1:
      break;
    }
    break;
  }

  if (input_file.empty())
  {
    std::cerr << "'-d dataset' is required" << std::endl;
    std::exit(1);
  }
  if (numIterations == -1)
  {
    std::cerr << "'-i iterations' is required" << std::endl;
    std::exit(1);
  }

  const size_t peers = numProcesses;
  const size_t id = rank;

  // Read the process's portion of the dataset from file
  size_t datasetSize = std::filesystem::file_size(input_file);
  size_t processChunk = (datasetSize + peers - 1) / peers;
  size_t start = processChunk * id;
  size_t end = start + processChunk;
  CSVReader csv(input_file, start, end);

  std::vector<std::vector<edge_t>> edges(peers);
  row_t row;
  while (csv.nextRow(row))
  {
    if (row.u != row.v) {
      edges[row.u % peers].emplace_back(row.u, row.v);
    }
  }

  std::unordered_map<int, std::unordered_set<int>> myAdj;
  mpiExchangeEdges(id, peers, 0, edges, [&myAdj](edge_t e) {
    myAdj[e.first].emplace(e.second);
  });
  edges.clear();

  // Save list of entering nodes
  std::vector<std::vector<edge_t>> edgesEnter(peers);
  for (auto &[u, vs] : myAdj)
  {
    for (auto &v : vs)
    {
      edgesEnter[v % peers].emplace_back(u, v);
    }
  }
  std::vector<edge_t> edgesEnterLocal;
  mpiExchangeEdges(id, peers, 1, edgesEnter, [&edgesEnterLocal](edge_t e) {
    edgesEnterLocal.emplace_back(e);
  });
  edgesEnter.clear();

  size_t n_changed = 1;
  for (int iter = 0; iter < numIterations && n_changed != 0; iter++)
  {
    n_changed = 0;
    std::vector<std::unordered_set<edge_t>> newEdges(peers);
    for (auto& e : edgesEnterLocal) {
      for (auto& v : myAdj[e.second]) {
        if (e.first != v) {
          newEdges[e.first % peers].emplace(e.first, v);
        }
      }
    }

    size_t n_changed_local = 0;
    mpiExchangeEdges(id, peers, 2, newEdges, [&myAdj, &n_changed_local](edge_t e) {
      if (myAdj[e.first].emplace(e.second).second)
      {
        n_changed_local += 1;
      }
    });
    newEdges.clear();

    MPI_Allreduce(&n_changed_local, &n_changed, 1, MPI_LONG, MPI_SUM, MPI_COMM_WORLD);
    if (id == 0) fprintf(stderr, "iter: %d, changed: %ld\n", iter, n_changed);
  }

  std::vector<std::vector<edge_t>> gatherEdges(peers);
  for (auto& [u, vs] : myAdj) {
    for (auto& v : vs) {
      gatherEdges[0].emplace_back(u, v);
    }
  }

  std::vector<edge_t> finalGraph;
  mpiExchangeEdges(id, peers, 3, gatherEdges, [&finalGraph](edge_t e) {
    finalGraph.emplace_back(e);
  });

  if (id == 0)fprintf(stderr, "count: %ld\n", finalGraph.size());

  MPI_Finalize();
}
