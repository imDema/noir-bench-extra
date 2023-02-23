#include "argument_parser/argument_parser.hpp"
#include "csv.hpp"
#include "mpi.h"
#include <cfloat>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <math.h>
#include <omp.h>
#include <random>
#include <sstream>
#include <string>
#include <tuple>
#include <vector>

struct Centroid {
  double x;
  double y;
  uint n;
  std::vector<double> px;
  std::vector<double> py;
};

#define zeros                                                                  \
  Centroid {                                                                   \
    0, 0, 0, {}, {}                                                            \
  }

using namespace chrono;

VerbosityLevel verbosity_level;
int mpi_rank, mpi_size;

// Return True of the centroid are not significantly different
bool operator==(const Centroid &lhs, const Centroid &rhs) {
  return (abs(lhs.x - rhs.x) < 0.1) && (abs(lhs.y - rhs.y) < 0.1);
}

// Add the coordinates of the centroid omp_in to the coordinates of the
// centroids of the vector omp_out
void addCentroids(vector<Centroid> &omp_out, vector<Centroid> omp_in) {
  for (uint i = 0; i < omp_out.size(); ++i) {
    omp_out[i].x += omp_in[i].x;
    omp_out[i].y += omp_in[i].y;
    omp_out[i].n += omp_in[i].n;
  }
}

// Merge the centroid omp_in to the vector of centroids omp_out
void mergeCentroids(vector<Centroid> &omp_out, vector<Centroid> omp_in) {
  for (uint i = 0; i < min(omp_out.size(), omp_in.size()); ++i) {
    omp_out[i].px.insert(omp_out[i].px.end(),
                         make_move_iterator(omp_in[i].px.begin()),
                         make_move_iterator(omp_in[i].px.end()));
    omp_out[i].py.insert(omp_out[i].py.end(),
                         make_move_iterator(omp_in[i].py.begin()),
                         make_move_iterator(omp_in[i].py.end()));
  }
}

// Calculate the distance used in the elbow method
double pointDistance(double x0, double y0, double x1, double y1, double x2,
                     double y2) {
  return abs((y2 - y1) * x0 - (x2 - x1) * y0 + x2 * y1 - y2 * x1) /
         (pow(y2 - y1, 2) + pow(x2 - x1, 2));
}

// Function that creates a vector of zeros centroid
vector<Centroid> centroids_default(uint n) {
  vector<Centroid> centroids = {};
  for (uint i = 0; i < n; ++i) {
    centroids.push_back(zeros);
  }
  return centroids;
}

// Receive from the other MPI processes the points associated to the given
// centroid
void receivePointsInCluster(Centroid &centroid) {
  for (int k = 1; k < mpi_size; ++k) {
    MPI_Status Stat;
    uint n_points = 0;
    vector<double> px, py;

    // Receiving number of points
    MPI_Recv(&n_points, 1, MPI_UNSIGNED, k, 1, MPI_COMM_WORLD, &Stat);

    // Receiving x coordinates
    px.resize(n_points);
    MPI_Recv(px.data(), n_points, MPI_DOUBLE, k, 1, MPI_COMM_WORLD, &Stat);

    // Receiving y coordinates
    py.resize(n_points);
    MPI_Recv(py.data(), n_points, MPI_DOUBLE, k, 1, MPI_COMM_WORLD, &Stat);

    // Adding the received points to the centroids list
    centroid.px.insert(end(centroid.px), begin(px), end(px));
    centroid.py.insert(end(centroid.py), begin(py), end(py));

    // Clearing vectors
    px.clear();
    py.clear();
  }
}

// Send to the root machine all the points associated to the given centroid
void sendPointsInCluster(Centroid &centroid) {
  uint size = (uint)centroid.px.size();

  // Send number of points
  MPI_Send(&size, 1, MPI_UNSIGNED, 0, 1, MPI_COMM_WORLD);

  // Send x and then y coordinates
  MPI_Send(centroid.px.data(), centroid.px.size(), MPI_DOUBLE, 0, 1,
           MPI_COMM_WORLD);
  MPI_Send(centroid.py.data(), centroid.py.size(), MPI_DOUBLE, 0, 1,
           MPI_COMM_WORLD);
}

#pragma omp declare reduction(addCentroids: vector<Centroid>: addCentroids(omp_out,omp_in))\
initializer(omp_priv = omp_orig)

#pragma omp declare reduction(mergeCentroids: vector<Centroid>: mergeCentroids(omp_out,omp_in))\
initializer(omp_priv = omp_orig)

/*
 * Main function
 */
int main(int argc, char *argv[]) {
  auto time0 = high_resolution_clock::now();
  srand(time(NULL));

  // Initialize the MPI execution environment
  MPI_Init(&argc, &argv);
  // Determine the rank of the calling process in the communicator
  MPI_Comm_rank(MPI_COMM_WORLD, &mpi_rank);
  // Determine the size of the group associated with a communicator
  MPI_Comm_size(MPI_COMM_WORLD, &mpi_size);

  // Parse arguments
  uint n_centroid, num_iter, min_centroids_n, max_centroids_n;
  string input_file, output_file;
  bool write_file, shared_fs;
  ArgumentParser parser;

  int err = parser.parseArgv(argc, argv, &n_centroid, &verbosity_level,
                             input_file, output_file, &write_file, &shared_fs,
                             &min_centroids_n, &max_centroids_n, &num_iter);

  // Check for error in the argument passing
  if (err) {
    // Termine MPI execution environment
    MPI_Finalize();
    return 0;
  }

  // Read the process's portion of the dataset from file
  size_t datasetSize = std::filesystem::file_size(input_file);
  size_t processChunk = (datasetSize + mpi_size - 1) / mpi_size;
  size_t start = processChunk * mpi_rank;
  size_t end = start + processChunk;
  CSVReader csv(input_file, start, end);
  vector<tuple<double, double>> dataset;
  row_t row;
  while (csv.nextRow(row)) {
    dataset.emplace_back(row.u, row.v);
  }

  // FileHandler fhandler;
  // vector<tuple<double, double>> dataset =
  //     fhandler.loadDataset(input_file, mpi_rank, mpi_size);

  // Init the uniform distributions to generate a random point in the dataset
  random_device rd;
  mt19937 gen(rd());
  uniform_real_distribution<> random_index(0, dataset.size() - 1);

  ALL_PROCESSES_LOG(MINIMAL)
      << "[" << mpi_rank << "] " << omp_get_max_threads() << " threads" << endl;
  LOG(MINIMAL) << "Processing..." << endl;

  // Init vectors of centroids, distances and vectors of centroids
  vector<Centroid> centroids = {};

  // Set centroid minimun and maximum number of centroids equal if no elbow
  // method has to be performed
  if (n_centroid > 0) {
    min_centroids_n = n_centroid;
    max_centroids_n = n_centroid;
  }

  auto time1 = high_resolution_clock::now();

  // Loop in the number of centroids
  for (uint h = min_centroids_n; h <= max_centroids_n; ++h) {
    n_centroid = h;

    // Init centroids vector with some random numbers in master
    if (mpi_rank == 0) {
      LOG(FULL_VERBOSE) << "Initial centroids: " << endl;
      centroids.clear();
      for (uint i = 0; i < n_centroid; ++i) {
        centroids.push_back(
            Centroid{get<0>(dataset[i]), get<1>(dataset[i]), 0, {}, {}});
        LOG(FULL_VERBOSE) << "x: " << centroids[i].x << endl;
        LOG(FULL_VERBOSE) << "y: " << centroids[i].y << endl;
        LOG(FULL_VERBOSE) << "n: " << centroids[i].n << endl;
      }
      // Init centroids vector to zeros centroids in non master threads; true
      // values will be sent after
    } else {
      centroids.clear();
      for (uint i = 0; i < n_centroid; ++i) {
        centroids.push_back(zeros);
      }
    }
    // Create a vector of centroids to check when to stop the computation
    vector<Centroid> prev_centroids = centroids_default(n_centroid);

    // Send to all the initial values of the centroids
    for (uint i = 0; i < centroids.size(); ++i) {
      MPI_Bcast(&centroids[i].x, 1, MPI_DOUBLE, 0, MPI_COMM_WORLD);
      MPI_Bcast(&centroids[i].y, 1, MPI_DOUBLE, 0, MPI_COMM_WORLD);
      MPI_Bcast(&centroids[i].n, 1, MPI_INT, 0, MPI_COMM_WORLD);
    }

    int i = 0;
    uint count = 0;
    // Loop until the centroids change
    while (!(centroids == prev_centroids) && count < num_iter) {
      i += 1;
      count += 1;
      prev_centroids = centroids;

      // Vector to store the updated positions of the new centroids
      centroids = centroids_default(n_centroid);

      #pragma omp parallel for schedule(static, 8196) reduction(addCentroids: centroids)
      // Find the nearest centoid for each point in the dataset
      for (size_t i = 0; i < dataset.size(); ++i) {
        float min_centroid_distance = FLT_MAX;
        int min_index = 0;

        // Loop on the old centroids
        for (size_t j = 0; j < prev_centroids.size(); ++j) {
          // Calculate distance from point to centroid
          double dist = sqrt(pow(prev_centroids[j].x - get<0>(dataset[i]), 2) +
                             pow(prev_centroids[j].y - get<1>(dataset[i]), 2));

          // Set nearest centroid and its distance from the point
          if (dist < min_centroid_distance) {
            min_centroid_distance = dist;
            min_index = j;
          }
        }

        // Add coordinates of the point to the cumulative sums of the nearest
        // centroid
        centroids[min_index].x += get<0>(dataset[i]);
        centroids[min_index].y += get<1>(dataset[i]);

        // Increment counter of points associated to the centroid
        centroids[min_index].n++;
      }

      // For each centroid the master receives from the other machines the
      // coordinates and the counter of associated points and sum them up the
      // values with the MPI_Reduce function
      for (uint i = 0; i < centroids.size(); ++i) {
        if (mpi_rank == 0) {
          MPI_Reduce(MPI_IN_PLACE, &centroids[i].x, 1, MPI_DOUBLE, MPI_SUM, 0,
                     MPI_COMM_WORLD);
          MPI_Reduce(MPI_IN_PLACE, &centroids[i].y, 1, MPI_DOUBLE, MPI_SUM, 0,
                     MPI_COMM_WORLD);
          MPI_Reduce(MPI_IN_PLACE, &centroids[i].n, 1, MPI_UNSIGNED, MPI_SUM, 0,
                     MPI_COMM_WORLD);
        } else {
          MPI_Reduce(&centroids[i].x, &centroids[i].x, 1, MPI_DOUBLE, MPI_SUM,
                     0, MPI_COMM_WORLD);
          MPI_Reduce(&centroids[i].y, &centroids[i].y, 1, MPI_DOUBLE, MPI_SUM,
                     0, MPI_COMM_WORLD);
          MPI_Reduce(&centroids[i].n, &centroids[i].n, 1, MPI_UNSIGNED, MPI_SUM,
                     0, MPI_COMM_WORLD);
        }
      }

      // Master normalizes the coordinates of the new centroids, dividing them
      // for the number of points assigned to that centroid If a centroid has
      // not any point associated to it, the centroid is regenerated, choosing a
      // random point belonging to the dataset
      if (mpi_rank == 0) {
        #pragma omp parallel for schedule(guided) shared(centroids)
        for (size_t j = 0; j < centroids.size(); ++j) {
          if (centroids[j].n != 0) {
            centroids[j].x = centroids[j].x / centroids[j].n;
            centroids[j].y = centroids[j].y / centroids[j].n;
          } else {
            int index = random_index(gen);
            centroids[j].x = get<0>(dataset[index]);
            centroids[j].y = get<1>(dataset[index]);
          }
          centroids[j].n = 0;
        }
      }

      // Master sends in broadcast the new centoids to be used in the next
      // iteration of the loop
      for (uint i = 0; i < centroids.size(); ++i) {
        MPI_Bcast(&centroids[i].x, 1, MPI_DOUBLE, 0, MPI_COMM_WORLD);
        MPI_Bcast(&centroids[i].y, 1, MPI_DOUBLE, 0, MPI_COMM_WORLD);
        MPI_Bcast(&centroids[i].n, 1, MPI_UNSIGNED, 0, MPI_COMM_WORLD);
      }
    }

    // Print information about the computation for the current number of
    // centroids
    LOG(FULL_VERBOSE) << "Iteration cycles: " << i << endl;
    LOG(FULL_VERBOSE) << "Number of centroids: " << n_centroid << endl;
  }

  // Log the result into terminal
  if (mpi_rank == 0) {
    LOG(MINIMAL) << "Number of centroids used: " << centroids.size() << endl;

    auto time3 = high_resolution_clock::now();
    chrono::duration<double> T = time3 - time0;

    LOG(MINIMAL) << "           Total time:\t" << T.count() * 1000 << " ms\t"
                 << endl;
    LOG(MINIMAL) << "timens:total:" << (size_t)(T.count() * 1e9) << endl;
  }

  // Termining MPI execution environment
  MPI_Finalize();
  return 0;
}
