#include "argument_parser/argument_parser.hpp"
#include "csv.hpp"
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

// Function that creates a vector of zeros centroid
vector<Centroid> initializeCentroids(uint n) {
  vector<Centroid> centroids = {};
  for (uint i = 0; i < n; ++i) {
    centroids.push_back(zeros);
  }
  return centroids;
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

  // Parse arguments
  uint n_centroid, num_iter, min_centroids_n, max_centroids_n;
  string input_file, output_file;
  bool write_file, shared_fs;
  ArgumentParser parser;

  int err = parser.parseArgv(argc, argv, &n_centroid, &verbosity_level,
                             input_file, output_file, &write_file, &shared_fs,
                             &min_centroids_n, &max_centroids_n, &num_iter);

  if (err) {
    return err;
  }


  // Read the process's portion of the dataset from file
  size_t datasetSize = std::filesystem::file_size(input_file);
  size_t processChunk = datasetSize;
  size_t start = 0;
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

  ALL_PROCESSES_LOG(MINIMAL) << omp_get_max_threads() << " threads" << endl;
  LOG(MINIMAL) << "Processing..." << endl;

  // Init vectors of centroids, distances and vectors of centroids
  vector<Centroid> centroids = {};
  vector<double> distances = {};

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
    LOG(FULL_VERBOSE) << "Initial centroids: " << endl;
    centroids.clear();
    for (uint i = 0; i < n_centroid; ++i) {
      centroids.push_back(
          Centroid{get<0>(dataset[i]), get<1>(dataset[i]), 0, {}, {}});
    }
    // Create a vector of centroids to check when to stop the computation
    vector<Centroid> old_centroid = initializeCentroids(n_centroid);

    int i = 0;
    uint count = 0;
    // Loop until the centroids change
    while (!(centroids == old_centroid) && count < num_iter) {
      i += 1;
      count += 1;
      old_centroid = centroids;

      // Vector to store the updated positions of the new centroids
      centroids = initializeCentroids(n_centroid);

      #pragma omp parallel for schedule(static) reduction(addCentroids: centroids)

      // Find the nearest centoid for each point in the dataset
      for (size_t i = 0; i < dataset.size(); ++i) {
        float min_centroid_distance = FLT_MAX;
        int min_index = 0;

        // Loop on the old centroids
        for (size_t j = 0; j < old_centroid.size(); ++j) {
          // Calculate distance from point to centroid
          double dist = sqrt(pow(old_centroid[j].x - get<0>(dataset[i]), 2) +
                             pow(old_centroid[j].y - get<1>(dataset[i]), 2));

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

      // Master normalizes the coordinates of the new centroids, dividing them
      // for the number of points assigned to that centroid If a centroid has
      // not any point associated to it, the centroid is regenerated, choosing a
      // random point belonging to the dataset
      #pragma omp parallel for schedule(dynamic, 1) shared(centroids)
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

    // Print information about the computation for the current number of
    // centroids
    LOG(FULL_VERBOSE) << "Iteration cycles: " << i << endl;
    LOG(FULL_VERBOSE) << "Number of centroids: " << n_centroid << endl;
  }

  auto time2 = high_resolution_clock::now();

  LOG(MINIMAL) << "Number of centroids used: " << centroids.size() << endl;

  chrono::duration<double> t1 = time1 - time0;
  chrono::duration<double> t2 = time2 - time1;
  chrono::duration<double> T = time2 - time0;

  LOG(MINIMAL) << "  Pre-processing time:\t" << t1.count() * 1000 << " ms\t"
                << t1.count() * 100 / T.count() << "%" << endl;
  LOG(MINIMAL) << "      Processing time:\t" << t2.count() * 1000 << " ms\t"
                << t2.count() * 100 / T.count() << "%" << endl;
  LOG(MINIMAL) << "           Total time:\t" << T.count() * 1000 << " ms\t"
                << endl;
  LOG(MINIMAL) << "timens:preprocess:" << (size_t)(t1.count() * 1e9) << endl;
  LOG(MINIMAL) << "timens:process:" << (size_t)(t2.count() * 1e9) << endl;
  LOG(MINIMAL) << "timens:total:" << (size_t)(T.count() * 1e9) << endl;

  return 0;
}
