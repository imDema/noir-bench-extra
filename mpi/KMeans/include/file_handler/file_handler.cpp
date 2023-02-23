#include "file_handler.hpp"
#include "../verbosity_level.hpp"
#include <cfloat>
#include <iostream>
#include <sstream>
#include <tuple>

using namespace std;

/**
 * Read and throw n lines from fstream, with n equal to num_lines.
 */
void FileHandler::ignoreLines(istream& fstream, size_t num_lines) {
    string s;
    for (; num_lines && fstream.peek() != EOF; --num_lines) {
        getline(fstream, s);
    }
}

/**
 * Read (up - low) lines from fstream and parse them to points, that are stored into the dataset.
 * If the pointers to min_x, max_x, min_y, max_y are set, compute and store the range of the coordinates.
 */
void FileHandler::readDatasetPortion(istream& fstream, vector<tuple<double, double>>& dataset,
                        long unsigned int low, long unsigned int up) {
    long unsigned int lines_to_read = up - low;
    long unsigned int read_lines = 0;
    while (fstream.good() && fstream.peek() != EOF && read_lines++ < lines_to_read) {
        string x, y;
        getline(fstream, x, ',');
        getline(fstream, y);
        if (!x.empty() && !y.empty()) {
            double _x = stod(x);
            double _y = stod(y);
            tuple<double, double> tuple = make_tuple(_x, _y);
            dataset.push_back(tuple);
        }
    }
}

/**
 * Read and parse the input_file, storing the right portion of the dataset on the bases of mpi_size and mpi_rank.
 * If the pointers to min_x, max_x, min_y, max_y are set, compute and store the range of the coordinates.
 */
vector<tuple<double, double>> FileHandler::loadDataset(string input_file, int mpi_rank, int mpi_size) {
    LOG(MINIMAL) << "Loading file..." << endl;
    ifstream fstream(input_file);

    if (!fstream.good()) {
        LOG(ABSENT) << "Error while reading the input file" << endl;
        throw runtime_error("Error while reading the input file");
    }

    // Read the first line containing the size of the dataset
    string dataset_size_str;
    getline(fstream, dataset_size_str);
    long unsigned int dataset_size = stoul(dataset_size_str);
    LOG(FULL_VERBOSE) << "Dataset total size: " << dataset_size << endl;

    // Calculate the indexes of the dataset portion on which the current process will run
    long unsigned int chunk_size = (dataset_size / mpi_size);
    long unsigned int low = chunk_size * mpi_rank;
    long unsigned int up = chunk_size * (mpi_rank + 1);
    if (dataset_size - up < chunk_size) {
        up = dataset_size;
    }
    ALL_PROCESSES_LOG(FULL_VERBOSE) << "Process: " << mpi_rank << ", low: " << low << ", up: " << up << endl;

    // Seek to the low-th row of the file (+ 1, becuase of the first read)
    ignoreLines(fstream, low);

    // Read the portion of the dataset
    vector<tuple<double, double>> dataset = {};
    readDatasetPortion(fstream, dataset, low, up);

    fstream.close();
    return dataset;
}

/**
 * Return the filename of the partial result file, on the basis of the rank.
 */
string FileHandler::getPartialResultFilename(string output_file, int rank) {
    if (rank == 0) {
        return output_file;
    } else {
        stringstream ss;
        ss << output_file << ".part" << rank;
        return ss.str();
    }
}

/**
 * Write on the output_file all the dataset, referring every point to the correct centroid.
 */
void FileHandler::writeResultToFile(vector<Centroid>& centroids, string output_file) {
    LOG(MINIMAL) << "Writing result to file..." << endl;
    char buff[64];
    output_file = getPartialResultFilename(output_file, mpi_rank);
    ofstream o(output_file);

    for (uint i = 0; i < centroids.size(); ++i) {
        for (uint j = 0; j < centroids[i].px.size(); ++j) {
            sprintf(buff, "%.4f,%.4f,%.4f,%.4f\n", centroids[i].x, centroids[i].y, centroids[i].px[j], centroids[i].py[j]);
            o << buff;
        }
    }
    o.close();

    LOG(MINIMAL) << "Result written to file" << endl;
}

/**
 * Append the content of the files named <output_file>.part<n> (where n is the mpi_rank of the slave) into the main file named <output_file>.
 * Return the number of points contained in the slave's output files.
 */
int FileHandler::mergeResult(string output_file, int mpi_size) {
    LOG(MINIMAL) << "Merging the files..." << endl;
    ofstream output(output_file, ios::app);
    if (!output.is_open()) {
        LOG_ERROR << "Cannot open the output file " << output_file << endl;
        return 0;
    }
    int num_points = 0;
    for (int k = 1; k < mpi_size; ++k) {
        string partial_out_filename = getPartialResultFilename(output_file, k);
        ifstream partial_out_file(partial_out_filename);
        if (!partial_out_file.is_open()) {
            LOG_ERROR << "Cannot open the partial output file " << partial_out_filename << endl;
            return 0;
        } else {
            while(partial_out_file.good()) {
                string line;
                getline(partial_out_file, line);
                if (!line.empty()) {
                    output << line << endl;
                    num_points++;
                }
            }
        }
        partial_out_file.close();
    }
    output.close();

    // delete all partial result files
    for (int k = 1; k < mpi_size; ++k) {
        string filename = getPartialResultFilename(output_file, k);
        if (remove(filename.data()) != 0) {
            LOG_ERROR << "ERROR deleting file " << filename << endl;
        }
    }

    return num_points;
}