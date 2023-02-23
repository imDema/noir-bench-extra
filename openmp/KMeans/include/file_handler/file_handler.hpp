#include <string>
#include <vector>
#include <fstream>

struct Centroid {
	double x;
	double y;
	uint n;
	std::vector<double> px;
	std::vector<double> py;
};

class FileHandler {
public:
	std::vector<std::tuple<double, double>> loadDataset(std::string input_file, int mpi_rank, int mpi_size);
	void writeResultToFile(std::vector<Centroid>& centroids, std::string output_file);
	int mergeResult(std::string output_file, int mpi_size);
private:
	void ignoreLines(std::istream& fstream, size_t num_lines);
	void readDatasetPortion(std::istream& fstream, std::vector<std::tuple<double, double>>& dataset,
	                        long unsigned int low, long unsigned int up);
	std::string getPartialResultFilename(std::string output_file, int rank);
};
