#include "../verbosity_level.hpp"
#include <string>

using namespace std;

#define OPTION_HELP "-h"
#define EXPLICIT_OPTION_HELP "--help"
#define OPTION_N_CENTROID "-cn"
#define EXPLICIT_OPTION_N_CENTROID "--centroids-number"
#define DEFAULT_VERBOSITY_LEVEL ABSENT
#define OPTION_VERBOSITY_LEVEL "-v"
#define EXPLICIT_OPTION_VERBOSITY_LEVEL "--verbose"
#define DEFAULT_INPUT_FILE "out.csv"
#define OPTION_INPUT_FILE "-if"
#define EXPLICIT_OPTION_INPUT_FILE "--input_file"
#define DEFAULT_OUTPUT_FILE "result.csv"
#define OPTION_OUTPUT_FILE "-of"
#define EXPLICIT_OPTION_OUTPUT_FILE "--output_file"
#define DEFAULT_WRITE_FILE true
#define OPTION_NO_WRITE "-nw"
#define EXPLICIT_OPTION_NO_WRITE "--no-write"
#define DEFAULT_SHARED_FS 1
#define OPTION_SHARED_FS "-sf"
#define EXPLICIT_OPTION_SHARED_FS "--shared_fs"
#define DEFAULT_MIN_CENTROIDS_N 3
#define DEFAULT_MAX_CENTROIDS_N 30
#define OPTION_ELBOW "-e"
#define EXPLICIT_OPTION_ELBOW "--elbow"
#define DEFAULT_NUM_ITER 30
#define OPTION_NUM_ITER "-it"
#define EXPLICIT_OPTION_NUM_ITER "--num-iter"

class ArgumentParser {
public:
  int parseArgv(int argc, char *argv[], uint *n_centroid,
                VerbosityLevel *verbosity_level, string &input_file,
                string &output_file, bool *write_file, bool *shared_fs,
                uint *min_centroids_n, uint *max_centroids_n, uint *num_iter);

private:
  void showUsage(const string program_name);
  bool isNumber(const string &s);
  bool isFloat(const string &s);
  int parseCentroidsNumberParam(const char *n_str, uint *n_centroid);
  int parseNumIterParam(const char *n_str, uint *num_iter);
  int parseVerbosityLevel(const char *verbosity_level_str,
                          VerbosityLevel *verbosity_level);
  int parseCentroidsRangeParam(const char *min_n_str, const char *max_n_str,
                               uint *min_centroids_n, uint *max_centroids_n);
  int parseSharedFileSys(const char *shared_fs_str, bool *shared_fs);
};
