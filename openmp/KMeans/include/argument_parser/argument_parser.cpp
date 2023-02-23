#include "argument_parser.hpp"
#include <algorithm>
#include <iostream>
#include <sstream>

using namespace std;

// Parse the arguments passed to main setting min_n_centroid, max_n_centroid and
// verbosity_level variables, setting default values if the relative argument is
// not specified.
int ArgumentParser::parseArgv(int argc, char *argv[], uint *n_centroid,
                              VerbosityLevel *verbosity_level,
                              string &input_file, string &output_file,
                              bool *write_file, bool *shared_fs,
                              uint *min_centroids_n, uint *max_centroids_n,
                              uint *num_iter) {
  // Default values
  *n_centroid = 0;
  *verbosity_level = DEFAULT_VERBOSITY_LEVEL;
  input_file = DEFAULT_INPUT_FILE;
  output_file = DEFAULT_OUTPUT_FILE;
  *write_file = DEFAULT_WRITE_FILE;
  *shared_fs = DEFAULT_SHARED_FS;
  *min_centroids_n = DEFAULT_MIN_CENTROIDS_N;
  *max_centroids_n = DEFAULT_MAX_CENTROIDS_N;
  *num_iter = DEFAULT_NUM_ITER;

  // Checking arguments
  int i = 1; // First argument is the executable file name
  int err = 0;
  while (i < argc && !err) {
    string arg = argv[i];
    if (arg == OPTION_HELP || arg == EXPLICIT_OPTION_HELP) {
      showUsage(argv[0]);
      err = 1;
    } else if (arg == OPTION_N_CENTROID || arg == EXPLICIT_OPTION_N_CENTROID) {
      if (i + 1 < argc) {
        err = parseCentroidsNumberParam(argv[i + 1], n_centroid);
      } else {
        LOG_ERROR << "The " << EXPLICIT_OPTION_N_CENTROID
                  << " option requires two arguments." << endl;
        err = 1;
      }
      i += 2;
    } else if (arg == OPTION_NUM_ITER || arg == EXPLICIT_OPTION_NUM_ITER) {
      if (i + 1 < argc) {
        err = parseNumIterParam(argv[i + 1], num_iter);
      } else {
        LOG_ERROR << "The " << EXPLICIT_OPTION_NUM_ITER
                  << " option requires two arguments." << endl;
        err = 1;
      }
      i += 2;
    } else if (arg == OPTION_VERBOSITY_LEVEL ||
               arg == EXPLICIT_OPTION_VERBOSITY_LEVEL) {
      *verbosity_level = MINIMAL;
      // Check if the verbosity level is specified
      if (i + 1 < argc && argv[i + 1][0] != '-') {
        err = parseVerbosityLevel(argv[i + 1], verbosity_level);
      }
      i += 2;
    } else if (arg == OPTION_INPUT_FILE || arg == EXPLICIT_OPTION_INPUT_FILE) {
      if (i + 1 < argc) {
        input_file = argv[i + 1];
      } else {
        LOG_ERROR << "The " << EXPLICIT_OPTION_INPUT_FILE
                  << " option requires one argument." << endl;
        err = 1;
      }
      i += 2;
    } else if (arg == OPTION_OUTPUT_FILE ||
               arg == EXPLICIT_OPTION_OUTPUT_FILE) {
      if (i + 1 < argc) {
        output_file = argv[i + 1];
      } else {
        LOG_ERROR << "The " << EXPLICIT_OPTION_OUTPUT_FILE
                  << " option requires one argument." << endl;
        err = 1;
      }
      i += 2;
    } else if (arg == OPTION_NO_WRITE || arg == EXPLICIT_OPTION_NO_WRITE) {
      *write_file = false;
      i += 1;
    } else if (arg == OPTION_SHARED_FS || arg == EXPLICIT_OPTION_SHARED_FS) {
      if (i + 1 < argc) {
        err = parseSharedFileSys(argv[i + 1], shared_fs);
      } else {
        LOG_ERROR << "The " << EXPLICIT_OPTION_SHARED_FS
                  << " option requires one argument." << endl;
        err = 1;
      }
      i += 2;
    } else if (arg == OPTION_ELBOW || arg == EXPLICIT_OPTION_ELBOW) {
      if (i + 2 < argc) {
        err = parseCentroidsRangeParam(argv[i + 1], argv[i + 2],
                                       min_centroids_n, max_centroids_n);
      } else {
        LOG_ERROR << "The " << EXPLICIT_OPTION_ELBOW
                  << " option requires two arguments." << endl;
        err = 1;
      }
      i += 3;
    } else {
      LOG_ERROR << "Unknown parameter found. Run again with -h option to print "
                   "the usage."
                << endl;
      err = 1;
    }
  }
  return err;
}

// Print the list of available options
void ArgumentParser::showUsage(const string program_name) {
  LOG_ERROR
      << "Usage: " << program_name << " <option(s)>" << endl
      << "Options:" << endl
      << endl
      << "  " << OPTION_HELP << "," << EXPLICIT_OPTION_HELP
      << "\t\t\t\tShow this help message." << endl
      << endl
      << "  " << OPTION_N_CENTROID << "," << EXPLICIT_OPTION_N_CENTROID << " N"
      << "\t\tSpecify the number centroids to use." << endl
      << "\t\t\t\t\tIn absence of this argument the Elbow method is used to "
         "get the optimal number of centroids."
      << endl
      << endl
      << "  " << OPTION_NUM_ITER << "," << EXPLICIT_OPTION_NUM_ITER << " N"
      << "\t\t\tSpecify the number of iterations." << endl
      << endl
      << endl
      << "  " << OPTION_ELBOW << "," << EXPLICIT_OPTION_ELBOW << " MIN MAX"
      << "\t\t\tSpecify the minimum and maximum number of centroids to use in "
         "order to perform the"
      << endl
      << "\t\t\t\t\tElbow method and choose the optimal number of centroids."
      << endl
      << "\t\t\t\t\tIn absence of this argument, if it is not specified the "
      << EXPLICIT_OPTION_N_CENTROID << " option," << endl
      << "\t\t\t\t\tthe MIN is set to " << DEFAULT_MIN_CENTROIDS_N
      << ", and the MAX is set to " << DEFAULT_MAX_CENTROIDS_N << endl
      << endl
      << "  " << OPTION_INPUT_FILE << "," << EXPLICIT_OPTION_INPUT_FILE
      << " INPUT_FILE"
      << "\t\tSpecify the path for the input dataset." << endl
      << "\t\t\t\t\tIn absence of this argument the input_file is set to "
      << DEFAULT_INPUT_FILE << endl
      << endl
      << "  " << OPTION_OUTPUT_FILE << "," << EXPLICIT_OPTION_OUTPUT_FILE
      << " OUTPUT_FILE"
      << "\t\tSpecify the path for the output dataset." << endl
      << "\t\t\t\t\tIn absence of this argument the output_file is set to "
      << DEFAULT_OUTPUT_FILE << endl
      << endl
      << "  " << OPTION_VERBOSITY_LEVEL << ","
      << EXPLICIT_OPTION_VERBOSITY_LEVEL << " [VERBOSITY_LEVEL=1]"
      << "\tSpecify the verbosity level (ABSENT = 0, MINIMAL = 1, NORMAL = 2, "
         "FULL_VERBOSE = 3)."
      << endl
      << "\t\t\t\t\tIn absence of this argument the verbosity level is set to "
      << DEFAULT_VERBOSITY_LEVEL << endl
      << endl
      << "  " << OPTION_NO_WRITE << "," << EXPLICIT_OPTION_NO_WRITE
      << "\t\t\tAvoid writing the result to file." << endl
      << endl
      << "  " << OPTION_SHARED_FS << "," << EXPLICIT_OPTION_SHARED_FS
      << " SHARED_FS"
      << "\t\tIndicate if the MPI processes work in a shared file system."
      << endl
      << "\t\t\t\t\tSHARED_FS = 1: presence of shared file system, SHARED_FS = "
         "0: there is no shared file system."
      << endl
      << "\t\t\t\t\tIn absence of this argument SHARED_FS is set to "
      << DEFAULT_SHARED_FS << "." << endl
      << "\t\t\t\t\tIn the case of a shared file system, each MPI process "
         "writes its own portion of the result"
      << endl
      << "\t\t\t\t\tin a dedicated file, then all the files are merged by the "
         "main process in a single result"
      << endl
      << "\t\t\t\t\tfile. Otherwise, every MPI process sends its own portion "
         "of the result to the main process,"
      << endl
      << "\t\t\t\t\tand then the main process will write a single file with "
         "the complete result."
      << endl
      << endl;
}

// Check whether the given string is a number
bool ArgumentParser::isNumber(const string &s) {
  return !s.empty() && all_of(s.begin(), s.end(), ::isdigit);
}

// Check whether the given string is a float number
bool ArgumentParser::isFloat(const string &s) {
  istringstream iss(s);
  float f;
  iss >> noskipws >> f; // noskipws considers leading whitespace invalid
  // Check the entire string was consumed and if either failbit or badbit is set
  return iss.eof() && !iss.fail();
}

// Parse the centroids_number option's arguments and set n_centroid. Return 0 if
// there are no errors, 1 otherwise.
int ArgumentParser::parseCentroidsNumberParam(const char *n_str,
                                              uint *n_centroid) {
  if (!isNumber(n_str)) {
    LOG_ERROR << "Wrong type for argument of parameter --centroids_number"
              << endl;
    return 1;
  }
  *n_centroid = atoi(n_str);

  if (*n_centroid < 1) {
    LOG_ERROR << "Setting n_centroid to 1" << endl;
    *n_centroid = 1;
  }
  return 0;
}

// Parse the num_iter option's arguments and set num_iter. Return 0 if there are
// no errors, 1 otherwise.
int ArgumentParser::parseNumIterParam(const char *n_str, uint *num_iter) {
  if (!isNumber(n_str)) {
    LOG_ERROR << "Wrong type for argument of parameter --num-iter" << endl;
    return 1;
  }
  *num_iter = atoi(n_str);

  if (*num_iter < 1) {
    LOG_ERROR << "Setting num_iter to 1" << endl;
    *num_iter = 1;
  }
  return 0;
}

// Parse the centroids number range and set min_centroids_n and max_centroids_n.
// Return 0 if there are no errors, 1 otherwise.
int ArgumentParser::parseCentroidsRangeParam(const char *min_n_str,
                                             const char *max_n_str,
                                             uint *min_centroids_n,
                                             uint *max_centroids_n) {
  if (!isNumber(min_n_str)) {
    LOG_ERROR
        << "Wrong type for arguments MIN_N of parameter --centroids_number"
        << endl;
    ;
    LOG_ERROR
        << "Wrong type for arguments MIN_N of parameter --centroids_number"
        << endl;
    return 1;
  }
  if (!isNumber(max_n_str)) {
    LOG_ERROR
        << "Wrong type for arguments MAX_N of parameter --centroids_number"
        << endl;
    ;
    LOG_ERROR
        << "Wrong type for arguments MAX_N of parameter --centroids_number"
        << endl;
    return 1;
  }
  *min_centroids_n = atoi(min_n_str);
  *max_centroids_n = atoi(max_n_str);
  if (*min_centroids_n < 1) {
    LOG_ERROR << "Setting min_centroids_n to 1" << endl;
    *min_centroids_n = 1;
  }
  if (*min_centroids_n > *max_centroids_n) {
    LOG_ERROR << "Setting max_centroids_n equal to min_centroids_n ("
              << *min_centroids_n << ")" << endl;
    *max_centroids_n = *min_centroids_n;
  }
  return 0;
}

// Parse the verbose option's argument and set the verbosity_level. Return 0 if
// there are no errors, 1 otherwise.
int ArgumentParser::parseVerbosityLevel(const char *verbosity_level_str,
                                        VerbosityLevel *verbosity_level) {
  if (!isNumber(verbosity_level_str)) {
    LOG_ERROR
        << "Wrong type for argument VERBOSITY_LEVEL of parameter --verbose"
        << endl;
    return 1;
  }
  int level = atoi(verbosity_level_str);
  if (level < ABSENT) {
    LOG_ERROR << "Setting verbosity level to " << ABSENT << endl;
    level = ABSENT;
  } else if (level > FULL_VERBOSE) {
    LOG_ERROR << "Setting verbosity level to " << FULL_VERBOSE << endl;
    level = FULL_VERBOSE;
  }
  *verbosity_level = static_cast<VerbosityLevel>(level);
  return 0;
}

// Parse the shared file system option and set shared_fs. Return 0 if there are
// no errors, 1 otherwise.
int ArgumentParser::parseSharedFileSys(const char *shared_fs_str,
                                       bool *shared_fs) {
  int err = 0;
  int s = DEFAULT_SHARED_FS;
  if (!isNumber(shared_fs_str)) {
    err = 1;
  } else {
    s = atoi(shared_fs_str);
    if (s != 0 && s != 1) {
      err = 1;
    }
  }
  if (!err) {
    *shared_fs = s == 1;
  } else {
    LOG_ERROR << "Wrong type for argument SHARED_FS of parameter "
              << EXPLICIT_OPTION_SHARED_FS << endl;
  }
  return err;
}
