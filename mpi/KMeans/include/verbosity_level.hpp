#ifndef VERBOSITY_LEVEL
#define VERBOSITY_LEVEL

enum VerbosityLevel {
	ABSENT = 0,
	MINIMAL = 1,
	NORMAL = 2,
	FULL_VERBOSE = 3
};

extern VerbosityLevel verbosity_level;
extern int mpi_rank;

#define LOG(level) if (mpi_rank == 0 && verbosity_level >= level) std::cout
#define ALL_PROCESSES_LOG(level) if (verbosity_level >= level) std::cout
#define LOG_ERROR if (mpi_rank == 0) std::cerr

#endif