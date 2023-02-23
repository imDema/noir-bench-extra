#include "timing.hpp"
#include <fcntl.h>
#include <filesystem>
#include <mpi.h>
#include <string>
#include <sys/mman.h>
#include <unistd.h>
#include <unordered_map>
#include <vector>

#define REQUESTS_LIMIT 10000

using result_t = std::unordered_map<std::string, size_t>;

const bool SINGLE_SINK = false;
const size_t STR_SIZE = 35;

struct Word {
  char word[STR_SIZE];
  size_t count;

  Word(const char *w, size_t c) : count(c) {
    strncpy(this->word, w, STR_SIZE);
    this->word[STR_SIZE - 1] = '\0';
  }

  Word() = default;
};

const size_t SINK = 0;
const char TERMINATOR[] = "###";

MPI_Datatype wordDataType;

void initDataTypes() {
  int blocklengths[] = {STR_SIZE, 1};
  MPI_Aint offsets[] = {offsetof(Word, word), offsetof(Word, count)};
  MPI_Datatype types[] = {MPI_CHAR, MPI_UNSIGNED_LONG_LONG};

  MPI_Type_create_struct(2, blocklengths, offsets, types, &wordDataType);
  MPI_Type_commit(&wordDataType);
}

void processWord(char *word, result_t &result, size_t winSize, size_t winStep, long &processedWindows) {
  result[word]++;
  if (result[word] == winSize) {
    Word w(word, winSize);
    if (SINGLE_SINK) {
      MPI_Send(&w, 1, wordDataType, SINK, 0, MPI_COMM_WORLD);
    }
    result[word] -= winStep;
    processedWindows++;
  }
}

// receive words, aggregate and send to 0 if window expired
void receiveWords(size_t winSize, size_t winStep, int numReaders, long &processedWindows) {
  result_t result;
  int numReceivedTerminators = 0;
  char cur[STR_SIZE];

  while (1) {
    MPI_Recv(cur, STR_SIZE, MPI_CHAR, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    if (strcmp(cur, TERMINATOR) == 0) {
      if (++numReceivedTerminators == numReaders) {
        break;
      }
    } else {
      processWord(cur, result, winSize, winStep, processedWindows);
    }
  }

  // send last windows
  for (auto [k, v] : result) {
    Word w(k.c_str(), v);
    while (w.count > 0) {
      if (SINGLE_SINK) {
        MPI_Send(&w, 1, wordDataType, SINK, 0, MPI_COMM_WORLD);
      }
      processedWindows++;
      if (w.count > winStep) {
        w.count -= winStep;
      } else {
        break;
      }
    }
  }

  if (SINGLE_SINK) {
    // send terminator to root
    Word w(TERMINATOR, 0);
    MPI_Send(&w, 1, wordDataType, SINK, 0, MPI_COMM_WORLD);
  }
}

// read words from file and send them
void readAndSendWords(int rank, int numReaders, int numReducers, std::string datasetPath) {
  const size_t datasetSize = std::filesystem::file_size(datasetPath);
  const size_t processChunk = (datasetSize + numReaders - 1) / numReaders;
  auto fd = open(datasetPath.c_str(), O_RDONLY);
  char *mmapped = (char *)mmap(NULL, datasetSize, PROT_READ, MAP_SHARED, fd, 0);
  int readerId = SINGLE_SINK ? rank - 1 : rank;
  size_t start = processChunk * readerId;
  size_t end = start + processChunk;
  size_t pos = start;
  size_t mb = 0;
  MPI_Request requests[REQUESTS_LIMIT];
  int currentRequest = 0;

  // if starting in the middle of file, skip until the end of the first word
  if (start != 0) {
    char c = mmapped[pos];
    while ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')) {
      pos++;
      c = mmapped[pos];
    }
  }

  char cur[STR_SIZE * REQUESTS_LIMIT];
  size_t cur_pos = 0;

  // read file (using mmap) line by line, sending and receiving words
  while (pos < datasetSize) {
    if ((pos - start) / (10 * 1024 * 1024) > mb) {
      mb = (pos - start) / (10 * 1024 * 1024);
      // std::cerr << rank << " read " << 10 * mb << " MBytes" << std::endl;
    }
    // read next line and send words around
    char c = mmapped[pos++];
    if (c >= 'a' && c <= 'z') {
      cur[STR_SIZE * currentRequest + cur_pos++] = c;
    } else if (c >= 'A' && c <= 'Z') {
      cur[STR_SIZE * currentRequest + cur_pos++] = tolower(c);
    } else {
      if (cur_pos > 0) {
        cur[STR_SIZE * currentRequest + cur_pos++] = '\0';
        size_t h = std::_Hash_bytes(&cur[STR_SIZE*currentRequest], cur_pos, 0);
        int dest = SINGLE_SINK ? 1 + numReaders + (h % numReducers) : numReaders + (h % numReducers);
        MPI_Isend(&cur[STR_SIZE*currentRequest], cur_pos, MPI_CHAR, dest, 0, MPI_COMM_WORLD, &requests[currentRequest]);
	      if (++currentRequest == REQUESTS_LIMIT) {
	        MPI_Waitall(currentRequest, requests, MPI_STATUS_IGNORE);
	        currentRequest = 0;
	      }
	    }
      // Break after reading the last word
      if (pos > end) {
        break;
      }
      cur_pos = 0;
    }
  }

  // wait for pending sends
  if (currentRequest > 0) {
    MPI_Waitall(currentRequest, requests, MPI_STATUS_IGNORE);
  }
  
  // send word terminator to everyone
  for (int i=0; i<numReducers; i++) {
    int dest = SINGLE_SINK ? 1 + numReaders + i : numReaders + i;
    MPI_Send(TERMINATOR, sizeof(TERMINATOR), MPI_CHAR, dest, 0, MPI_COMM_WORLD);
  }
}

void receiveWindows(int numReducers) {
  
  int numReceivedTerminators = 0;
  long numReceivedWindows = 0;
  
  while (numReceivedTerminators < numReducers) {
    Word w;
    MPI_Recv(&w, 1, wordDataType, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);   
    if (strcmp(w.word, TERMINATOR) == 0) {
      numReceivedTerminators++;
    } else {
      numReceivedWindows++;
    }
  }
  std::cout << "Sink finished after receiving " << numReceivedWindows << " windows" << std::endl;
}

int main(int argc, char **argv) {
  int rank;
  int numProcesses;

  MPI_Init(&argc, &argv);
  MPI_Comm_size(MPI_COMM_WORLD, &numProcesses);
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  
  const int numReaders = numProcesses / 2;
  const int numReducers = SINGLE_SINK ? numProcesses - numReaders - 1 : numProcesses - numReaders;

  initDataTypes();

  std::string datasetPath = "data/gutenberg40.txt";
  size_t winSize = 10, winStep = 5;

  for (;;) {
    switch (getopt(argc, argv, "hd:")) {
    case 'd':
      datasetPath = optarg;
      continue;
    case 'h':
    default:
      std::cerr << "Usage: " << argv[0] << " [-h] [-d dataset] [-m method]\n";
      std::cerr << " -h          Show this message and exit\n";
      std::cerr << " -d dataset  Use the specified dataset file\n";
      std::exit(1);
    case -1:
      break;
    }
    break;
  }

  MPI_Barrier(MPI_COMM_WORLD);
  Timer time("total");

  int firstReducer = SINGLE_SINK ? 1 + numReaders : numReaders;
  long processedWindows = 0;

  if (rank == 0 && SINGLE_SINK) { // I am the root. I only receive windows
    receiveWindows(numReducers);
  } else if (rank < firstReducer) { // I am a reader
    readAndSendWords(rank, numReaders, numReducers, datasetPath);
  } else { // I am a reducers
    receiveWords(winSize, winStep, numReaders, processedWindows);
  }

  MPI_Barrier(MPI_COMM_WORLD);
  time.stop();
  
  long totalProcessedWindows;
  MPI_Reduce(&processedWindows, &totalProcessedWindows, 1, MPI_LONG, MPI_SUM, 0, MPI_COMM_WORLD);
  if (rank == 0) {
    std::cout << "Total number of processed windows: " << totalProcessedWindows << std::endl;
    Timer::printTimes();
  }
  
  MPI_Type_free(&wordDataType);
  MPI_Finalize();
}
