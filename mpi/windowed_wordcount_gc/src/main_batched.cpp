#include "timing.hpp"
#include <fcntl.h>
#include <filesystem>
#include <mpi.h>
#include <string>
#include <sys/mman.h>
#include <unistd.h>
#include <unordered_map>
#include <vector>

using result_t = std::unordered_map<std::string, size_t>;

const size_t STR_SIZE = 35;
const size_t PER_PROCESS_BATCH_SIZE = 5*1024;
const size_t MAX_PER_PROCESS_BATCH_SIZE = 10 * PER_PROCESS_BATCH_SIZE;  // Awful :-)
  
const char TERMINATOR[] = "###";

struct WordList {
  char data[MAX_PER_PROCESS_BATCH_SIZE];
  size_t lastPos;
  
  WordList() : lastPos(0) {
    memset(data, 0, MAX_PER_PROCESS_BATCH_SIZE);
  }

  void addWord(const char *w, const size_t wLen) {
    // wLen is the entire char[] len, including the terminator '\0'
    memcpy(data+lastPos, w, wLen);
    lastPos += wLen;
  }

  void clear() {
    lastPos = 0;
  }
};
  
void processWord(char *word, result_t &result, size_t winSize, size_t winStep, long &processedWindows) {
  result[word]++;
  if (result[word] == winSize) {
    result[word] -= winStep;
    processedWindows++;
  }
}

// receive words from others and process them
void receiveWords(result_t &result, int &numFinishedSendingWords, WordList *words, int rank, int numProcesses, size_t winSize, size_t winStep, long &processedWindows) {
  MPI_Status status;
  int count;
  for (int source=0; source<numProcesses; source++) {
    if (source == rank) continue;
    MPI_Recv(words[rank].data, MAX_PER_PROCESS_BATCH_SIZE, MPI_CHAR, source, 0, MPI_COMM_WORLD, &status);
    MPI_Get_count(&status, MPI_CHAR, &count);
    words[rank].lastPos = count;
    if (words[rank].lastPos == 1) { // there were no words for me
      continue;
    }
    size_t cur_pos = 0;
    while (cur_pos < words[rank].lastPos) {
      char *curWord = words[rank].data + cur_pos;
      if (strcmp(curWord, TERMINATOR) == 0) {
        numFinishedSendingWords++;
      } else {
        processWord(curWord, result, winSize, winStep, processedWindows);
      }
      cur_pos += strlen(curWord)+1;
    }
  }
}

int main(int argc, char **argv) {
  int rank;
  int numProcesses;

  MPI_Init(&argc, &argv);
  MPI_Comm_size(MPI_COMM_WORLD, &numProcesses);
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);

  std::string datasetPath = "data/gutenberg40.txt";
  size_t winSize = 10, winStep = 5;

  for (;;) {
    switch (getopt(argc, argv, "hd:")) {
    case 'd':
      datasetPath = optarg;
      continue;
    case 'h':
    default:
      std::cerr << "Usage: " << argv[0] << " [-h] [-d dataset]\n";
      std::cerr << " -h          Show this message and exit\n";
      std::cerr << " -d dataset  Use the specified dataset file\n";
      std::exit(1);
    case -1:
      break;
    }
    break;
  }

  int numFinishedSendingWords = 0;
  result_t result;
  WordList *words = new WordList[numProcesses];
  MPI_Request *sendWordsReq = new MPI_Request[numProcesses];
  long processedWindows = 0;

  MPI_Barrier(MPI_COMM_WORLD);
  Timer time("total");
  
  const size_t datasetSize = std::filesystem::file_size(datasetPath);
  const size_t processChunk = (datasetSize + numProcesses - 1) / numProcesses;
  auto fd = open(datasetPath.c_str(), O_RDONLY);
  char *mmapped = (char *)mmap(NULL, datasetSize, PROT_READ, MAP_SHARED, fd, 0);
  size_t start = processChunk * rank;
  size_t end = start + processChunk;
  size_t pos = start;
  size_t batch_size = PER_PROCESS_BATCH_SIZE * numProcesses;
  size_t numBatchesProcessed = 0;

  // if starting in the middle of file, skip until the end of the first word
  if (start != 0) {
    char c = mmapped[pos];
    while ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')) {
      pos++;
      c = mmapped[pos];
    }
  }

  char cur[STR_SIZE];
  size_t cur_pos = 0;
  
  // read file (using mmap) sending and receiving words
  while (pos < datasetSize && pos <= end) {
    // read a batch stopping at the end of last word (do not break words)
    while (pos < datasetSize) {
      char c = mmapped[pos++];
      if (c >= 'a' && c <= 'z')
        cur[cur_pos++] = c;
      else if (c >= 'A' && c <= 'Z')
        cur[cur_pos++] = tolower(c);
      else {
        if (cur_pos > 0) {
          cur[cur_pos++] = '\0';
          size_t h = std::_Hash_bytes(cur, cur_pos, 0);
          int dest = h % numProcesses;
          if (dest == rank) { // I am responsible for this word
            processWord(cur, result, winSize, winStep, processedWindows);
          } else {
            words[dest].addWord(cur, cur_pos);
          }
        }
        cur_pos = 0;
        // Break after reading the last word
        if ((pos-start)/batch_size > numBatchesProcessed || pos > end) break;
      }
    }
    //fprintf(stderr, "%d processed batch %lu\n", rank, numBatchesProcessed);
    numBatchesProcessed = (pos-start)/batch_size;
    if (pos >= datasetSize || pos>end) {
      numFinishedSendingWords++; // I finished
      // place terminator as last word of every wordset
      for (int dest=0; dest<numProcesses; dest++) {
        if (dest == rank) continue;
        words[dest].addWord(TERMINATOR, strlen(TERMINATOR)+1);
      }
    }
    // send accumulated words around
    for (int dest=0; dest<numProcesses; dest++) {
      if (dest == rank) continue;
      if (words[dest].lastPos == 0) {
        // send a single char (the shortest word is two chars, one char plus the terminator)
        MPI_Isend(words[dest].data, 1, MPI_CHAR, dest, 0, MPI_COMM_WORLD, &sendWordsReq[dest]);
      } else {
        MPI_Isend(words[dest].data, words[dest].lastPos, MPI_CHAR, dest, 0, MPI_COMM_WORLD, &sendWordsReq[dest]);
      }
    }
    // receive words from others and process them
    receiveWords(result, numFinishedSendingWords, words, rank, numProcesses, winSize, winStep, processedWindows);
    // wait for async sends to complete and clear words buffers
    for (int dest=0; dest<numProcesses; dest++) {
      if (dest == rank) continue;
      MPI_Wait(&sendWordsReq[dest], MPI_STATUS_IGNORE);
      words[dest].clear();
    }
  } // finished reading file
  // receive last words and terminators. Send empty results to avoid locking others
  while (numFinishedSendingWords < numProcesses) {
    // send empty results to others, avoiding deadlocks
    for (int dest=0; dest<numProcesses; dest++) {
      if (dest == rank) continue;
      MPI_Isend(words[dest].data, 1, MPI_CHAR, dest, 0, MPI_COMM_WORLD, &sendWordsReq[dest]);
    }
    receiveWords(result, numFinishedSendingWords, words, rank, numProcesses, winSize, winStep, processedWindows);
    // wait for async sends to complete
    for (int dest=0; dest<numProcesses; dest++) {
      if (dest == rank) continue;
      MPI_Wait(&sendWordsReq[dest], MPI_STATUS_IGNORE);
    }
  }
  // process last windows
  for (auto [k, v] : result) {
    while (v > 0) {
      processedWindows++;
      if (v > winStep) {
        v -= winStep;
      } else {
        break;
      }
    }
  }

  MPI_Barrier(MPI_COMM_WORLD);
  time.stop();

  long totalProcessedWindows;
  MPI_Reduce(&processedWindows, &totalProcessedWindows, 1, MPI_LONG, MPI_SUM, 0, MPI_COMM_WORLD);
  if (rank == 0) {
    std::cout << "Total number of processed windows: " << totalProcessedWindows << std::endl;
    Timer::printTimes();
  }

  delete[] words;
  delete[] sendWordsReq;
  MPI_Finalize();
}
