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

struct Word {
  char word[STR_SIZE];
  size_t count;

  Word(const char *w, size_t c) : count(c) {
    strncpy(this->word, w, STR_SIZE);
    this->word[STR_SIZE - 1] = '\0';
  }

  Word() = default;
};

enum Tag { TAG_WORD = 1, TAG_WINDOW = 2 };
const size_t ROOT = 0;
const char TERMINATOR[] = "###";

MPI_Datatype wordDataType;

void initDataTypes() {
  int blocklengths[] = {STR_SIZE, 1};
  MPI_Aint offsets[] = {offsetof(Word, word), offsetof(Word, count)};
  MPI_Datatype types[] = {MPI_CHAR, MPI_UNSIGNED_LONG_LONG};

  MPI_Type_create_struct(2, blocklengths, offsets, types, &wordDataType);
  MPI_Type_commit(&wordDataType);
}

void processWord(char *word, result_t &result, size_t winSize, size_t winStep) {
  result[word]++;
  if (result[word] == winSize) {
    Word w(word, winSize);
    MPI_Send(&w, 1, wordDataType, ROOT, TAG_WINDOW, MPI_COMM_WORLD);
    result[word] -= winStep;
  }
}

// receive words, aggregate and send to 0 if window expired
void receiveWords(result_t &result, int &numFinishedSendingWord, size_t winSize,
                  size_t winStep) {
  int flag;
  char cur[STR_SIZE];

  while (1) {
    MPI_Iprobe(MPI_ANY_SOURCE, TAG_WORD, MPI_COMM_WORLD, &flag,
               MPI_STATUS_IGNORE);
    if (!flag)
      break;
    // data available, read it
    MPI_Recv(cur, STR_SIZE, MPI_CHAR, MPI_ANY_SOURCE, TAG_WORD, MPI_COMM_WORLD,
             MPI_STATUS_IGNORE);
    if (strcmp(cur, TERMINATOR) == 0)
      numFinishedSendingWord++;
    else {
      processWord(cur, result, winSize, winStep);
    }
  }
}

int main(int argc, char **argv) {
  int rank;
  int numProcesses;

  MPI_Init(&argc, &argv);
  MPI_Comm_size(MPI_COMM_WORLD, &numProcesses);
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);

  const int numWorkers = numProcesses - 1;

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
      std::cerr << "Usage: " << argv[0] << " [-h] [-d dataset]\n";
      std::cerr << " -h          Show this message and exit\n";
      std::cerr << " -d dataset  Use the specified dataset file\n";
      std::exit(1);
    case -1:
      break;
    }
    break;
  }

  if (rank == 0) { // I am the root. I only receive windows
    Timer time("total");
    int numFinishedSendingWin = 0;
    // receive windows
    while (numFinishedSendingWin < numWorkers) {
      Word w;
      MPI_Recv(&w, 1, wordDataType, MPI_ANY_SOURCE, TAG_WINDOW, MPI_COMM_WORLD,
               MPI_STATUS_IGNORE);
      if (strcmp(w.word, TERMINATOR) == 0)
        numFinishedSendingWin++;
      //else
      //std::cout << w.word << "," << w.count << std::endl;
    }
    time.stop();
    Timer::printTimes();
  } else { // I am a worker. I read file, send/receive words, send windows to
           // root
    int numFinishedSendingWord = 0;
    result_t result;

    const size_t datasetSize = std::filesystem::file_size(datasetPath);
    const size_t processChunk = (datasetSize + numWorkers - 1) / numWorkers;
    auto fd = open(datasetPath.c_str(), O_RDONLY);
    char *mmapped =
        (char *)mmap(NULL, datasetSize, PROT_READ, MAP_SHARED, fd, 0);
    size_t start = processChunk * (rank - 1);
    size_t end = start + processChunk;
    size_t pos = start;
    size_t mb = 0;

    // if starting in the middle of file, skip until the end of first line
    if (start != 0) {
      while (mmapped[pos] != '\n')
        pos++;
    }

    // read file (using mmap) line by line, sending and receiving words
    while (pos < datasetSize && pos <= end) {
      if ((pos - start) / (10 * 1024 * 1024) > mb) {
        mb = (pos - start) / (10 * 1024 * 1024);
        std::cerr << rank << " read " << 10 * mb << " MBytes" << std::endl;
      }
      char cur[STR_SIZE];
      size_t cur_pos = 0;
      // read next line and send words around
      char c;
      do {
        c = mmapped[pos++];
        if (c >= 'a' && c <= 'z')
          cur[cur_pos++] = c;
        else if (c >= 'A' && c <= 'Z')
          cur[cur_pos++] = tolower(c);
        else {
          if (cur_pos > 0) {
            cur[cur_pos++] = '\0';
            size_t h = std::_Hash_bytes(cur, cur_pos, 0);
            int dest = h % numWorkers + 1;
            if (dest == rank) { // I am responsible for this word
              processWord(cur, result, winSize, winStep);
            } else {
              MPI_Send(cur, cur_pos, MPI_CHAR, dest, TAG_WORD, MPI_COMM_WORLD);
            }
          }
          cur_pos = 0;
        }
      } while (c != '\n' && pos < datasetSize); // misses last word if last line of dataset does not end in '\n'
      receiveWords(result, numFinishedSendingWord, winSize, winStep);
    }
    // send word terminator to everyone
    for (int dest = 1; dest < numProcesses; dest++) {
      if (dest != rank)
        MPI_Send(TERMINATOR, sizeof(TERMINATOR), MPI_CHAR, dest, TAG_WORD,
                 MPI_COMM_WORLD);
    }
    // receive last words and terminators
    numFinishedSendingWord++; // I finished
    while (numFinishedSendingWord < numWorkers) {
      receiveWords(result, numFinishedSendingWord, winSize, winStep);
    }
    // send last windows
    for (auto [k, v] : result) {
      Word w(k.c_str(), v);
      while (w.count > 0) {
        MPI_Send(&w, 1, wordDataType, ROOT, TAG_WINDOW, MPI_COMM_WORLD);
        if (w.count > winStep)
          w.count -= winStep;
        else
          break;
      }
    }
    // send window terminator to root
    Word w(TERMINATOR, 0);
    MPI_Send(&w, 1, wordDataType, ROOT, TAG_WINDOW, MPI_COMM_WORLD);
  }

  MPI_Type_free(&wordDataType);
  MPI_Finalize();
}
