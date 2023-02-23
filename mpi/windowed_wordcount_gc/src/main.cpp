#include "timing.hpp"
#include <mpi.h>
#include <string>
#include <unordered_map>
#include <vector>
#include <filesystem>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>

using result_t = std::unordered_map<std::string, size_t>;

const size_t STR_SIZE = 35;

struct Word {
  char word[STR_SIZE];
  size_t count;
  
  Word(const char *w, size_t c): count(c) {
    strncpy(this->word, w, STR_SIZE);
    this->word[STR_SIZE - 1] = '\0';
  }

  Word() = default;
};

MPI_Datatype wordDataType;

void initDataTypes() {
  int blocklengths[] = {STR_SIZE, 1};
  MPI_Aint offsets[] = {offsetof(Word, word), offsetof(Word, count)};
  MPI_Datatype types[] = {MPI_CHAR, MPI_UNSIGNED_LONG_LONG};

  MPI_Type_create_struct(2, blocklengths, offsets, types, &wordDataType);
  MPI_Type_commit(&wordDataType);
}

// receive words, aggregate and send to 0 if window expired
void receiveWords(result_t &result, int &numFinishedSendingWord, size_t winSize, size_t winStep, int rank) {
  int flag;
  char cur[STR_SIZE];
  
  while (1) {
    MPI_Iprobe(MPI_ANY_SOURCE, 1, MPI_COMM_WORLD, &flag, MPI_STATUS_IGNORE);
    if (!flag) break;
    // data available, read it
    MPI_Recv(cur, STR_SIZE, MPI_CHAR, MPI_ANY_SOURCE, 1, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    if (strcmp(cur,"###") == 0) numFinishedSendingWord++;
    else {
      result[cur]++;
      if (result[cur] == winSize) {
        if (rank != 0) {
          Word w(cur, winSize);
          MPI_Bsend(&w, 1, wordDataType, 0, 2, MPI_COMM_WORLD);
        }
        result[cur] -= winStep;
      }
    }
  }
}

// receive windows
void receiveWindows(int &numFinishedSendingWin) {
  int flag;
  while (1) {
    MPI_Iprobe(MPI_ANY_SOURCE, 2, MPI_COMM_WORLD, &flag, MPI_STATUS_IGNORE);
    if (!flag) break;
    Word w;
    MPI_Recv(&w, 1, wordDataType, MPI_ANY_SOURCE, 2, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    if (strcmp(w.word,"###")==0) numFinishedSendingWin++;
  }
}

int main(int argc, char **argv) {
  int rank;
  int numProcesses;

  MPI_Init(&argc, &argv);
  MPI_Comm_size(MPI_COMM_WORLD, &numProcesses);
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);

  initDataTypes();
  void * buf = malloc(1024*1024);
  MPI_Buffer_attach(buf, 1024*1024);

  std::string datasetPath = "data/gutenberg40.txt";
  size_t winSize = 10, winStep = 5;
  
  for (;;) {
    switch (getopt(argc, argv, "hd:")) {
    case 'd':
      datasetPath = optarg;
      continue;
    case 'h':
    default:
      std::cerr << "Usage: " << argv[0]
                << " [-h] [-d dataset] [-m method]\n";
      std::cerr << " -h          Show this message and exit\n";
      std::cerr << " -d dataset  Use the specified dataset file\n";
      std::exit(1);
    case -1:
      break;
    }
    break;
  }

  Timer time("total");
  int numFinishedSendingWord = 0, numFinishedSendingWin = 0;
  result_t result;

  const size_t datasetSize = std::filesystem::file_size(datasetPath);
  const size_t processChunk = (datasetSize + numProcesses - 1) / numProcesses;
  auto fd = open(datasetPath.c_str(), O_RDONLY);
  char *mmapped = (char *)mmap(NULL, datasetSize, PROT_READ, MAP_SHARED, fd, 0);
  size_t start = processChunk * rank;
  size_t end = start + processChunk;
  size_t pos = start;
  size_t mb = 0;

  // if starting in the middle of file, skip until the end first line
  if (start != 0) {
    while (mmapped[pos] != '\n') pos++;
  }

  // read file (using mmap) line by line, sending and receiving words
  while (pos < datasetSize && pos <= end) {
    if ((pos-start)/(10*1024*1024) > mb) {
      mb = (pos-start)/(10*1024*1024);
      std::cout << rank << " read " << 10*mb << " MBytes"<< std::endl;
    }
    char cur[STR_SIZE];
    size_t cur_pos = 0;
    // read next line and send words around
    char c = mmapped[pos++];
    while (c != '\n') {
      if (c >= 'a' && c <= 'z') cur[cur_pos++] = c;
      else if (c >= 'A' && c <= 'Z') cur[cur_pos++] = tolower(c);
      else {
        if (cur_pos>0) {
          cur[cur_pos++] = '\0';
          size_t h = std::_Hash_bytes(cur, cur_pos, 0);
          int dest = h % numProcesses;
          if(dest==rank) {
            result[cur]++;
            if (result[cur] == winSize) {
              if (rank != 0) {
                Word w(cur, winSize);
                MPI_Bsend(&w, 1, wordDataType, 0, 2, MPI_COMM_WORLD);
              }
              result[cur] -= winStep;
            }
          } else {
            MPI_Send(cur, cur_pos, MPI_CHAR, dest, 1, MPI_COMM_WORLD);
          }
        }
        cur_pos = 0;
      }
      c = mmapped[pos++];
    }
    receiveWords(result, numFinishedSendingWord, winSize, winStep, rank) ;
    if (rank == 0) receiveWindows(numFinishedSendingWin);
  }
  // send word terminator to everyone
  for(int dest=0; dest < numProcesses; dest++) {
    if (dest != rank) MPI_Send("###", 4, MPI_CHAR, dest, 1, MPI_COMM_WORLD);
  }
  
  numFinishedSendingWord++; // I finished
  while (numFinishedSendingWord < numProcesses) {
    receiveWords(result, numFinishedSendingWord, winSize, winStep, rank) ;
    if (rank == 0) receiveWindows(numFinishedSendingWin);
  }  

  // send last windows
  for (auto [k, v] : result) {
    if (rank != 0) {
      Word w(k.c_str(), v);
      MPI_Bsend(&w, 1, wordDataType, 0, 2, MPI_COMM_WORLD);
    }
  }

  if (rank != 0) {
    // send window terminator
    Word w("###", 0);
    MPI_Bsend(&w, 1, wordDataType, 0, 2, MPI_COMM_WORLD);
  } else {
    numFinishedSendingWin++; // I finished
    while (numFinishedSendingWin < numProcesses) {
      receiveWindows(numFinishedSendingWin);
    }
  }
  
  time.stop();
  
  if (rank == 0) {
#ifndef NDEBUG
    std::vector<std::pair<std::string, size_t>> sorted(result.begin(), result.end());
    std::sort(sorted.begin(), sorted.end());
    for (auto &t : sorted) {
      std::cout << t.first << "," << t.second << "\n";
    }
#endif
    Timer::printTimes();
  }

  MPI_Type_free(&wordDataType);
  MPI_Finalize();
}
