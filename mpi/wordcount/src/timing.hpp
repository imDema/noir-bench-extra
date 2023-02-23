#pragma once

#include <array>
#include <atomic>
#include <chrono>
#include <iostream>

// List with all the supported timing keys. They must be known beforehand to
// allow the `times` array to be treated as an MT-safe map using atomics.
// std::map is not thread-safe so an array of atomic values with direct access
// is used instead.
// std::chrono::steady_clock on x86_64 internally uses `rdtsc`.
constexpr const std::array KEYS = {"total", "csv", "network", "execution"};
inline std::array<std::atomic<size_t>, KEYS.size()> times;

// A class that measure time as accurately as possible. It internally uses
// std::chrono::steady_clock that avoids clock drifting and offers
// nanoseconds-precision.
class Timer {
  // index of the timer inside `times`
  size_t index;
  // time_point of the start of the timer
  std::chrono::time_point<std::chrono::steady_clock> start;
  // whether the timer has been stopped already
  bool stopped = false;

public:
  Timer(const char *name) {
    // store the index of the timer only, not the name
    index = std::find(KEYS.begin(), KEYS.end(), name) - KEYS.begin();
    if (index >= KEYS.size()) {
      throw std::runtime_error(std::string("Invalid Timer name: ") + name);
    }
    start = std::chrono::steady_clock::now();
  }

  // Automatically stop the timer when it goes out of scope, if not stopped
  // already.
  ~Timer() { stop(); }

  // Manually stop the timer, if not already stopped.
  void stop() {
    auto end = std::chrono::steady_clock::now();
    if (stopped)
      return;
    times[index] += (end - start).count();
    stopped = true;
  }

  // Print to stdout all the timers.
  static void printTimes() {
    for (size_t i = 0; i < KEYS.size(); i++) {
      std::cout << "timens:" << KEYS[i] << ":" << times[i] << std::endl;
    }
  }
};
