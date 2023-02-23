#!/usr/bin/env bash
set -xe

mvn package

for h in 4 3 2 1; do
	~/flink/bin/stop-cluster.sh
	(for i in $(seq 1 $h); do echo StreamWorker$i; done) > ~/flink/conf/workers
	~/flink/bin/start-cluster.sh

	~/flink/bin/flink run -np $((8*h)) target/rollingTopWords-1.0.jar

	(for i in $(seq 1 $h); do ssh StreamWorker$i cat ~/flink/log/flink-ubuntu-taskexecutor-0-StreamWorker$i.out; done) > /home/ubuntu/rstream2/tools/benchmarks/rolling-results/flink/2M/${h}h.log
done
