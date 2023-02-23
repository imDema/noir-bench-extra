/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package rollingTopWords;

import java.io.Serializable;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.MultipleParameterTool;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.*;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Slide;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;
import org.apache.flink.types.Row;
import org.apache.flink.table.descriptors.OldCsv;

import static org.apache.flink.table.api.Expressions.*;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.regex.Pattern;

public class RollingTopWords {

    static public class TopNAccum {
        public ArrayList<Tuple2<Long, String>> words = new ArrayList<>();
    }

    static public class TopN extends TableAggregateFunction<Tuple3<String, Long, Integer>, TopNAccum> {

        private static int N = 5;

        @Override
        public TopNAccum createAccumulator() {
            return new TopNAccum();
        }


        public void accumulate(TopNAccum acc, Long count, String word) {
            acc.words.add(Tuple2.of(count, word));
            acc.words.sort(Comparator.comparingLong(t -> -t.f0));
            while (acc.words.size() > N) {
                acc.words.remove(acc.words.size() - 1);
            }
        }

        public void merge(TopNAccum acc, java.lang.Iterable<TopNAccum> iterable) {
            for (TopNAccum otherAcc : iterable) {
                acc.words.addAll(otherAcc.words);
                acc.words.sort(Comparator.comparingLong(t -> -t.f0));
                while (acc.words.size() > N) {
                    acc.words.remove(acc.words.size() - 1);
                }
            }
        }

        public void emitValue(TopNAccum acc, Collector<Tuple3<String, Long, Integer>> out) {
            for (int i = 0; i < acc.words.size(); i++) {
                var word = acc.words.get(i);
                out.collect(Tuple3.of(word.f1, word.f0, i + 1));
            }
        }
    }

    static private class ThroughputTester implements Serializable {
        private String name;
        private long count;
        private long limit;
        private long last;
        private long total;
        private long start;

        private ThroughputTester(String name, long limit) {
            this.name = name;
            this.limit = limit;
            this.count = 0;
            this.last = System.nanoTime();
            this.total = 0;
            this.start = System.nanoTime();
        }

        private void add() {
            this.count += 1;
            this.total += 1;
            if (this.count > this.limit) {
                double elapsed = (System.nanoTime() - this.last) / 1.0e9;
                System.out.printf("%s: %10.2f/s @ %d @ %d\n", this.name, this.count / elapsed, System.nanoTime(), this.total);
                this.count = 0;
                this.last = System.nanoTime();
            }
        }

        private void sub() {
            this.count -= 1;
        }

        private void stop() {
            double elapsed = (System.nanoTime() - this.start) / 1.0e9;
            System.out.printf("(done) %s: %10.2f/s (total %d)\n", this.name, this.total / elapsed, this.total);
        }
    }

    static public class WordSource extends RichParallelSourceFunction<String> {
        private long count = 0L;
        private volatile boolean isRunning = true;
        private String[] topics = {
            "#love",
            "#instagood",
            "#fashion",
            "#photooftheday",
            "#beautiful",
            "#art",
            "#photography",
            "#happy",
            "#picoftheday",
            "#cute",
            "#follow",
            "#tbt",
            "#followme",
            "#nature",
            "#like",
            "#travel",
            "#instagram",
            "#style",
            "#repost",
            "#summer",
            "#instadaily",
            "#selfie",
            "#me",
            "#friends",
            "#fitness",
            "#girl",
            "#food",
            "#fun",
            "#beauty",
            "#instalike",
            "#smile",
            "#family",
            "#photo",
            "#life",
            "#likeforlike",
            "#music",
            "#ootd",
            "#follow",
            "#makeup",
            "#amazing",
            "#igers",
            "#nofilter",
            "#dog",
            "#model",
            "#sunset",
            "#beach",
            "#instamood",
            "#foodporn",
            "#motivation",
            "#followforfollow",
        };
        private Random random = new Random();

        private String randomTopic() {
            double epsilon = 0.1;
            for (String topic : topics) {
                if (random.nextDouble() < epsilon) {
                    return topic;
                }
            }
            return topics[0];
        }

        @Override
        public void run(SourceContext<String> ctx) {
            long start = System.nanoTime();
            int index = getRuntimeContext().getIndexOfThisSubtask();
            int parallelism = getRuntimeContext().getNumberOfParallelSubtasks();
            ThroughputTester tester = new ThroughputTester("source" + index, 10000);
            while (isRunning && count < 2000000 * 6 * 8 / parallelism) {
                synchronized (ctx.getCheckpointLock()) {
                    ctx.collectWithTimestamp(randomTopic(), count * parallelism + index);
                    if (count % 10 == 0) {
                        ctx.emitWatermark(new Watermark(count * parallelism + index));
                    }
                    tester.add();
                    count++;
                }
            }
            tester.stop();
        }

        public void cancel() {
            isRunning = false;
        }
    }

    public static void main(String[] args) throws Exception {

        final MultipleParameterTool params = MultipleParameterTool.fromArgs(args);
        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        var source = env.addSource(new WordSource(), "source");
        var words = tEnv.fromDataStream(source, $("word"), $("rowtime").rowtime());

        tEnv.registerFunction("topN", new TopN());

        var rankings = words
            // sliding window
            .window(Slide.over(lit(1).seconds()).every(lit(500).millis()).on($("rowtime")).as("win"))
            // partition by word, grouping by (word, windows)
            .groupBy($("word"), $("win"))
            // count how many words in each window
            .select($("win").rowtime().as("win_time"), $("word"), $("word").count().as("count"))
            // group only by window
            .groupBy($("win_time"))
            // compute the topN for each window
            .flatAggregate(call("topN", $("count"), $("word")).as("word", "count", "rank"))
            .select($("win_time"), $("word"), $("count"), $("rank"));

        DataStream<Tuple2<Boolean, Row>> stream = tEnv.toRetractStream(rankings, Row.class);
        stream.addSink(new SinkFunction<Tuple2<Boolean, Row>>() {
            ThroughputTester tester = new ThroughputTester("sink", 100);

            @Override
            public void invoke(Tuple2<Boolean, Row> tuple, SinkFunction.Context context) {
                if ((int)tuple.f1.getField(3) == 1) {
                    if (tuple.f0) {
                        tester.add();
                    } else {
                        tester.sub();
                    }
                }
            }
        }).setParallelism(1);

        env.execute();
    }
}
