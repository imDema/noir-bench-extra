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

package WordCountWindowed;

import java.util.Comparator;
import java.util.List;
import java.util.regex.Pattern;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

public class StreamingJob {
  public static void main(String[] args) throws Exception {

    final ParameterTool params = ParameterTool.fromArgs(args);

    // set up the execution environment
    final StreamExecutionEnvironment env =
        StreamExecutionEnvironment.getExecutionEnvironment();

    // make parameters available in the web interface
    env.getConfig().setGlobalJobParameters(params);

    // get input data
    DataStream<String> text = env.readTextFile(params.get("input"));
    long win_size = Long.parseLong(params.get("win_size"));
    long win_steps = Long.parseLong(params.get("win_steps"));
    long win_interval = win_size / win_steps;

    Preconditions.checkNotNull(text, "Input DataSet should not be null.");

    Summer sink = new Summer();

    long start = System.nanoTime();
    text.flatMap(new Tokenizer())
        .keyBy(tuple -> tuple.f0)
        .countWindow(win_size, win_interval)
        .sum(1)
        .addSink(sink);

    JobExecutionResult res = env.execute("WindowWordCount");
    long stop = System.nanoTime();
    System.out.printf("total:%d", stop - start);
  }

  public static final class Summer
      implements SinkFunction<Tuple2<String, Integer>> {
    @Override
    public void invoke(Tuple2<String, Integer> value,
                       SinkFunction.Context context) {}
  }

  public static final class Tokenizer
      implements FlatMapFunction<String, Tuple2<String, Integer>> {

    Pattern patt = Pattern.compile("[^A-Za-z]+");

    @Override
    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
      String[] tokens = patt.matcher(value).replaceAll(" ").split("\\W+");
      // emit the pairs
      for (String token : tokens) {
        if (token.length() > 0) {
          out.collect(new Tuple2<>(token.toLowerCase(), 1));
        }
      }
    }
  }
}
