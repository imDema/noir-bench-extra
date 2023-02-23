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

package triangles;

import java.util.ArrayList;
import java.util.Iterator;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import triangles.util.EnumTrianglesData;
import triangles.util.EnumTrianglesDataTypes.Edge;
import triangles.util.EnumTrianglesDataTypes.Triad;

public class EnumTriangles {

  // *************************************************************************
  //     PROGRAM
  // *************************************************************************

  private static class AdjList {

    int node;
    ArrayList<Integer> next;

    AdjList(int node, ArrayList<Integer> next) {
      this.node = node;
      this.next = next;
    }
  }

  public static void main(String[] args) throws Exception {

    // Checking input parameters
    final ParameterTool params = ParameterTool.fromArgs(args);

    // set up execution environment
    final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    // make parameters available in the web interface
    env.getConfig().setGlobalJobParameters(params);

    long start = System.nanoTime();

    // read input data
    DataSet<Edge> edges;
    if (params.has("edges")) {
      edges =
          env.readCsvFile(params.get("edges"))
              .fieldDelimiter(",")
              .includeFields(true, true)
              .types(Integer.class, Integer.class)
              .map((MapFunction<Tuple2<Integer, Integer>, Edge>) edge -> {
                if (edge.f0 < edge.f1) {
                  return new Edge(edge.f0, edge.f1);
                } else {
                  return new Edge(edge.f1, edge.f0);
                }
              });
    } else {
      System.out.println("Executing EnumTriangles example with default edges data set.");
      System.out.println("Use --edges to specify file input.");
      edges = EnumTrianglesData.getDefaultEdgeDataSet(env);
    }

    long triangles =
        edges
            // build triads
            .groupBy(Edge.V1)
            .reduceGroup((GroupReduceFunction<Edge, AdjList>) (edgesIter, out) -> {
              final Iterator<Edge> outEdges = edgesIter.iterator();
              ArrayList<Integer> next = new ArrayList<>();
              int node = -1;
              while (outEdges.hasNext()) {
                Edge e = outEdges.next();
                node = e.f0;
                next.add(e.f1);
              }
              out.collect(new AdjList(node, next));
            })
            .returns(Types.GENERIC(AdjList.class))
            .flatMap((FlatMapFunction<AdjList, Triad>) (adjList, collector) -> {
              for (int i = 0; i < adjList.next.size(); i++) {
                for (int j = 0; j < i; j++) {
                  int v2 = Math.min(adjList.next.get(i), adjList.next.get(j));
                  int v3 = Math.max(adjList.next.get(i), adjList.next.get(j));
                  collector.collect(new Triad(adjList.node, v2, v3));
                }
              }
            })
            .returns(Types.TUPLE(Triad.class))
            // filter triads
            .join(edges)
            .where(Triad.V2, Triad.V3)
            .equalTo(Edge.V1, Edge.V2)
            .with((JoinFunction<Triad, Edge, Triad>) (triad, edge) -> triad)
            .count();

    long stop = System.nanoTime();
    System.out.printf("total:%d\n", stop - start);

    System.out.printf("Triangles: %d\n", triangles);
  }
}
