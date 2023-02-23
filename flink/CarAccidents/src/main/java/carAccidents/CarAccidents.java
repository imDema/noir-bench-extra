package carAccidents;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.utils.MultipleParameterTool;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import java.util.List;
import java.util.stream.Stream;

public class CarAccidents {

    public static class Accident {
        public String date;
        public String borough;
        public int killed;
        public String factor1;
        public String factor2;
        public String factor3;
        public String factor4;
        public String factor5;

        private static final Integer[] daysBefore = {0, 0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334};

        public Tuple2<Integer, Integer> getWeek() {
            String[] splitted = this.date.split("/");
            int month = Integer.parseInt(splitted[0]);
            int day = Integer.parseInt(splitted[1]);
            int year = Integer.parseInt(splitted[2]);
            day += daysBefore[month];
            if (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0) && month >= 3) {
                day += 1;
            }
            return Tuple2.of(year, day / 7);
        }
    }

    /**
     * First query: Number of lethal accidents per Week
     */
    static void query1(DataSet<Accident> csv) throws Exception {
        DataSet<Tuple2<Tuple2<Integer, Integer>, Integer>> query1 = csv
                .map(new MapFunction<Accident, Tuple2<Tuple2<Integer, Integer>, Integer>>() {
                    @Override
                    public Tuple2<Tuple2<Integer, Integer>, Integer> map(Accident accident) {
                        return Tuple2.of(accident.getWeek(), accident.killed > 0 ? 1 : 0);
                    }
                })
                // Group by week
                .groupBy(0)
                // Number of lethal accidents
                .sum(1);

        query1.collect();
    }

    /**
     * Second query: Number of accidents and percentage of number of deaths per contributing factor
     */
    static void query2(DataSet<Accident> csv) throws Exception {
        DataSet<Tuple3<String, Integer, Integer>> query2 = csv
                .flatMap(new FlatMapFunction<Accident, Tuple3<String, Integer, Integer>>() {
                    @Override
                    public void flatMap(Accident accident, Collector<Tuple3<String, Integer, Integer>> collector) {
                        // Take distinct and non-empty factors
                        Stream.of(accident.factor1,
                                accident.factor2,
                                accident.factor3,
                                accident.factor4,
                                accident.factor5)
                                .distinct()
                                .filter(f -> f.length() > 0)
                                .map(f -> Tuple3.of(f, 1, accident.killed > 0 ? 1 : 0))
                                .forEach(collector::collect);
                    }
                })
                // Group by factor
                .groupBy(0)
                // Number of accidents
                .sum(1)
                // Number of lethal accidents
                .andSum(2);

        query2.collect();
    }

    /**
     * Third query: number of accidents and average number of lethal accidents per week per borough
     */
    static void query3(ExecutionEnvironment env, DataSet<Accident> csv) throws Exception {
        List<Tuple4<String, Tuple2<Integer, Integer>, Integer, Integer>> grouped_accidents = csv
                .map(new MapFunction<Accident, Tuple4<String, Tuple2<Integer, Integer>, Integer, Integer>>() {
                    @Override
                    public Tuple4<String, Tuple2<Integer, Integer>, Integer, Integer> map(Accident accident) {
                        return Tuple4.of(accident.borough, accident.getWeek(), 1, accident.killed > 0 ? 1 : 0);
                    }
                })
                // Group by borough and week (both year and number of week)
                .groupBy(0, 1)
                // Number of accidents
                .sum(2)
                // Number of lethal accidents
                .andSum(3)
                .collect();

        DataSet<Tuple5<String, Integer, Integer, Integer, Double>> avg_per_week = env.fromCollection(grouped_accidents)
                .map(new MapFunction<Tuple4<String, Tuple2<Integer, Integer>, Integer, Integer>, Tuple4<String, Integer, Integer, Integer>>() {
                    @Override
                    public Tuple4<String, Integer, Integer, Integer> map(Tuple4<String, Tuple2<Integer, Integer>, Integer, Integer> t) {
                        return Tuple4.of(t.f0, t.f1.f1, t.f2, t.f3);
                    }
                })
                // Group by borough and number of week
                .groupBy(0, 1)
                // Number of accidents
                .sum(2)
                // Number of lethal accidents
                .andSum(3)
                .map(new MapFunction<Tuple4<String, Integer, Integer, Integer>, Tuple5<String, Integer, Integer, Integer, Double>>() {
                    @Override
                    public Tuple5<String, Integer, Integer, Integer, Double> map(Tuple4<String, Integer, Integer, Integer> t) {
                        return Tuple5.of(t.f0, t.f1, t.f2, t.f3, (100.0 * t.f3) / t.f2);
                    }
                });

        avg_per_week.collect();
    }

    public static void main(String[] args) throws Exception {

        final MultipleParameterTool params = MultipleParameterTool.fromArgs(args);
        // set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        long start = System.nanoTime();

        // Read dataset
        DataSet<Accident> csv = env.readCsvFile(params.get("input"))
                .ignoreFirstLine()
                .parseQuotedStrings('"')
                .includeFields(
                        true,  // Date
                        false, // Time
                        true,  // Borough
                        false, // Zip
                        false, // Latitude
                        false, // Longitude
                        false, // Location
                        false, // On street name
                        false, // Cross street name
                        false, // Off street name
                        false, // Number of persons injured
                        true,  // Number of persons killed
                        false, // Number of pedestrians injured
                        false, // Number of pedestrians killed
                        false, // Number of cyclists injured
                        false, // Number of cyclists killed
                        false, // Number of motorists killed
                        false, // Number of motorists killed
                        true,  // Contributing factor vehicle 1
                        true,  // Contributing factor vehicle 2
                        true,  // Contributing factor vehicle 3
                        true,  // Contributing factor vehicle 4
                        true  // Contributing factor vehicle 5
                )
                .pojoType(
                        Accident.class,
                        "date",
                        "borough",
                        "killed",
                        "factor1",
                        "factor2",
                        "factor3",
                        "factor4",
                        "factor5");

        Preconditions.checkNotNull(csv, "Input DataSet should not be null.");

        query1(csv);
        query2(csv);
        query3(env, csv);

        //env.execute();

		long stop = System.nanoTime();
		System.out.printf("timens:total:%d", stop-start);
    }
}
