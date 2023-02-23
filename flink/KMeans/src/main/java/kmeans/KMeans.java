package kmeans;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class KMeans {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        final ParameterTool params = ParameterTool.fromArgs(args);

        String path;
        if (params.has("points")) {
			path = params.get("points");
		} else {
			System.out.println("Missing --points");
			return;
        }

        int centroids_n;
        if (params.has("n")) {
			centroids_n = Integer.parseInt(params.get("n"));
		} else {
			System.out.println("Missing --n (number of centroids)");
			return;
        }
        System.out.println(centroids_n);

        int num_iter;
        if (params.has("iter")) {
			num_iter = Integer.parseInt(params.get("iter"));
		} else {
			System.out.println("Missing --iter");
			return;
        }
        System.out.println(centroids_n);

        long start = System.nanoTime();

        DataSet<Tuple2<Double, Double>> dataset = env.readCsvFile(path).types(Double.class, Double.class);
        List<Tuple2<Double, Double>> firstNPoints =  readFirstNPoints(path, centroids_n);

        DataSet<Centroid> centroids = env.fromCollection(initialCentroids(centroids_n, firstNPoints));
        // Set maximum number of iterations
        IterativeDataSet<Centroid> loop = centroids.iterate(num_iter);

        DataSet<Centroid> new_centroids = dataset
                // Add to each point his nearest centroid
                .map(new SelectNearestCenter()).withBroadcastSet(loop, "centroids")
                .returns(TypeInformation.of(new TypeHint<Tuple2<Centroid, Centroid>>() {
                }))
                // Group by the old centroids
                .groupBy(0)
                // Sum up new centroids coordinates
                .reduce(new CentroidAccumulator())
                // Divide the coordinates by N to get the new centroids and reset N to 0 for the following iteration
                .map(t -> {
                    t.f1.setX(t.f1.getX() / t.f1.getN());
                    t.f1.setY(t.f1.getY() / t.f1.getN());
                    t.f1.setN(0);
                    return t.f1;
                });


        DataSet<Centroid> finalCentroids = loop.closeWith(new_centroids);

		DataSet<Tuple2<Centroid, Centroid>> clusteredPoints = dataset
            // assign points to final clusters
            .map(new SelectNearestCenter()).withBroadcastSet(finalCentroids, "centroids");


        Long count = clusteredPoints.count();
        System.out.println("NPoints "+count);

        long stop = System.nanoTime();
        System.out.printf("timens:total:%d\n", stop - start);
    }

    public static final class CentroidAccumulator implements ReduceFunction<Tuple2<Centroid, Centroid>> {

		@Override
		public Tuple2<Centroid, Centroid> reduce(Tuple2<Centroid, Centroid> t1, Tuple2<Centroid, Centroid> t2) {
            t1.f1.setX(t1.f1.getX() + t2.f1.getX());
            t1.f1.setY(t1.f1.getY() + t2.f1.getY());
            t1.f1.setN(t1.f1.getN() + t2.f1.getN());

			return  t1;
		}
    }

    private static List<Tuple2<Double, Double>> readFirstNPoints(String path, int n) {
        List<Tuple2<Double, Double>> points = new ArrayList<>();
        try{
            BufferedReader br = Files.newBufferedReader(Paths.get(path), StandardCharsets.UTF_8);

            for (int i = 0; i < n; i++) {
                String[] point = br.readLine().split(",");
                points.add(new Tuple2<Double,Double>(new Double(point[0]), new Double(point[1])));
            }
            br.close();
        }catch (Exception e){
            System.out.println("Exception readFirstNPoints "+e);
        }
        return points;
    }

    private static ArrayList<Centroid> initialCentroids(int centroidsNum, List<Tuple2<Double, Double>> firstNPoints) {
        //System.out.println("Generating initial centroids");
        ArrayList<Centroid> centroids_list = new ArrayList<>();
        for (int i = 0; i < centroidsNum; i++) {
            Centroid tmp_centroid = new Centroid(
                    firstNPoints.get(i).f0,
                    firstNPoints.get(i).f1,
                    i);
            //System.out.println(tmp_centroid);
            centroids_list.add(tmp_centroid);
        }
        return centroids_list;
    }
}
