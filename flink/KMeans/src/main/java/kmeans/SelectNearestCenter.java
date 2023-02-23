package kmeans;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

import java.util.Collection;

/**
 * Calculate the nearest centroid for each point
 */
public class SelectNearestCenter extends RichMapFunction<Tuple2<Double, Double>, Tuple2<Centroid, Centroid>> {
    private Collection<Centroid> centroids;     // collection of the centroids

    /**
     * Gives access to the RuntimeContext and setup the centroids
     * @param parameters are the configuration parameters
     */
    @Override
    public void open(Configuration parameters) {
        this.centroids = getRuntimeContext().getBroadcastVariable("centroids");
    }

    /**
     * Map a point to its nearest centroid
     * @param p is the point to be mapped
     * @return a tuple <Centroid,Centroid> in which the first field contains the nearest centroid found and the second field contains the point analyzed
     */
    @Override
    public Tuple2<Centroid, Centroid> map(Tuple2<Double, Double> p) {
        double min_distance = Double.MAX_VALUE;
        Centroid min_centroid = null;

        for (Centroid centroid : centroids) {
            double d = distance(centroid, p);
            if (d < min_distance) {
                min_distance = d;
                min_centroid = centroid;
            }
        }

        return Tuple2.of(min_centroid, new Centroid(p.f0, p.f1, min_centroid.getId()));
    }

    /**
     * Calculate the distance from a centroid to a point
     * @param centroid
     * @param point
     * @return the distance
     */
    private static double distance(Centroid centroid, Tuple2<Double, Double> point) {
        return Math.sqrt(Math.pow(centroid.getX() - point.f0, 2) + Math.pow(centroid.getY() - point.f1, 2));
    }
}