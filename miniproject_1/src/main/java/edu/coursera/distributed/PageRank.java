package edu.coursera.distributed;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;

/**
 * A wrapper class for the implementation of a single iteration of the iterative
 * PageRank algorithm.
 */
public final class PageRank {
    /**
     * Default constructor.
     */
    private PageRank() {
    }

    /**
     * TODO Given an RDD of websites and their ranks, compute new ranks for all
     * websites and return a new RDD containing the updated ranks.
     * <p>
     * Recall from lectures that given a website B with many other websites
     * linking to it, the updated rank for B is the sum over all source websites
     * of the rank of the source website divided by the number of outbound links
     * from the source website. This new rank is damped by multiplying it by
     * 0.85 and adding that to 0.15. Put more simply:
     * <p>
     * new_rank(B) = 0.15 + 0.85 * sum(rank(A) / out_count(A)) for all A linking to B
     * <p>
     * For this assignment, you are responsible for implementing this PageRank
     * algorithm using the Spark Java APIs.
     * <p>
     * The reference solution of sparkPageRank uses the following Spark RDD
     * APIs. However, you are free to develop whatever solution makes the most
     * sense to you which also demonstrates speedup on multiple threads.
     * <p>
     * 1) JavaPairRDD.join
     * 2) JavaRDD.flatMapToPair
     * 3) JavaPairRDD.reduceByKey
     * 4) JavaRDD.mapValues
     *
     * @param sites The connectivity of the website graph, keyed on unique
     *              website IDs.
     * @param ranks The current ranks of each website, keyed on unique website
     *              IDs.
     * @return The new ranks of the websites graph, using the PageRank
     * algorithm to update site ranks.
     */
    public static JavaPairRDD<Integer, Double> sparkPageRank(
            final JavaPairRDD<Integer, Website> sites,
            final JavaPairRDD<Integer, Double> ranks) {

        JavaPairRDD<Integer, Double> newRanks = sites
                .join(ranks)
                .flatMapToPair(kv -> {
                    Tuple2<Website, Double> websiteRankPair = kv._2();
                    Website website = websiteRankPair._1();
                    Double currentWebsiteRank = websiteRankPair._2();
                    Iterator<Integer> edgesIterator = website.edgeIterator();
                    List<Tuple2<Integer, Double>> contribs = new LinkedList<>();
                    while (edgesIterator.hasNext()) {
                        Integer edgeWebsiteId = edgesIterator.next();
                        double rank = currentWebsiteRank / website.getNEdges();
                        Tuple2<Integer, Double> e = new Tuple2(edgeWebsiteId, rank);
                        contribs.add(e);
                    }
                    return contribs;
                });
        return newRanks.reduceByKey((k1, k2) -> k1 + k2).mapValues(v -> 0.15 + 0.85 * v);
    }
}
