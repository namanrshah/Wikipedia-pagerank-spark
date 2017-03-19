package proj.pagerank.wiki.spark;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.graphx.Pregel;
import org.apache.spark.graphx.lib.PageRank;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

/**
 *
 * @author namanrs
 */
public class WikipediaPageRank {

    /**
     * @param args the command line arguments
     */
    public static float beta = 0.85f;
    public static float oneMinusBeta = (float) 1 - beta;

    public static void main(String[] args) throws IOException {
        SparkConf sparkConf = new SparkConf().setAppName("Spark_wikipedia_pagerank");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        Configuration hadoopConfiguration = jsc.hadoopConfiguration();

        FileSystem fileSystemForsparkExample = FileSystem
                .get(hadoopConfiguration);
        String tempOutputPath = "hdfs://dover:42311/cs535/Fall16/PA1/Data/Output/temp";
        if (fileSystemForsparkExample.exists(new Path(tempOutputPath))) {
            fileSystemForsparkExample.delete(new Path(tempOutputPath), true);
        }

        //read from-to file
        JavaRDD<String> textFile = jsc.textFile("hdfs://dover:42311/cs535/Fall16/PA1/Data/Input/links-simple-sorted.txt");
//        JavaRDD<String> textFile = jsc.textFile("hdfs://dover:42311/cs535/Fall16/PA1/Data/Input/pr_test");
        textFile = textFile.cache();

//        JavaPairRDD<Long, String> zipWithIndex = jsc.textFile("hdfs://dover:42311/cs535/Fall16/PA1/Data/Input/titles-sorted.txt").zipWithIndex().mapToPair(new PairFunction<Tuple2<String, Long>, Long, String>() {
//
//            @Override
//            public Tuple2<Long, String> call(Tuple2<String, Long> t) throws Exception {
//                return new Tuple2<>(t._2 + 1, t._1);
//            }
//        });
//        zipWithIndex.saveAsTextFile(tempOutputPath);
        //From-Tos structure
        //Pattern <from, <tos>>
        JavaPairRDD<String, List<String>> fromToRDD = textFile.mapToPair(new PairFunction<String, String, List<String>>() {

            @Override
            public Tuple2<String, List<String>> call(String t) throws Exception {
                String[] fromAndTo = t.split(":");
                String[] tos = fromAndTo[1].trim().split(" ");
                List<String> tosList = Arrays.asList(tos);
                return new Tuple2<>(fromAndTo[0].trim(), tosList);
            }
        });

        JavaRDD<String> linksWithOutputEdges = fromToRDD.keys();

        JavaRDD<String> linksWithInputEdges = textFile.flatMapToPair((String t) -> {
            List<Tuple2<String, String>> intermediateOP = new ArrayList<>();
            String[] fromAndTo = t.split(":");
            String[] tos = fromAndTo[1].trim().split(" ");
            for (String to : tos) {
                intermediateOP.add(new Tuple2<>(to, fromAndTo[0].trim()));
            }
            return intermediateOP;
        }).reduceByKey(new Function2<String, String, String>() {

            @Override
            public String call(String t1, String t2) throws Exception {
                return "";
            }
        }).keys();
        long linksWithInputCount = linksWithInputEdges.count();

        JavaRDD<String> deadEnds = linksWithInputEdges.subtract(linksWithOutputEdges);
        long deadEndsCount = deadEnds.count();
//        fromToRDD = fromToRDD.cache();
//        fromToRDD = fromToRDD.persist(StorageLevel.MEMORY_AND_DISK());
//        fromToRDD.saveAsTextFile(tempOutputPath);
        //RDD containing pagewise outputlink count
        //Pattern <from, tos_count>
        JavaPairRDD<String, Integer> pagewiseLinkCountRDD = fromToRDD.mapValues((List<String> t1) -> {
            return t1.size();
        });
        pagewiseLinkCountRDD = pagewiseLinkCountRDD.cache();
//        pagewiseLinkCountRDD.saveAsTextFile(tempOutputPath);

        long totalNoOfLinks = fromToRDD.count();
//
        JavaPairRDD<String, Tuple2<Float, Float>> pageRanks = fromToRDD.mapValues((List<String> t1) -> {
            float initPageRank = (float) 1 / totalNoOfLinks;
            return new Tuple2<>(initPageRank, initPageRank);
        });

        JavaPairRDD<String, Tuple2<Float, Float>> pageRanksInit = fromToRDD.mapValues((List<String> t1) -> {
            float initPageRank = (float) 1 / totalNoOfLinks;
            return new Tuple2<>(initPageRank, initPageRank);
        });
//        pageRanksInit = pageRanksInit.cache();

        //compute join for outlinks and count
        JavaPairRDD<String, Tuple2<List<String>, Integer>> outlinksAndCountJoin = fromToRDD.join(pagewiseLinkCountRDD, 120);
        outlinksAndCountJoin = outlinksAndCountJoin.cache();
        JavaPairRDD<String, Tuple2<Float, Float>> noInLinkPagesRDD = null;
        //iterations
        for (int i = 0; i < 25; i++) {
            JavaPairRDD<String, Tuple2<Float, Float>> contributions = outlinksAndCountJoin.join(pageRanks).values().flatMapToPair((Tuple2<Tuple2<List<String>, Integer>, Tuple2<Float, Float>> t) -> {
                //<<link, <contrib_from_idealize_pr, contrib_from_taxation_pr>>>
                List<Tuple2<String, Tuple2<Float, Float>>> contribParts = new ArrayList<>();
                List<String> outlinks = t._1._1;
                int outLinksCount = t._1._2;
                float idealizePageRank = t._2._1;
                float taxationPageRank = t._2._2;
                for (String outlink : outlinks) {
                    contribParts.add(new Tuple2<>(outlink, new Tuple2<>((float) idealizePageRank / outLinksCount, (float) taxationPageRank / outLinksCount)));
                }
                return contribParts;
            });

            pageRanks = contributions.reduceByKey((Tuple2<Float, Float> t1, Tuple2<Float, Float> t2) -> new Tuple2<>(t1._1 + t2._1, t1._2 + t2._2)).mapValues((Tuple2<Float, Float> t1) -> {
                return new Tuple2<>(t1._1, beta * (t1._2) + oneMinusBeta * (1.0f / totalNoOfLinks)); //
            });

            //Taxation pagerank for links with no input links
            JavaPairRDD<String, Tuple2<Float, Float>> noInLinkPages = pageRanksInit.subtractByKey(pageRanks);
            noInLinkPagesRDD = noInLinkPages.mapValues((Tuple2<Float, Float> t1) -> new Tuple2<>(0f, (float) oneMinusBeta * 1 / totalNoOfLinks));
            pageRanks = pageRanks.union(noInLinkPagesRDD);
        }
        //titles
        JavaRDD<String> titlesRDD = jsc.textFile("hdfs://dover:42311/cs535/Fall16/PA1/Data/Input/titles-sorted.txt");
        JavaPairRDD<String, String> titlesWithIdRDD = titlesRDD.zipWithIndex().mapToPair(new PairFunction<Tuple2<String, Long>, String, String>() {

            @Override
            public Tuple2<String, String> call(Tuple2<String, Long> t) throws Exception {
                return new Tuple2<>(Long.toString(t._2 + 1), t._1);
            }
        });

        pageRanks = JavaPairRDD.fromJavaRDD(titlesWithIdRDD.join(pageRanks).values());

//        pageRanks.saveAsTextFile(tempOutputPath);
        JavaPairRDD<Float, String> swappedRDDForIdeaPRs = pageRanks.mapToPair((Tuple2<String, Tuple2<Float, Float>> t) -> new Tuple2<>(t._2._1, t._1));
        JavaPairRDD<Float, String> swappedSortedRDDForIdeaPRs = swappedRDDForIdeaPRs.sortByKey(false);
        JavaPairRDD<String, Float> sortedIdealPRs = swappedSortedRDDForIdeaPRs.mapToPair((Tuple2<Float, String> t) -> t.swap());

        JavaPairRDD<Float, String> swappedRDDForTaxationPRs = pageRanks.mapToPair((Tuple2<String, Tuple2<Float, Float>> t) -> new Tuple2<>(t._2._2, t._1));
        JavaPairRDD<Float, String> swappedSortedRDDForTaxationPRs = swappedRDDForTaxationPRs.sortByKey(false);
        JavaPairRDD<String, Float> sortedTaxationPRs = swappedSortedRDDForTaxationPRs.mapToPair((Tuple2<Float, String> t) -> t.swap());

        //merge titles and ideal PageRanks
//        sortedIdealPRs = JavaPairRDD.fromJavaRDD(titlesWithIdRDD.join(sortedIdealPRs).values());
//        sortedIdealPRs = titlesWithIdRDD.join(sortedIdealPRs).values().mapToPair(new PairFunction<Tuple2<String, Float>, String, Float>() {
//
//            @Override
//            public Tuple2<String, Float> call(Tuple2<String, Float> t) throws Exception {
//                return new Tuple2<>(t._1, t._2);
//            }
//        });
        //merge titles and taxation PageRanks
//        sortedTaxationPRs = JavaPairRDD.fromJavaRDD(titlesWithIdRDD.join(sortedTaxationPRs).values());
//        sortedTaxationPRs = titlesWithIdRDD.join(sortedTaxationPRs).values().mapToPair(new PairFunction<Tuple2<String, Float>, String, Float>() {
//
//            @Override
//            public Tuple2<String, Float> call(Tuple2<String, Float> t) throws Exception {
//                return new Tuple2<>(t._1, t._2);
//            }
//        });
        String outputPathIdeal = "hdfs://dover:42311/cs535/Fall16/PA1/Data/Output/ideal";
        String outputPathTaxation = "hdfs://dover:42311/cs535/Fall16/PA1/Data/Output/taxation";
//        String outputPath = "hdfs://denver:44311/SYMPHONY/RCA/Data/Colorado/tmp";
//        String outputPath = "hdfs://helena:45311/SYMPHONY/RCA/Data/Colorado/tmp";
        if (fileSystemForsparkExample.exists(new Path(outputPathIdeal))) {
            fileSystemForsparkExample.delete(new Path(outputPathIdeal), true);
        }
        sortedIdealPRs.saveAsTextFile(outputPathIdeal);

        if (fileSystemForsparkExample.exists(new Path(outputPathTaxation))) {
            fileSystemForsparkExample.delete(new Path(outputPathTaxation), true);
        }
        sortedTaxationPRs.saveAsTextFile(outputPathTaxation);

        //stats
        JavaRDD<String> textFilesForIdealPR = jsc.textFile("hdfs://dover:42311/cs535/Fall16/PA1/Data/Output/ideal/*");
        long idealPRCount = textFilesForIdealPR.count();
        JavaRDD<String> textFilesForTaxPR = jsc.textFile("hdfs://dover:42311/cs535/Fall16/PA1/Data/Output/taxation/*");
        long taxPRCount = textFilesForTaxPR.count();
        ArrayList<Tuple2<String, String>> wikiPRStats = new ArrayList<Tuple2<String, String>>();
        wikiPRStats.add(new Tuple2<>("", "NODE_WITH_IDEAL_PR_COUNT" + Long.toString(idealPRCount)));
        wikiPRStats.add(new Tuple2<>("", "NODE_WITH_TAXATION_PR_COUNT" + Long.toString(taxPRCount)));
        wikiPRStats.add(new Tuple2<>("", "NODE_COUNT" + Long.toString(totalNoOfLinks)));
        wikiPRStats.add(new Tuple2<>("", "NO_INLINK_NODE_COUNT" + Long.toString(noInLinkPagesRDD.count())));
        JavaPairRDD<String, Float> sumOfIdealPR = sortedIdealPRs.mapToPair((Tuple2<String, Float> t) -> new Tuple2<>("1", t._2)).reduceByKey((Float t1, Float t2) -> t1 + t2);
        wikiPRStats.add(new Tuple2<>("", "IDEAL_PR_SUM" + Float.toString(sumOfIdealPR.first()._2)));
        JavaPairRDD<String, Float> sumOfTaxationPR = sortedTaxationPRs.mapToPair((Tuple2<String, Float> t) -> new Tuple2<>("1", t._2)).reduceByKey((Float t1, Float t2) -> t1 + t2);
        wikiPRStats.add(new Tuple2<>("", "TAX_PR_SUM" + Float.toString(sumOfTaxationPR.first()._2)));

        wikiPRStats.add(new Tuple2<>("", "DEAD_END_COUNTS = " + Long.toString(deadEndsCount) + " " + Long.toString(linksWithInputCount) + " " + Long.toString(deadEndsCount + totalNoOfLinks)));

        JavaRDD<Tuple2<String, String>> parallelizeHerdWithPRCount = jsc.parallelize(wikiPRStats);
        parallelizeHerdWithPRCount.saveAsTextFile(tempOutputPath);
    }

}
//If we consider literacy as people who attended school, then town dwellers make up the most literate subsistence group. More than 70% of people in this group have attended school (primary schooling + at least middle school) and the rest are illiterate. The shifting cultivators group is the least literate. Nearly 67% of those are illiterate and only 33% have attended school. Moreover, settled agriculturists is somewhere in the middle. It does not have the highest or lowest level of literacy. Nonetheless, it has the lowest percentage of people who studied until primary school. 
