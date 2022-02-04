import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import scala.Tuple2;

/*
 * Please define the path of the data file in line 34, variable "dataFilePath", 
 * Or keep the file "twitter2D_2.txt" at the location
 *  ...\eclipse_workspace\<project_name_folder>\twitter2D_2.txt 
 */

public class TweetClustering {
	
	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "C:/winutils");
		// defining the configuration for spark
		SparkConf sparkConf = new SparkConf()
						.setAppName("tweetClustering")
						.setMaster("local[4]").set("spark.executer.memory", "1g");
		// creating sparkContext
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		
		// reading the txt file
		String dataFilePath = "twitter2D_2.txt";
		JavaRDD<String> data = sc.textFile(dataFilePath).cache();
		
		// splitting the string to get the corrdinates
		JavaRDD<Vector> coordinates = data.map((String s) -> {
			String[] elements = s.split(",");
			double[] location = {Double.parseDouble(elements[2]), Double.parseDouble(elements[3])};
			return Vectors.dense(location);			
		}).cache();  // caching the RDD as it will be used for clustering
		
		// model prepared with the configuration on 50 iterations
		int n_clusters = 4, n_iterations = 50;
		KMeansModel tweetClusters = KMeans.train(coordinates.rdd(), n_clusters, n_iterations);
		
		// storing the text data with its cluster as key
		JavaPairRDD<Integer, Tuple2<String, Integer>> results = 
				coordinates.zip(data).mapToPair((Tuple2<Vector, String> t) -> {
					String[] elements = t._2().split(",");
					int cluster = tweetClusters.predict(t._1());
					return new Tuple2<Integer, Tuple2<String, Integer>>(cluster, 
							new Tuple2<String, Integer>(elements[1], Integer.parseInt(elements[0])));
				}).sortByKey().cache(); 	
					// sorting by key to print clusters in order as required
		
		// creating rdd to store number of spams in each cluster
		// obtained by taking the labels and reducing(summing) them by key(cluster)
		JavaPairRDD<Integer, Integer> numSpam = results.mapToPair((Tuple2<Integer, Tuple2<String, Integer>> t) 
				-> new Tuple2<Integer, Integer>(t._1(), t._2()._2())).reduceByKey((x, y) -> x + y).sortByKey();
		// sorting by key to print in order of clusters as required
		
		// first part of print, mentioning cluster for each tweet
		for(Tuple2<?,?> res: results.collect())
		    System.out.println("Tweet " + ((Tuple2<String, Integer>)res._2())._1() 
		            		+ " is in cluster " + res._1());
		// second part of results, number of spams in each cluster
		for(Tuple2<?,?> cluster: numSpam.collect())
            System.out.println("Cluster " + cluster._1() + " has " + cluster._2() + " spam tweets");
		
		sc.stop();
		sc.close();
	}
}
