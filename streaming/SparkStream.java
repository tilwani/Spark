package lsda_assignment_5;

// importing required libraries
import java.util.Arrays;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;
import scala.Tuple2;
import org.apache.spark.api.java.JavaPairRDD;
public class SparkStream {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		System.setProperty("hadoop.home.dir", "C:/winutils");
		SparkConf sparkConf = new SparkConf()
				.setAppName("tweetTrends")
				.setMaster("local[4]").set("spark.executer.memory", "2g");
		// creating StreamingContext
		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(1));
		
		// connecting to stream on port 9999, reading as stream and caching the result
		JavaDStream<String> twoots = jssc.socketTextStream("localhost", 9999).cache();
		
		// answer to question 1, printing4 elements of each RDD
		twoots.print(4);
		
		// keeping info of all words, to be used to calculate count
		// window of 5 seconds and slide of 2 seconds used
		JavaDStream<String> allWords =twoots
				.window(Durations.seconds(5), Durations.seconds(2))
				.flatMap((String x) -> Arrays.asList(x.split(" ")).iterator()).cache(); 
		
		// filtering usernames and hashtags as relevant keyWords
		JavaDStream<String> keyWords = allWords.filter((String x) -> x.startsWith("@") || x.startsWith("#")).cache();
		// calculating the words by value, obtained from windowed stream
		// cached as this will be required to calculate frequencies of relevant keywords (usernames and hashtags)
		JavaPairDStream<String, Long> wordCount = keyWords.countByValue().cache();
		// answer to question 2, printing the keywords by their values
		wordCount.print(4);
		
		// answer to qustion 3, calculating and printing frequent hashtags from keywords
		JavaPairDStream<String, Long> freqHashtag = keyWords
				.filter((String x) -> x.startsWith("#")).countByValue().transformToPair(
						(JavaPairRDD<String, Long> x) -> x.sortByKey());
		freqHashtag.print(4);
		
		// calculating frequencies
		// wordCount is used to reduce relevant words into pairs of ("word", (String word, Long count))
		JavaPairDStream<String, Tuple2> keyedWords = wordCount.mapToPair(
				(Tuple2<String, Long> x) -> new Tuple2<String, Tuple2>("word", x));
		
		// allWords is used to reduce total words in to pairs ("word", 1) and then reducing by key to get count
		JavaPairDStream<String, Long> totalWordCount = allWords.mapToPair(
				(String x) -> new Tuple2<String, Long>("word", 1L)).reduceByKey((x, y) -> x + y);
		
		// both the above are joined over key "word" and combined
		// by dividing the keyedWord counts present in keyedWords by the total word count in totalWordCount
		keyedWords.join(totalWordCount).mapToPair(
				(Tuple2<String, Tuple2<Tuple2,Long>> x) -> 
				new Tuple2<String, Float>(x._2()._1()._1().toString(), 
						Float.parseFloat(x._2()._1()._2().toString())/x._2()._2())).print(30);
		// above result is printed
		
		// providing checkpoint to streaming context
		jssc.checkpoint(
				"D:\\NUIG_materials\\Sem_1\\LSDA\\assignment\\assignment_5\\checkPointDir");
		
		// starting the streaming application
		jssc.start();
		try {
			jssc.awaitTermination();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
