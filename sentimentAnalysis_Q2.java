package lsda_assignment_3;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.mllib.feature.HashingTF;
import org.apache.spark.mllib.feature.IDF;
import org.apache.spark.mllib.feature.IDFModel;
import org.apache.spark.mllib.classification.SVMModel;
import org.apache.spark.mllib.classification.SVMWithSGD;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics;
import scala.Tuple2;
import shapeless.ops.tuple;


/*
 * Please define the data filename in line 98, variable "dataFilePath", 
 * Or keep the file "imdb_labelled.txt" at the location
 *  ...\eclipse_workspace\<project_name_folder>\imdb_labelled.txt 
 */

public class sentimentAnalysis_Q2 {
	
	// function to read the data File and return as string and its label in pairs
	public static JavaPairRDD<String, Integer> readFile(JavaSparkContext sc, String dataFilePath) {
		JavaRDD<List<String>> rddData = sc.textFile(dataFilePath).map((String s) -> Arrays.asList(s.split("\t")));
		JavaPairRDD<String, Integer> dataPoints = rddData.mapToPair(
										(List<String> example) -> 
										new Tuple2<>(example.get(0), Integer.parseInt(example.get(1))));
		return dataPoints;
	}
	
	public static JavaPairRDD<String, Integer> preProcessData(JavaPairRDD<String, Integer> dataPoints) {
		/*
		 * Preprocessing of the dataset (passed as argument) using the following steps:
		 * 1. Replacing all non alphabetic characters with space - i.e. punctuations and symbols
		 * 2. Replacing all multiple spaces with single spaces, as above step might cause multiple spaces at some places.
		 * 3. Removing leading and trailing whitespaces.
		 * 4. Converting the sentences to lowercase
		 * 
		 * Kindly note that the \n and \t occurences can also be removed, but the readme file of dataset specifically defines each data point to be in a single line
		 * 
		 */
		// converting to lowercase after removing punctuation and extra spaces
		JavaPairRDD<String, Integer> processedData = dataPoints.mapToPair(
				(Tuple2<String, Integer> dataPoint) 
				-> new Tuple2<String, Integer>(dataPoint._1.toString().replaceAll("[^A-Za-z ]", " ").
						replaceAll(" +", " ").trim().toLowerCase(), dataPoint._2()));
		
		return processedData;	
	}
	
	// function to return lists of words from the input string, Integer dataset, for further processing and vectorization
	public static JavaRDD<List<String>> getWordLists(JavaPairRDD<String, Integer> dataSet) {
		return dataSet.map(
				(Tuple2<String, Integer> line) -> Arrays.asList(line._1.toString().split(" ")));	
	}
	
	// return input JavaPairRDD as RDD of labeled examples having labels and features, to input in model training
	public static JavaRDD<LabeledPoint> getLabeledData(JavaPairRDD<String, Integer> dataSet, JavaRDD<Vector> tfIdf) {
		return dataSet.zip(tfIdf).map((Tuple2<Tuple2<String, Integer>, Vector> t) 
				-> new LabeledPoint(t._1._2, t._2));
	}
	
	// function to evaluate on testdata using the trained model after preprocessing and vectorizing
	public static JavaPairRDD<Object, Object> evaluateOnData(JavaPairRDD<String, Integer> data, SVMModel model, 
			HashingTF t_freq, IDFModel idf) {
		
		// preprocess and vectorize the incoming data
		JavaPairRDD<String, Integer> processedTestData = preProcessData(data).cache();
		
		JavaRDD<Vector> tfIdf_test = idf.transform(t_freq.transform(getWordLists(processedTestData)));
		JavaRDD<LabeledPoint> testData = getLabeledData(processedTestData, tfIdf_test);
		
		// running model on the given data
		JavaPairRDD<Object, Object> predictions = testData.mapToPair((LabeledPoint lp) 
				-> new Tuple2<Object, Object>(model.predict(lp.features()), lp.label()));
		return predictions;
	}
	
	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "C:/winutils");
		
		// defining the configuration for spark
		SparkConf sparkConf = new SparkConf()
						.setAppName("sentimentAnalysis")
						.setMaster("local[4]").set("spark.executer.memory", "3g");
				
		// creating sparkContext
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		
		// reading the data file and caching for further use in sampling
		String dataFilePath = "imdb_labelled.txt";
		JavaPairRDD<String, Integer> dataPoints = readFile(sc, dataFilePath).cache();
		
		// taking out the training sample from the dataset
		JavaPairRDD<String, Integer> trainingSample = dataPoints.sample(false, 0.62, 0).cache();
		
		// preprocessing on sample of training data
		JavaPairRDD<String, Integer> processedTrainData = preProcessData(trainingSample).cache();
		
		// vectorizing input - calculating tf-idf scores of all sentences in training data
		HashingTF t_freq = new HashingTF();
		// caching as it is being used in next statements
		JavaRDD<Vector> tf_train = t_freq.transform(getWordLists(processedTrainData)).cache();
		IDFModel idf = new IDF().fit(tf_train);
		JavaRDD<Vector> tfIdf_train = idf.transform(tf_train);
		
		
		// keeping the train data in labeled format of truth label and features
		JavaRDD<LabeledPoint> trainData = getLabeledData(processedTrainData, tfIdf_train);
		
		// training of svm model
		int iterations = 50;
		SVMModel svmModel = SVMWithSGD.train(trainData.rdd(), iterations);
		
		// evaluate on remaining data (test-data)
		JavaPairRDD<String, Integer> testingSample = dataPoints.subtract(trainingSample).cache();
		
		// printing few movie test reviews, by taking a small sample of 3% from test data
		JavaPairRDD<String, Integer> printTestSample = testingSample.sample(false, 0.05, 0);
		JavaPairRDD<Object, Object> samplePredictions = evaluateOnData(printTestSample, svmModel, t_freq, idf);
		
		// formatting the sampleResults to print string and its label on console
		JavaPairRDD<String, Object> sampleResults = printTestSample.zip(samplePredictions)
				.mapToPair((Tuple2<Tuple2<String, Integer>, Tuple2<Object, Object>> t) 
						-> new Tuple2<String, Object>(t._1()._1(), t._2()._1()));
		
		// printing the line along with its label
		System.out.println("Predictions of few lines from the test set: ");
		for(Tuple2<String, Object> result: sampleResults.collect())
            System.out.println(result._1 + ": " + result._2);
		
		// predictions and truth labels for the data to be evaluated - testingSample
		JavaPairRDD<Object, Object> predictions = evaluateOnData(testingSample, svmModel, t_freq, idf);
		
		// calculating the required AUROC metric
		BinaryClassificationMetrics metrics = new BinaryClassificationMetrics(predictions.rdd());
		System.out.println("Metric AUROC for the given task of sentiment analysis is: " + metrics.areaUnderROC());
		
		sc.stop();
		sc.close();
	}
}
