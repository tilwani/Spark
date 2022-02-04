package lsda_assignment_3;
import java.util.*;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Console;

// using the serializable interface to enable serialization of the class objects and non-static members
public class WeatherStation_Q1 implements java.io.Serializable {
	private String city;
	private List<Measurement> measurements = new ArrayList<Measurement>();
	// list of stations being static, common across all objects
	private static List<WeatherStation_Q1> stations = new ArrayList<WeatherStation_Q1>();
	
	WeatherStation_Q1(String city) {
		this.city = city;
		// adding the created object to stations
		stations.add(this);
	}
	
	public void add_Measurement(int time, double temp) {
		measurements.add(new Measurement(time, temp));
	}
	
	// getters for measurements and city of the current instance
	public List<Measurement> getMeasurements() {
		return measurements;
	}
	
	public String getCity() {
		return city;
	}
	
	// function to calculate temperatures between [t-1, t+1] across all stations
	public static long countTemperature(double t) {
		// defining the configuration for spark
		SparkConf sparkConf = new SparkConf()
						.setAppName("countTemperature")
						.setMaster("local[4]").set("spark.executer.memory", "1.5g");
				
		// creating sparkContext
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		
		// parallelizing the stations 
		JavaRDD<WeatherStation_Q1> rddStations = sc.parallelize(stations);
		// obtaining all the measurements using flatmap
		JavaRDD<Measurement> rddMeasurements = rddStations.flatMap(
				(WeatherStation_Q1 ws) -> ws.getMeasurements().iterator());
		// counting using the closures that keep lambda along with the binding to variable t
		long temp_count = rddMeasurements.filter(
						(Measurement m) -> m.getTemperature() >= (t - 1) && m.getTemperature() <= (t + 1))
						.count();
		sc.stop();
		sc.close();
		return temp_count;
	}
	
	
	public static void main(String args[]) {
		System.setProperty("hadoop.home.dir", "C:/winutils");
		
		// creation of two weather stations with city names
		WeatherStation_Q1 wsGalway = new WeatherStation_Q1("Galway");
		WeatherStation_Q1 wsDublin = new WeatherStation_Q1("Dublin");
		
		// adding measurements to both the instances
		wsGalway.add_Measurement(200, 20.0);
		wsGalway.add_Measurement(400, 11.7);
		wsGalway.add_Measurement(100, -5.4);
		wsGalway.add_Measurement(300, 18.7);
		wsGalway.add_Measurement(450, 20.9);
		wsDublin.add_Measurement(350, 8.4);
		wsDublin.add_Measurement(300, 19.2);
		wsDublin.add_Measurement(450, 7.2);
		
		double tempOfIntrest = 20;
		String output = String.format("For t = %.2f, number of measurements in interval [%.2f, %.2f] are: %d",
				tempOfIntrest, tempOfIntrest-1, tempOfIntrest+1, WeatherStation_Q1.countTemperature(tempOfIntrest));
		System.out.println(output);
	}
}
