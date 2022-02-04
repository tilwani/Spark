package lsda_assignment_3;

//using the serializable interface to enable serialization of the class objects and non-static members
public class Measurement implements java.io.Serializable {
	
	private int time;
	private double temperature;
	
	// constructor and settters for the measurement
	Measurement(int t, double temp) {
		this.time = t;
		this.temperature = temp;
	}
	
	// getters for time and measurement
	public int getTime() {
		return time;
	}
	
	public double getTemperature() {
		return temperature;
	}
}
