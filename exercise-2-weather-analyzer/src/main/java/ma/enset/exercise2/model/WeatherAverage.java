package ma.enset.exercise2.model;

/**
 * POJO used as an aggregator in Kafka Streams to calculate
 * running averages of temperature and humidity per station.
 */
public class WeatherAverage {
    private String station;
    private double tempSum;
    private long tempCount;
    private double tempAverage;
    
    private double humiditySum;
    private long humidityCount;
    private double humidityAverage;

    // Default constructor for Jackson JSON Deserialization
    public WeatherAverage() {
    }

    public WeatherAverage(String station) {
        this.station = station;
        this.tempSum = 0.0;
        this.tempCount = 0;
        this.tempAverage = 0.0;
        this.humiditySum = 0.0;
        this.humidityCount = 0;
        this.humidityAverage = 0.0;
    }

    /**
     * Accumulates a new weather record into the average calculations.
     */
    public WeatherAverage add(double temperature, double humidity) {
        this.tempSum += temperature;
        this.tempCount++;
        this.tempAverage = this.tempSum / this.tempCount;

        this.humiditySum += humidity;
        this.humidityCount++;
        this.humidityAverage = this.humiditySum / this.humidityCount;

        return this;
    }

    // Getters and Setters
    public String getStation() {
        return station;
    }

    public void setStation(String station) {
        this.station = station;
    }

    public double getTempSum() {
        return tempSum;
    }

    public void setTempSum(double tempSum) {
        this.tempSum = tempSum;
    }

    public long getTempCount() {
        return tempCount;
    }

    public void setTempCount(long tempCount) {
        this.tempCount = tempCount;
    }

    public double getTempAverage() {
        return tempAverage;
    }

    public void setTempAverage(double tempAverage) {
        this.tempAverage = tempAverage;
    }

    public double getHumiditySum() {
        return humiditySum;
    }

    public void setHumiditySum(double humiditySum) {
        this.humiditySum = humiditySum;
    }

    public long getHumidityCount() {
        return humidityCount;
    }

    public void setHumidityCount(long humidityCount) {
        this.humidityCount = humidityCount;
    }

    public double getHumidityAverage() {
        return humidityAverage;
    }

    public void setHumidityAverage(double humidityAverage) {
        this.humidityAverage = humidityAverage;
    }

    @Override
    public String toString() {
        return String.format("%s : Temperature moyenne = %.1f F , Humidite moyenne = %.1f %%",
                station, tempAverage, humidityAverage);
    }
}
