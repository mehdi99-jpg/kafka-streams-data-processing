package ma.enset.exercise2.model;

/**
 * POJO representing a single weather measurement from a station.
 */
public class WeatherRecord {
    private String station;
    private double temperature; // in Celsius initially, then converted to Fahrenheit
    private double humidity;

    // Default constructor for Jackson JSON Deserialization
    public WeatherRecord() {
    }

    public WeatherRecord(String station, double temperature, double humidity) {
        this.station = station;
        this.temperature = temperature;
        this.humidity = humidity;
    }

    public String getStation() {
        return station;
    }

    public void setStation(String station) {
        this.station = station;
    }

    public double getTemperature() {
        return temperature;
    }

    public void setTemperature(double temperature) {
        this.temperature = temperature;
    }

    public double getHumidity() {
        return humidity;
    }

    public void setHumidity(double humidity) {
        this.humidity = humidity;
    }

    @Override
    public String toString() {
        return "WeatherRecord{" +
                "station='" + station + '\'' +
                ", temperature=" + temperature +
                ", humidity=" + humidity +
                '}';
    }
}
