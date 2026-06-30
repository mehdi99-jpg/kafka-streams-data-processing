package ma.enset.exercise2;

import ma.enset.exercise2.model.WeatherAverage;
import ma.enset.exercise2.model.WeatherRecord;
import ma.enset.exercise2.serde.JsonSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Properties;

/**
 * Exercise 2: Meteorological Data Analyzer
 * 
 * Educational Objectives:
 * 1. Handling stateful operations in Kafka Streams (aggregations).
 * 2. Implementing custom models (POJOs) for structured data mapping.
 * 3. Handling custom JSON serialization using a generic Serde helper.
 * 4. Enhancing data resilience (try-catch within map/flatmap to filter out malformed lines).
 * 5. Grouping streams by key and maintaining aggregations in local RocksDB stores.
 */
public class WeatherAnalyzerApp {

    static {
        // Force slf4j-simple to output to System.out instead of System.err to avoid red text in consoles
        System.setProperty("org.slf4j.simpleLogger.logFile", "System.out");
    }

    private static final Logger log = LoggerFactory.getLogger(WeatherAnalyzerApp.class);

    // Topics to use
    public static final String INPUT_TOPIC = "weather-data";
    public static final String OUTPUT_TOPIC = "station-averages";

    public static void main(String[] args) {
        log.info("Starting Exercise 2: Kafka Streams Weather Data Analyzer Application...");

        // 1. Properties configuration
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "weather-analyzer-app");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        
        // Default String Serdes since we read text lines initially
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        // 2. Build the topology
        StreamsBuilder builder = new StreamsBuilder();

        // Consume the raw comma-separated records as a stream of strings
        KStream<String, String> rawWeatherData = builder.stream(INPUT_TOPIC);

        // Parse and validate raw data to WeatherRecord POJOs with try-catch logic
        KStream<String, WeatherRecord> parsedRecords = rawWeatherData.flatMapValues(value -> {
            try {
                if (value == null || value.trim().isEmpty()) {
                    return Collections.emptyList();
                }

                // Split record by comma: station,temperature,humidity
                String[] parts = value.split(",");
                if (parts.length != 3) {
                    log.warn("Malformed record ignored (incorrect field count): '{}'", value);
                    return Collections.emptyList();
                }

                String station = parts[0].trim();
                double temperature = Double.parseDouble(parts[1].trim());
                double humidity = Double.parseDouble(parts[2].trim());

                WeatherRecord record = new WeatherRecord(station, temperature, humidity);
                log.info("Successfully parsed: {}", record);
                return Collections.singletonList(record);

            } catch (NumberFormatException nfe) {
                log.error("Failed to parse temperature or humidity values as numbers. Line ignored: '{}'. Error: {}", value, nfe.getMessage());
                return Collections.emptyList();
            } catch (Exception e) {
                log.error("Failed to parse line. Line ignored: '{}'. Error: {}", value, e.getMessage());
                return Collections.emptyList();
            }
        });

        // Apply filtering: Keep records where temperature is > 30 degrees Celsius
        KStream<String, WeatherRecord> hotWeatherRecords = parsedRecords.filter((key, record) -> {
            boolean isHot = record.getTemperature() > 30.0;
            if (!isHot) {
                log.info("Filtering out record (temp <= 30°C): {}", record);
            }
            return isHot;
        });

        // Convert temperatures from Celsius to Fahrenheit: F = (C * 1.8) + 32
        KStream<String, WeatherRecord> fahrenheitRecords = hotWeatherRecords.mapValues(record -> {
            double celsius = record.getTemperature();
            double fahrenheit = (celsius * 1.8) + 32.0;
            record.setTemperature(fahrenheit);
            log.info("Converted temp to Fahrenheit: {}°C -> {}°F for station {}", celsius, fahrenheit, record.getStation());
            return record;
        });

        // Group records by station. This is required because we are calculating averages per station.
        // Selecting key changes key to station name, requiring us to specify key and value Serdes.
        KGroupedStream<String, WeatherRecord> groupedByStation = fahrenheitRecords.groupBy(
                (key, record) -> record.getStation(),
                Grouped.with(Serdes.String(), new JsonSerde<>(WeatherRecord.class))
        );

        // Perform stateful aggregation to calculate running averages
        KTable<String, WeatherAverage> averagesTable = groupedByStation.aggregate(
                // Initializer: Create a new WeatherAverage object
                WeatherAverage::new,
                // Aggregator: Accumulate the measurements into the running average object
                (stationName, record, average) -> {
                    if (average.getStation() == null) {
                        average.setStation(stationName);
                    }
                    return average.add(record.getTemperature(), record.getHumidity());
                },
                // Materialize state store using custom Serdes
                Materialized.<String, WeatherAverage, KeyValueStore<Bytes, byte[]>>as("weather-averages-store")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(new JsonSerde<>(WeatherAverage.class))
        );

        // Map KTable updates to string representation and output to Kafka
        averagesTable.toStream()
                .mapValues(WeatherAverage::toString)
                .peek((station, averageStr) -> log.info("Published average: {}", averageStr))
                .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

        // Build and display topology
        Topology topology = builder.build();
        log.info("Topology built successfully:\n{}", topology.describe());

        // 3. Start streams client
        KafkaStreams streams = new KafkaStreams(topology, config);

        // Shutdown Hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutdown hook triggered. Closing Kafka Streams Weather Analyzer...");
            streams.close();
            log.info("Kafka Streams Weather Analyzer closed.");
        }));

        streams.start();
        log.info("Kafka Streams Weather Analyzer started. Press Ctrl+C to stop.");
    }
}
