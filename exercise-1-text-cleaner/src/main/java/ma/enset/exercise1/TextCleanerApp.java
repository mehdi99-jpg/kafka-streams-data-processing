package ma.enset.exercise1;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Exercise 1: Text Cleaner and Validator
 * 
 * Educational Objectives:
 * 1. Setting up basic Kafka Streams configuration properties.
 * 2. Creating a stateless stream processing topology.
 * 3. Applying stateless transformations: mapValues (trim, replace whitespace, uppercase).
 * 4. Conditional branching (routing clean messages to one topic and rejected messages to another).
 * 5. Implementing a clean shutdown hook for applications.
 */
public class TextCleanerApp {

    static {
        // Force slf4j-simple to output to System.out instead of System.err to avoid red text in consoles
        System.setProperty("org.slf4j.simpleLogger.logFile", "System.out");
    }

    private static final Logger log = LoggerFactory.getLogger(TextCleanerApp.class);

    // Topics to use
    public static final String INPUT_TOPIC = "text-input";
    public static final String CLEAN_TOPIC = "text-clean";
    public static final String DEAD_LETTER_TOPIC = "text-dead-letter";

    public static void main(String[] args) {
        log.info("Starting Exercise 1: Kafka Streams Text Cleaner Application...");

        // 1. Configuration properties
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "text-cleaner-app");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        
        // Define default key and value Serdes. Since this is text processing, we use StringSerde.
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        // 2. Build the topology
        StreamsBuilder builder = new StreamsBuilder();
        
        // Read raw text messages as a stream (KStream) from the input topic
        KStream<String, String> textInput = builder.stream(INPUT_TOPIC);

        // Clean each message using stateless mapValues
        KStream<String, String> cleanedInput = textInput.mapValues(value -> {
            if (value == null) {
                return "";
            }
            
            // Apply cleaning requirements:
            // a. Trim leading and trailing whitespaces
            // b. Replace multiple spaces with a single space using regex
            // c. Convert entire text to uppercase
            String cleaned = value.trim()
                                  .replaceAll("\\s+", " ")
                                  .toUpperCase();
            
            log.info("Original: '{}' -> Cleaned: '{}'", value, cleaned);
            return cleaned;
        });

        // Split the cleaned stream into two separate branches based on rules
        cleanedInput.split()
            // Branch 1: Valid messages (pass validation checks)
            .branch((key, value) -> isValid(value),
                    Branched.withConsumer(validStream -> {
                        validStream.to(CLEAN_TOPIC);
                        log.info("Routing valid message to topic: {}", CLEAN_TOPIC);
                    }))
            // Branch 2 (Default): Invalid messages
            .defaultBranch(Branched.withConsumer(invalidStream -> {
                // Map the value to "Message rejeté" and route to dead-letter
                invalidStream.mapValues(value -> "Message rejeté")
                             .to(DEAD_LETTER_TOPIC);
                log.info("Routing rejected message to topic: {}", DEAD_LETTER_TOPIC);
            }));

        // Build the topology object
        Topology topology = builder.build();
        log.info("Topology built successfully:\n{}", topology.describe());

        // 3. Instantiate and start the Kafka Streams client
        KafkaStreams streams = new KafkaStreams(topology, config);
        
        // Add a shutdown hook to cleanly close Kafka Streams when the program terminates (Ctrl+C)
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutdown hook triggered. Closing Kafka Streams...");
            streams.close();
            log.info("Kafka Streams closed.");
        }));

        streams.start();
        log.info("Kafka Streams application started. Press Ctrl+C to stop.");
    }

    /**
     * Helper method containing the business logic for validating a text message.
     * Rejection Criteria:
     * - Message is empty or null.
     * - Message length exceeds 100 characters.
     * - Message contains any forbidden words (HACK, SPAM, XXX).
     */
    private static boolean isValid(String value) {
        if (value == null || value.trim().isEmpty()) {
            log.warn("Rejection reason: Message is empty or null.");
            return false;
        }
        
        if (value.length() > 100) {
            log.warn("Rejection reason: Message length ({}) exceeds 100 characters limit.", value.length());
            return false;
        }

        // Search for forbidden words (since value is already converted to uppercase, checking contains is sufficient)
        if (value.contains("HACK")) {
            log.warn("Rejection reason: Message contains forbidden word 'HACK'.");
            return false;
        }
        if (value.contains("SPAM")) {
            log.warn("Rejection reason: Message contains forbidden word 'SPAM'.");
            return false;
        }
        if (value.contains("XXX")) {
            log.warn("Rejection reason: Message contains forbidden word 'XXX'.");
            return false;
        }

        return true;
    }
}
