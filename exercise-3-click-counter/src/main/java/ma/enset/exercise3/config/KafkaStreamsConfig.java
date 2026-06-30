package ma.enset.exercise3.config;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;

/**
 * Spring Kafka Streams configuration class.
 * Defines the real-time processing topologies and materializes local RocksDB state stores.
 */
@Configuration
@EnableKafkaStreams
public class KafkaStreamsConfig {

    public static final String CLICKS_TOPIC = "clicks";
    public static final String CLICK_COUNTS_TOPIC = "click-counts";
    
    // Store names for Interactive Queries
    public static final String USER_CLICKS_STORE = "user-clicks-store";
    public static final String GLOBAL_CLICKS_STORE = "global-clicks-store";

    @Bean
    public KStream<String, String> clickStreamProcessing(StreamsBuilder streamsBuilder) {
        // 1. Consume click events from the 'clicks' topic
        KStream<String, String> clickStream = streamsBuilder.stream(CLICKS_TOPIC, 
                Consumed.with(Serdes.String(), Serdes.String()));

        // --- VARIANT 2: Click Count per User ---
        // Group by the key (userId) and count events. Results are materialized in 'user-clicks-store'.
        KTable<String, Long> userCounts = clickStream
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as(USER_CLICKS_STORE)
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.Long()));

        // Send user count updates to 'click-counts'
        userCounts.toStream()
                .map((user, count) -> new KeyValue<>(user, String.valueOf(count)))
                .to(CLICK_COUNTS_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

        // --- VARIANT 1: Global Click Count ---
        // Map all click events to a constant key "totalClicks" to group them globally.
        // Results are materialized in 'global-clicks-store'.
        KTable<String, Long> globalCounts = clickStream
                .map((key, value) -> new KeyValue<>("totalClicks", value))
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as(GLOBAL_CLICKS_STORE)
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.Long()));

        // Send global count updates to 'click-counts'
        globalCounts.toStream()
                .map((totalKey, count) -> new KeyValue<>(totalKey, String.valueOf(count)))
                .to(CLICK_COUNTS_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

        return clickStream;
    }
}
