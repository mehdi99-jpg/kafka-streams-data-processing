package ma.enset.exercise3.controller;

import ma.enset.exercise3.config.KafkaStreamsConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;

/**
 * REST Controller for the REST Consumer.
 * Uses Kafka Streams Interactive Queries to fetch results directly from local State Stores.
 */
@RestController
@CrossOrigin(origins = "*")
@RequestMapping("/clicks")
public class ClickConsumerController {

    private static final Logger log = LoggerFactory.getLogger(ClickConsumerController.class);

    // Factory bean that manages the lifecycle of the KafkaStreams instance in Spring
    private final StreamsBuilderFactoryBean factoryBean;

    public ClickConsumerController(StreamsBuilderFactoryBean factoryBean) {
        this.factoryBean = factoryBean;
    }

    /**
     * Exposes click counts from Kafka Streams state stores.
     * Endpoint: GET /clicks/count?byUser=true (or false for global)
     */
    @GetMapping("/count")
    public Map<String, Object> getClickCounts(@RequestParam(defaultValue = "false") boolean byUser) {
        KafkaStreams kafkaStreams = factoryBean.getKafkaStreams();
        if (kafkaStreams == null || kafkaStreams.state() != KafkaStreams.State.RUNNING) {
            log.warn("Kafka Streams application is not running yet. State: {}", 
                    kafkaStreams != null ? kafkaStreams.state() : "NULL");
            Map<String, Object> error = new HashMap<>();
            error.put("error", "Kafka Streams state store is not available yet. Please wait.");
            return error;
        }

        Map<String, Object> result = new HashMap<>();

        try {
            if (byUser) {
                log.info("Querying local store '{}' for user-specific click counts...", KafkaStreamsConfig.USER_CLICKS_STORE);
                
                // Retrieve the read-only key-value store containing user click counts
                ReadOnlyKeyValueStore<String, Long> userStore = kafkaStreams.store(
                        StoreQueryParameters.fromNameAndType(
                                KafkaStreamsConfig.USER_CLICKS_STORE,
                                QueryableStoreTypes.keyValueStore()
                        )
                );

                // Iterate over all entries in the store and put them in the result map
                try (KeyValueIterator<String, Long> iterator = userStore.all()) {
                    while (iterator.hasNext()) {
                        var next = iterator.next();
                        result.put(next.key, next.value);
                    }
                }
            } else {
                log.info("Querying local store '{}' for global click count...", KafkaStreamsConfig.GLOBAL_CLICKS_STORE);

                // Retrieve the read-only key-value store containing global count
                ReadOnlyKeyValueStore<String, Long> globalStore = kafkaStreams.store(
                        StoreQueryParameters.fromNameAndType(
                                KafkaStreamsConfig.GLOBAL_CLICKS_STORE,
                                QueryableStoreTypes.keyValueStore()
                        )
                );

                Long totalClicks = globalStore.get("totalClicks");
                result.put("totalClicks", totalClicks != null ? totalClicks : 0L);
            }
        } catch (Exception e) {
            log.error("Error querying Kafka Streams state store: {}", e.getMessage(), e);
            result.put("error", "State store query failed: " + e.getMessage());
        }

        return result;
    }
}
