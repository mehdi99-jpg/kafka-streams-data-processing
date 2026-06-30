package ma.enset.exercise3.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;

/**
 * REST Controller for the Web Producer.
 * Receives click actions from the Web interface and publishes them to the 'clicks' topic in Kafka.
 */
@RestController
@CrossOrigin(origins = "*")
@RequestMapping("/clicks")
public class ClickProducerController {

    private static final Logger log = LoggerFactory.getLogger(ClickProducerController.class);

    private final KafkaTemplate<String, String> kafkaTemplate;

    public ClickProducerController(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * Publishes a click event to Kafka.
     * Endpoint: POST /clicks/produce?userId=user1
     */
    @PostMapping("/produce")
    public Map<String, String> produceClick(@RequestParam String userId) {
        String cleanUserId = userId != null ? userId.trim() : "anonymous";
        
        log.info("Received click event from user: {}", cleanUserId);
        
        // Publish to topic: clicks, Key = userId, Value = click
        kafkaTemplate.send("clicks", cleanUserId, "click");

        Map<String, String> response = new HashMap<>();
        response.put("status", "success");
        response.put("userId", cleanUserId);
        response.put("message", "Click registered and sent to Kafka.");
        return response;
    }
}
