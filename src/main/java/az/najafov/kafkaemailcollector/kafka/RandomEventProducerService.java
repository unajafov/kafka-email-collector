package az.najafov.kafkaemailcollector.kafka;

import az.najafov.kafkaemailcollector.model.UserRegistrationEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Random;

@Service
@RequiredArgsConstructor
@Slf4j
public class RandomEventProducerService {

    private final KafkaTemplate<String, String> kafkaTemplate;

    private final Random random = new Random();
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");

    // Predefined emails for potential repetition
    private final List<String> predefinedEmails = List.of("repeat1@example.com", "repeat2@sample.org",
            "repeat3@test.net");

    private UserRegistrationEvent generateRandomEvent() {
        // Decide whether to use a predefined or generate a new email
        String email;
        if (random.nextInt(5) == 0) { // 20% chance to pick a predefined email (1 out of 5)
            email = predefinedEmails.get(random.nextInt(predefinedEmails.size()));
        } else {
            // Generate a new email
            String localPart = random.ints(97, 122 + 1)
                    .limit(10)
                    .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
                    .toString();
            String domain = "example.com"; // Consider varying the domain as well
            email = localPart + "@" + domain;
        }

        String name = "User" + random.nextInt(1000); // Keep generating random names
        String signUpDate = dateFormat.format(new Date());
        String country = "Country" + random.nextInt(5); // Simplified country generation
        String interest = "Interest" + random.nextInt(5); // Simplified interest generation

        return new UserRegistrationEvent(email, name, signUpDate, country, interest);
    }

    private void produceEvent() {
        UserRegistrationEvent event = generateRandomEvent();
        try {
            String eventJson = objectMapper.writeValueAsString(event);
            kafkaTemplate.send("userRegistrationTopic", eventJson);
            log.info("Produced: " + eventJson);
        } catch (Exception e) {
            log.error("Error occurred while trying to publish an event to Kafka.", e.getCause());
        }
    }

    @Scheduled(fixedRate = 1000)
    public void produceEventPeriodically() {
        produceEvent();
    }

}
