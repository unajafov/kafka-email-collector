package az.najafov.kafkaemailcollector.kafka;

import az.najafov.kafkaemailcollector.model.Email;
import az.najafov.kafkaemailcollector.model.UserRegistrationEvent;
import az.najafov.kafkaemailcollector.repository.EmailRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class MailEventConsumerService {

    private final EmailRepository emailRepository;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @KafkaListener(topics = "userRegistrationTopic", groupId = "mail-info-collector")
    public void listen(String message) {
        try {
            UserRegistrationEvent event = objectMapper.readValue(message, UserRegistrationEvent.class);
            Email emailEntity = new Email();
            String domain = event.getEmail().substring(event.getEmail().indexOf('@') + 1);

            emailEntity.setEmailAddress(event.getEmail());
            emailEntity.setDomain(domain);

            emailRepository.save(emailEntity);
            System.out.println("Processed: " + event.getEmail());
        } catch (Exception e) {
            log.error("Error occurred while trying to listen an event.", e.getCause());
        }
    }

}
