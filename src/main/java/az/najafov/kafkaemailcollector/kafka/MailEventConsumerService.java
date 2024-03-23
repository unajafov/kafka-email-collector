package az.najafov.kafkaemailcollector.kafka;

import az.najafov.kafkaemailcollector.model.Email;
import az.najafov.kafkaemailcollector.model.UserRegistrationEvent;
import az.najafov.kafkaemailcollector.repository.EmailRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Service
@RequiredArgsConstructor
@Slf4j
public class MailEventConsumerService {

    private final EmailRepository emailRepository;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final ExecutorService executorService = Executors.newFixedThreadPool(10);


    @PostConstruct
    public void processStream() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "mail-info-stream-processor");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> sourceStream = builder.stream("userRegistrationTopic");

        sourceStream.flatMapValues(value -> {
            List<Email> result = new ArrayList<>();
            try {
                UserRegistrationEvent event = objectMapper.readValue(value, UserRegistrationEvent.class);
                String domain = event.getEmail().substring(event.getEmail().indexOf('@') + 1);
                Email emailEntity = new Email();
                emailEntity.setEmailAddress(event.getEmail());
                emailEntity.setDomain(domain);
                CompletableFuture.supplyAsync(() -> emailRepository.save(emailEntity), executorService)
                        .thenAccept(email -> log.info("EVENT PROCESSED SUCCESSFULLY"))
                        .exceptionally(e -> {
                            log.error("Error occurred during database operation", e);
                            return null;
                        });
            } catch (Exception e) {
                log.error("Error occurred during processing stream", e);
            }
            return result;
        }).foreach((key, value) -> log.info("Completed processing for key: {}", key));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

}
