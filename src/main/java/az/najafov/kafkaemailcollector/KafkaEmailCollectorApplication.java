package az.najafov.kafkaemailcollector;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableScheduling
@SpringBootApplication
public class KafkaEmailCollectorApplication {

    public static void main(String[] args) {
        SpringApplication.run(KafkaEmailCollectorApplication.class, args);
    }

}
