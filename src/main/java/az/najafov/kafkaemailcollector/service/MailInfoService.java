package az.najafov.kafkaemailcollector.service;

import az.najafov.kafkaemailcollector.repository.EmailRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class MailInfoService {

    private final EmailRepository emailRepository;

    public long countUniqueEmailAddresses() {
        return emailRepository.countDistinctEmailAddress();
    }

    public long countUniqueDomains() {
        return emailRepository.countDistinctDomain();
    }

}
