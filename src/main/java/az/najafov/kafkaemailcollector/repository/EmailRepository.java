package az.najafov.kafkaemailcollector.repository;

import az.najafov.kafkaemailcollector.model.Email;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

public interface EmailRepository extends JpaRepository<Email, Long> {

    @Query(value = "select count(distinct email_address) from email", nativeQuery = true)
    long countDistinctEmailAddress();

    @Query(value = "select count(distinct domain) from email", nativeQuery = true)
    long countDistinctDomain();

}
