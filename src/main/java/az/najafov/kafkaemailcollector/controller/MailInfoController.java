package az.najafov.kafkaemailcollector.controller;

import az.najafov.kafkaemailcollector.service.MailInfoService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
@RequestMapping("/api/v1/mail-info")
public class MailInfoController {

    private final MailInfoService mailInfoService;

    @GetMapping("/emails/count")
    public long getUniqueEmailCount() {
        return mailInfoService.countUniqueEmailAddresses();
    }

    @GetMapping("/domains/count")
    public long getUniqueDomainCount() {
        return mailInfoService.countUniqueDomains();
    }

}
