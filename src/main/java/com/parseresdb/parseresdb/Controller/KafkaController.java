package com.parseresdb.parseresdb.Controller;

import com.parseresdb.parseresdb.Service.kafka.KafkaProducerService;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/kafka")
public class KafkaController {

    private final KafkaProducerService kafkaProducerService;

    public KafkaController(KafkaProducerService kafkaProducerService) {
        this.kafkaProducerService = kafkaProducerService;
    }

//    @GetMapping("/send/{message}")
//    public String sendMessage(@PathVariable String message) {
//        kafkaProducerService.sendMessage(message);
//        return "Message sent: " + message;
//    }
}
