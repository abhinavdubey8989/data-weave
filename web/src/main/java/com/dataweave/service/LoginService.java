package com.dataweave.service;


import com.dataweave.dto.enums.SerializationType;
import com.dataweave.dto.request.LoginRequest;
import com.dataweave.kafka.KafkaAvroProducerService;
import com.dataweave.kafka.KafkaProducerService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class LoginService {

    @Autowired
    private KafkaProducerService kafkaProducerService;


    @Autowired
    private KafkaAvroProducerService kafkaAvroProducerService;


    public Object login(LoginRequest req) {
        log.info("Login Request: " + req);
        if (SerializationType.AVRO == req.getSerializationType()) {
            kafkaAvroProducerService.publishAvroMessagesAsync(req);
        } else {
            kafkaProducerService.publishMessagesAsync(req);
        }
        return req;
    }
}
