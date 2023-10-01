package com.github.app.streams;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;

@SpringBootApplication
public class StreamsApplication {

    public static void main(String[] args) {
        SpringApplication.run(StreamsApplication.class, args);
    }

}