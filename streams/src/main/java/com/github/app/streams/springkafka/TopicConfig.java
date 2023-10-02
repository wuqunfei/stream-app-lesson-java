package com.github.app.streams.springkafka;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class TopicConfig {

    @Bean
    public NewTopic inputTopic() {
        return TopicBuilder.name(WordCounterProcessor.INPUT_TOPIC).partitions(1).replicas(1).build();
    }

    @Bean
    public NewTopic outputTopic() {
        return TopicBuilder.name(WordCounterProcessor.OUTPUT_TOPIC).partitions(1).replicas(1).build();
    }
}
