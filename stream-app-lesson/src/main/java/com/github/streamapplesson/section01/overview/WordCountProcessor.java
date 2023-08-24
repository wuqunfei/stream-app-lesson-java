package com.github.streamapplesson.section01.overview;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Arrays;

@Component
public class WordCountProcessor {
    private static final Serde<String> STRING_SERDE = Serdes.String();
    private Logger logger = LoggerFactory.getLogger(WordCountProcessor.class);

    @Autowired
    void buildPipeline(StreamsBuilder streamsBuilder) {
        KStream<String, String> messageStream = streamsBuilder
                .stream("input-topic", Consumed.with(STRING_SERDE, STRING_SERDE));

        KTable<String, Long> wordCounts = messageStream
                .mapValues((ValueMapper<String, String>) String::toLowerCase)
                .flatMapValues(value -> Arrays.asList(value.split("\\W+")))
                .groupBy((key, word) -> word, Grouped.with(STRING_SERDE, STRING_SERDE))
                .count(Materialized.as("CounterStore"));

        wordCounts.toStream().to("output-topic");

    }

    @Autowired
    Topology buildTopology(StreamsBuilder streamsBuilder) {
        Serde<String> stringSerde = Serdes.String();
        String inputTopic = "input_word";
        String outputTopic = "out_word";
        streamsBuilder
                .stream(inputTopic, Consumed.with(stringSerde, stringSerde))
                .peek((k, v) -> logger.info("Observed event: {}", v))
                .mapValues(s -> s.toUpperCase())
                .peek((k, v) -> logger.info("Transformed event: {}", v))
                .to(outputTopic, Produced.with(stringSerde, stringSerde));
        return streamsBuilder.build();
    }
}
