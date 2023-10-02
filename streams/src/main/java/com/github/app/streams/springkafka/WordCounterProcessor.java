package com.github.app.streams.springkafka;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Arrays;

@Component
public class WordCounterProcessor {
    @Autowired
    void process(StreamsBuilder builder) {
        KStream<String, String> inputStream = builder.stream("input-topic", Consumed.with(Serdes.String(), Serdes.String()));
        KTable<String, Long> wordCounts = inputStream
                .flatMapValues(value -> Arrays.asList(value.toLowerCase().split("\\W+")))
                .groupBy((key, word) -> word, Grouped.with(Serdes.String(), Serdes.String()))
                .count(Materialized.as("counter-store"));
        KStream<String, Long> outputStream = wordCounts.toStream();
        outputStream.to("output-topic", Produced.with(Serdes.String(), Serdes.Long()));
    }
}
