package com.github.app.streams.springcloudstream;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.*;
import org.springframework.context.annotation.Bean;

import java.util.Arrays;
import java.util.function.Function;

public class WordCounterFunction {

    private static KStream<String, Long> apply(KStream<String, String> stream) {

        KTable<String, Long> counterTable = stream.flatMapValues(value -> Arrays.asList(value.toLowerCase().split("\\W+")))
                .groupBy((key, word) -> word, Grouped.with(Serdes.String(), Serdes.String()))
                .count(Materialized.as("counter-store"));
        KStream<String, Long> outputStream = counterTable.toStream();
        outputStream.to("output-topic", Produced.with(Serdes.String(), Serdes.Long()));
        return outputStream;
    }

    @Bean
    public Function<KStream<String, String>, KStream<String, Long>> process() {
        return WordCounterFunction::apply;
    }
}
