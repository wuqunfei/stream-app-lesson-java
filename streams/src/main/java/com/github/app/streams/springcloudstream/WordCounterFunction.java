package com.github.app.streams.springcloudstream;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.*;
import org.springframework.context.annotation.Bean;

import java.util.Arrays;
import java.util.function.Function;

public class WordCounterFunction {

    @Bean
    public Function<KStream<String, String>, KTable<String, Long>> process() {
        return stream -> stream.mapValues((ValueMapper<String, String>) String::toLowerCase)
                .flatMapValues(value -> Arrays.asList(value.split("\\W+")))
                .groupBy((key, word) -> word, Grouped.with(Serdes.String(), Serdes.String()))
                .count(Materialized.as("counter-store"));
    }
}
