package com.github.app.streams.springcloudstream;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Arrays;
import java.util.function.Function;

@Configuration
public class WordCounterConfig {

    private static KStream<String, Long> apply(KStream<String, String> stream) {

        KTable<String, Long> counterTable = stream.flatMapValues(value -> Arrays.asList(value.toLowerCase().split("\\W+")))
                .groupBy((key, word) -> word, Grouped.with(Serdes.String(), Serdes.String()))
                .count(Materialized.as("counter-store"));
        KStream<String, Long> outputStream = counterTable.toStream();
        outputStream.to("output-topic", Produced.with(Serdes.String(), Serdes.Long()));
        return outputStream;
    }

    @Bean
    public Function<KStream<String, String>, KStream<String, Long>> wordProcess() {
        return WordCounterConfig::apply;
    }
}
