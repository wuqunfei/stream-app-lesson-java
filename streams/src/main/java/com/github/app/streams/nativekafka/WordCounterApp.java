package com.github.app.streams.nativekafka;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.Properties;

import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG;


public class WordCounterApp {
    public static void main(String[] args) {

        WordCounterApp app = new WordCounterApp();

        Topology topology = app.createTopology();       // Create streams transform Topology
        Properties properties = app.createProperty();   // Create connection property

        try (KafkaStreams streams = new KafkaStreams(topology, properties)) {  // Create Stream application with Safety
            Runtime.getRuntime().addShutdownHook(new Thread(streams::close));  // Close Application and stream application gracefully
            streams.start(); // Start stream application
        }
    }

    public Topology createTopology() {
        String inputTopicName = "input-topic";
        String outputTopicName = "output-topic";

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> inputStream = builder.stream(inputTopicName, Consumed.with(Serdes.String(), Serdes.String()));

        KTable<String, Long> wordCounts = inputStream
                .flatMapValues(value -> Arrays.asList(value.toLowerCase().split("\\W+")))
                .groupBy((key, word) -> word, Grouped.with(Serdes.String(), Serdes.String()))
                .count(Materialized.as("counter-store"));

        KStream<String, Long> outputStream = wordCounts.toStream();
        // Convert the `KTable<String, Long>` into a `KStream<String, Long>` and write to the output topic.
        outputStream.to(outputTopicName, Produced.with(Serdes.String(), Serdes.Long()));
        return builder.build();
    }

    public Properties createProperty() {
        Properties properties = new Properties();
        properties.put(APPLICATION_ID_CONFIG, "native-kafka-app");
        properties.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:19092");
        properties.put(DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        return properties;
    }
}
