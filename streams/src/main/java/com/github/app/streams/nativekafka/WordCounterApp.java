package com.github.app.streams.nativekafka;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
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

        try (KafkaStreams streams = new KafkaStreams(topology, properties)) {  // Create Stream application
            Runtime.getRuntime().addShutdownHook(new Thread(streams::close));  // Close Application and close stream application Gracefully
            streams.start(); // Start stream application
        }
    }

    public Topology createTopology() {
        String inputTopicName = "input-topic";
        String outputTopicName = "output-topic";

        Serde<String> stringSerde = Serdes.String();
        Serde<Long> longSerde = Serdes.Long();

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> textLines = builder.stream(inputTopicName, Consumed.with(Serdes.String(), stringSerde));
        KTable<String, Long> wordCounts = textLines
                // Split each text line, by whitespace, into words.  The text lines are the message
                .flatMapValues(value -> Arrays.asList(value.toLowerCase().split("\\W+")))
                // We use `groupBy` to ensure the words are available as message keys
                .groupBy((key, value) -> value)
                // Count the occurrences of each word (message key).
                .count();

        // Convert the `KTable<String, Long>` into a `KStream<String, Long>` and write to the output topic.
        wordCounts.toStream().to(outputTopicName, Produced.with(stringSerde, longSerde));
        return builder.build();
    }

    public Properties createProperty() {
        Properties properties = new Properties();
        properties.put(APPLICATION_ID_CONFIG, "native-kafka-app");
        properties.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        return properties;
    }
}
