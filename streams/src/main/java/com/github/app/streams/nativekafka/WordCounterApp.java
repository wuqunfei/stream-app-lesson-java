package com.github.app.streams.nativekafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.Properties;
import java.util.regex.Pattern;

public class WordCounterApp {

    static final String INPUT_TOPIC = "input-native-topic";
    static final String OUTPUT_TOPIC = "output-native-topic";


    public static void main(final String[] args) {


        Properties streamsConfiguration = createProperties();
        StreamsBuilder builder = new StreamsBuilder();
        createWordCountStream(builder);
        KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);
        streams.cleanUp();
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    public static Properties createProperties() {
        final Properties properties = new Properties();
        String bootstrapServers = "localhost:19092";
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "native-kafka-app");
        properties.put(StreamsConfig.CLIENT_ID_CONFIG, "native-kafka-client");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "native-kafka-group");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        return properties;
    }

    static void createWordCountStream(final StreamsBuilder builder) {

        final KStream<String, String> textLines = builder.stream(INPUT_TOPIC);

        final Pattern pattern = Pattern.compile("\\W+", Pattern.UNICODE_CHARACTER_CLASS);

        final KTable<String, Long> wordCounts = textLines

                .flatMapValues(value -> Arrays.asList(pattern.split(value.toLowerCase())))

                .groupBy((keyIgnored, word) -> word)

                .count();
        wordCounts.toStream().to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.Long()));
    }

}
