bin/kafka-console-producer.sh --broker-list localhost:19092 --topic input-word-process


bin/kafka-console-consumer.sh --bootstrap-server localhost:19092 --topic input-word-process --from-beginning


bin/kafka-console-consumer.sh --bootstrap-server localhost:19092 \
    --topic output-word-process \
    --from-beginning \
    --formatter kafka.tools.DefaultMessageFormatter \
    --property print.key=true \
    --property print.value=true \
    --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
    --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer
