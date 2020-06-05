package org.mylearning.kafkabeginner;

import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;

/**
 * Consumer assign & seek demo. Assign & seek is typically used to replay the
 * messages from certain offset.
 */
public class ConsumerDemoAssignSeek {
    private static final Logger LOG = LoggerFactory.getLogger(ConsumerDemo.class);

    @SuppressWarnings(value = "all")
    public static void main(String[] args) {
        // turn off DEBUG log
        ch.qos.logback.classic.Logger root = (ch.qos.logback.classic.Logger) LoggerFactory
                .getLogger(Logger.ROOT_LOGGER_NAME);
        root.setLevel(Level.INFO);

        // config consumer. notice: group-id is NOT needed
        Properties properties = new Properties();
        properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(AUTO_OFFSET_RESET_CONFIG, "latest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        TopicPartition partition = new TopicPartition("first_topic", 0); // create certian partition of topic
        consumer.assign(Collections.singleton(partition)); // assign certian partition of topic to the consumer
        consumer.seek(partition, 15l); // seek messages from certian offset

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
            records.forEach(record -> LOG.info("Key: {}, Value: {}, Partition: {}, Offset: {}", record.key(),
                    record.value(), record.partition(), record.offset()));
        }
    }

}