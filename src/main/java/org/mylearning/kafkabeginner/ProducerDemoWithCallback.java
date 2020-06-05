package org.mylearning.kafkabeginner;

import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;

/**
 * Demo of Kafka producer with callback
 */
public class ProducerDemoWithCallback {
    private static final Logger LOG = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

    public static void main(String[] args) {
        // turn off DEBUG log
        ch.qos.logback.classic.Logger root = (ch.qos.logback.classic.Logger) LoggerFactory
                .getLogger(Logger.ROOT_LOGGER_NAME);
        root.setLevel(Level.INFO);

        // create producer properties
        Properties properties = new Properties();
        properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // create and send data
        for (int i = 0; i < 100; i++) {
            String key = "id_" + i;
            producer.send(new ProducerRecord<String, String>("first_topic", key, "hello world " + i), new Callback() {
                // producer callback
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception == null) {
                        LOG.info("\nTopic: {}\nPartition: {}\nOffset: {}\nTimestamp: {}\nKey: {}\n", metadata.topic(),
                                metadata.partition(), metadata.offset(), metadata.timestamp(), key);
                    } else {
                        LOG.error("Error while processing", exception);
                    }
                }
            });
        }

        producer.flush(); // you must flush or close producer so that data get sent out!
        producer.close(); // close producer
    }

}