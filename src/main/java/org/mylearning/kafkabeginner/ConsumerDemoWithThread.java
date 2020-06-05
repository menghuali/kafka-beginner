package org.mylearning.kafkabeginner;

import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;

/**
 * Andvanced consumer with thread. The consumer is properly closed when
 * shutdown.
 */
public class ConsumerDemoWithThread {
    private static final Logger LOG = LoggerFactory.getLogger(ConsumerDemo.class);

    public static void main(String[] args) {
        // turn off DEBUG log
        ch.qos.logback.classic.Logger root = (ch.qos.logback.classic.Logger) LoggerFactory
                .getLogger(Logger.ROOT_LOGGER_NAME);
        root.setLevel(Level.INFO);

        CountDownLatch latch = new CountDownLatch(1);
        Runnable consumer = new InnerConsumerRunnable(latch); // create consumer runnable object

        // add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Caught shutdown hook");
            ((InnerConsumerRunnable) consumer).shutdown(); // ask consumer thread to shutdown
            try {
                latch.await(); // wait until consumer thread is properly shutdown
            } catch (InterruptedException e) {
                LOG.error("Thread was interrupted", e);
            }
            LOG.info("Application has exited");
        }));

        // create and run consumer thread
        Thread thread = new Thread(consumer);
        thread.start();

        // wait until consumer thread is properly shutdown
        try {
            latch.await();
        } catch (InterruptedException e) {
            LOG.error("Application is interrupted", e);
        } finally {
            LOG.info("Application is closing");
        }
    }

    /**
     * Runnable class of consumber
     */
    public static class InnerConsumerRunnable implements Runnable {
        private static final Logger INNER_LOG = LoggerFactory.getLogger(InnerConsumerRunnable.class);
        private CountDownLatch latch;
        KafkaConsumer<String, String> consumer;

        /**
         * Default constructor
         * 
         * @param latch count down latch
         */
        public InnerConsumerRunnable(CountDownLatch latch) {
            this.latch = latch;

            // create consumber
            Properties properties = new Properties();
            properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            properties.setProperty(GROUP_ID_CONFIG, "first_group");
            properties.setProperty(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(AUTO_OFFSET_RESET_CONFIG, "latest");
            consumer = new KafkaConsumer<>(properties);
            consumer.subscribe(Collections.singleton("first_topic"));
        }

        @Override
        public void run() {
            try {
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                    records.forEach(record -> LOG.info("Key: {}, Value: {}, Partition: {}, Offset: {}", record.key(),
                            record.value(), record.partition(), record.offset()));
                }
            } catch (WakeupException e) {
                INNER_LOG.info("Received wakeup signal");
            } finally {
                consumer.close();
                INNER_LOG.info("Consumer was closed");
                latch.countDown(); // tell main code to exit
            }

        }

        /**
         * Shutdown consumer thread
         */
        public void shutdown() {
            consumer.wakeup(); // make consumer throw WakeupException
        }

    }

}