package consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Properties;

/**
 * Simple kafka consumer code
 */
public class ConsumerDemo {

    static Logger logger = LoggerFactory.getLogger(ConsumerDemo.class);

    public static void main(String[] args) {
        // Preparation
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "my-ex-java-application");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Create Consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Arrays.asList("java_practice_topicx"));

        // Get data
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));
//            take all data and store in a variable
//            Iterable<ConsumerRecord<String, String>> x = consumer.poll(Duration.ofSeconds(10)).records("my-java-application");

//            Get data and processing per data
            for (ConsumerRecord<String, String> record : records) {
                logger.info("key : " + record.key() + ", value : " + record.value());
                logger.info("partition : " + record.partition() + ", offset : " + record.offset());
            }
        }
    }

}
