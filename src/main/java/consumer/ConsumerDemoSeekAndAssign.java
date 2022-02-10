package consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

/**
 * seek and assign consumer. getting data for specific partitions and offsets
 */
public class ConsumerDemoSeekAndAssign {

    private static final String bootstrapServer = "127.0.0.1:9092";
    private static final String stringDes = StringDeserializer.class.getName();
    private static final String autoOffsetResetConfig = "earliest";
    private static final String topic = "java_practice_topic";
    static Logger logger = LoggerFactory.getLogger(ConsumerDemo.class);

    public static void main(String[] args) {
//        Preparation
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, stringDes);
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, stringDes);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "please-delete");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetResetConfig);

//        Create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

//        assign for specific partition
        TopicPartition partitionToReadFrom = new TopicPartition(topic, 0);
        consumer.assign(Arrays.asList(partitionToReadFrom));
        long offsetToReadFrom = 15L;

//       init seek data from specific partition and specific offsets
        consumer.seek(partitionToReadFrom, offsetToReadFrom);

        int numberOfMessageToRead = 5;
        boolean keepOnReading = true;
        int numberOfMessageReadSoFar = 0;

//        code in below, will read data from specific partition and specific offsets
//        so, after 5 message / offsets has been reading, the consumer will stop.
        while (keepOnReading) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                numberOfMessageReadSoFar += 1;
                logger.info("Key : " + record.key() + ", Value : " + record.value());
                logger.info("Partition : " + record.partition() + ", Offset : " + record.offset());
                if (numberOfMessageReadSoFar >= numberOfMessageToRead) {
                    keepOnReading = false;
                    break;
                }
            }
        }
        logger.info("End of reading the message!!!");

    }
}
