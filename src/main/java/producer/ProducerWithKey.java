package producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerWithKey {

    static Logger logger = LoggerFactory.getLogger(ProducerWithKey.class);

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        // set properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        // create record
//        ProducerRecord<String, String> record = new ProducerRecord<>("java_practice_topic", "hiya-hiya-hiya");
        for (int i = 10; i < 20; i++) {
            String topic = "java_practice_topicx";
            String data = "Number of data : " + i;
            String key = "id_" + i;

            logger.info(data + " - " + key);
            /*
            10 - id_10 -> partition 2 ; offset 16
            11 - id_11 -> partition 1 ; offset 34
            12 - id_12 -> parititon 2 ; offset 17
            13 - id_13 -> partition 1 ; offset 35
            14 - id_14 -> partition 2 ; offset 18
            15 - id_15 -> partition 0 ; offset 34
            16 - id_16 -> partition 2 ; offset 19
            17 - id_17 -> partition 2 ; offset 20
            18 - id_18 -> partition 1 ; offset 36
            19 - id_19 -> partition 1 ; offset 37
            summary : same key will insert into same partition but still move the offset.
            so when data has been input to the kafka, it cant to edit or override. still move and create new offset.
             */

            ProducerRecord<String, String> record = new ProducerRecord<>(
                    topic,
                    key,
                    data
            );
            // send data
//        producer.send(record); // when dont use callback
            producer.send(record, (recordMetadata, e) -> {
                if (e == null) {
//                    do something with metadata
                    logger.info(
                            "\n===THIS METADATA=== \n" +
                                    "Key" + record.key() + "\n" +
                                    "topic : " + recordMetadata.topic() + "\n" +
                                    "partition : " + recordMetadata.partition() + "\n" +
                                    "offset : " + recordMetadata.offset() + "\n" +
                                    "timestamp : " + recordMetadata.timestamp()
                    );
                } else {
//                    see the error!!!
                    logger.error("error while producing : " + e);

                }
            }).get();
        }
//        producer.flush(); // for flush
        producer.close(); // for flush and close

    }

}
