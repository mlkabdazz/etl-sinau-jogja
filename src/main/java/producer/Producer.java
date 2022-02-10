package producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Simple producer
 */
public class Producer {

    static Logger logger = LoggerFactory.getLogger(Producer.class);

    public static void main(String[] args) {
//        Preparation
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

//         create producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

//        start looping for sending data
        for (int i = 0; i < 10; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<>("topic_name", "message" );

//            send data, when don 't use callback (get some result)
//            producer.send(record);

//          end data and get callback (get some result)
            producer.send(record, (recordMetadata, e) -> {
                if (e == null) {
//                    do something with metadata
                    logger.info(
                            "\n===THIS METADATA=== \n" +
                                    "topic : " + recordMetadata.topic() + "\n" +
                                    "partition : " + recordMetadata.partition() + "\n" +
                                    "offset : " + recordMetadata.offset() + "\n" +
                                    "timestamp : " + recordMetadata.timestamp() + "\n"
                    );
                } else {
//                    see the error!!!
                    logger.error("error while producing : " + e);

                }
            });
        }

        producer.close(); // for flush and close

    }

}
