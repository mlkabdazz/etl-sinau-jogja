package streams;

import com.google.gson.JsonParser;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

/**
 * Streaming processing
 */
public class StreamProcessing {

    public static void main(String[] args) {
//         preparation
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "demo-kafka-streams"); // similar like group but for streams
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

//        create stream
        StreamsBuilder builder = new StreamsBuilder();

//        input topic (reading from). filter user with greater than 1000 followers and send to new topic
        KStream<String, String> topic = builder.stream("twitter_tweets");
        KStream<String, String> filter = topic.filter((k, jsonTweet) ->
                extractUserFollowersInTweet(jsonTweet) > 1000
        );

//        output topic (target topic to save message)
        filter.to("twitter_userfollowers_gt_1000");

//        build stream
        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), properties);

//        start
        kafkaStreams.start();
    }

    private static JsonParser jsonParser = new JsonParser();

    private static Integer extractUserFollowersInTweet(String tweetJson) {
        try {
            return jsonParser.
                    parse(tweetJson).
                    getAsJsonObject().
                    get("user").
                    getAsJsonObject()
                    .get("followers_count")
                    .getAsInt();
        } catch (NullPointerException e) {
            return 0;
        }
    }

}
