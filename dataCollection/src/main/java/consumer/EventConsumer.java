package consumer;

import com.google.common.io.Resources;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Properties;

/**
 * Created by panagiotis on 5/5/2018.
 */
public class EventConsumer {

    private final static String EVENT_TOPIC = "events";

    public static Consumer<String,String> createConsumer() throws IOException {

        try (InputStream props = Resources.getResource("consumer.properties").openStream()) {
            Properties properties = new Properties();
            properties.load(props);

            final Consumer<String, String> consumer = new KafkaConsumer<>(properties);
            consumer.subscribe(Collections.singletonList(EVENT_TOPIC));
            return consumer;
        }
    }

    public static void runConsumer() throws InterruptedException, IOException {
        final Consumer<String, String> consumer = createConsumer();
        final int giveUp = 100;   int noRecordsCount = 0;
        while (true) {
            final ConsumerRecords<String, String> consumerRecords =
                    consumer.poll(100);
            if (consumerRecords.count()==0) {
                noRecordsCount++;
                if (noRecordsCount > giveUp) break;
                else continue;
            }
            consumerRecords.forEach(record -> {
                System.out.printf("Consumer Record:(%d, %s, %d, %d)\n",
                        record.key(), record.value(),
                        record.partition(), record.offset());
            });
            consumer.commitAsync();
        }
        consumer.close();
        System.out.println("DONE");
    }
}
