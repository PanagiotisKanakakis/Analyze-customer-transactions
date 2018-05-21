package producer;

import org.apache.kafka.clients.producer.*;

import com.google.common.io.Resources;

import java.io.*;
import java.net.URL;
import java.net.URLConnection;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * Created by panagiotis on 10/4/2018.
 */
public class CsvProducer {

    private final static String EVENT_TOPIC = "events";

    public static Producer createProducer() throws IOException {

        try (InputStream props = Resources.getResource("producer.properties").openStream()) {
            Properties properties = new Properties();
            properties.load(props);
            return new KafkaProducer<>(properties);
        }
    }

    public static void runProducer() throws InterruptedException, IOException {

        final Producer<String, String> producer = createProducer();


        // open csv
        URL url = new URL("http://195.134.66.230:8080/dataset/category_tree.csv");
        URLConnection connection = url.openConnection();

        InputStreamReader input = new InputStreamReader(connection.getInputStream());
        BufferedReader buffer = null;
        String line = "";
        String csvSplitBy = ",";

        try {
            buffer = new BufferedReader(input);
            while ((line = buffer.readLine()) != null) {
                System.out.println(line);
                final ProducerRecord<String, String> record =
                        new ProducerRecord<>(EVENT_TOPIC,line);
                producer.send(record, (metadata, exception) -> {
                    if (metadata != null) {
                        System.out.printf("sent record(key=%s value=%s) " +
                                        "meta(partition=%d, offset=%d)\n",
                                record.key(), record.value(), metadata.partition(),
                                metadata.offset());
                    } else {
                        exception.printStackTrace();
                    }
                });
            }
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            producer.flush();
            producer.close();
        }
    }


}
