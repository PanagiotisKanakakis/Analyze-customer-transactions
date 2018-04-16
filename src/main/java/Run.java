import org.apache.kafka.clients.producer.Producer;
import producer.CsvProducer;
import producer.DBProducer;

import java.io.IOException;

/**
 * Created by panagiotis on 10/4/2018.
 */
public class Run {

    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            throw new IllegalArgumentException("Must have either 'producer' or 'consumer' as argument");
        }
        switch (args[0]) {
            case "producer":
                CsvProducer.runProducer();
//                    DBProducer.main(args);
                break;
            case "consumer":
//                    Consumer.main(args);
                break;
            default:
                throw new IllegalArgumentException("Don't know how to do " + args[0]);
        }
    }
}
