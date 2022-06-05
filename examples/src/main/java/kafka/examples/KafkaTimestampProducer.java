package kafka.examples;

import java.util.Properties;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

/**
 * @author 0x822a5b87
 */
public class KafkaTimestampProducer extends Thread {

    private final kafka.javaapi.producer.Producer<Integer, String> producer;

    private final String topic;

    public KafkaTimestampProducer(String topic) {
        Properties props = new Properties();
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("metadata.broker.list", "localhost:9092");
        // Use random partitioner. Don't need the key type. Just set it to Integer.
        // The message is of type String.
        producer = new kafka.javaapi.producer.Producer<Integer, String>(new ProducerConfig(props));
        this.topic = topic;
    }

    @Override
    public void run() {
        for (int i = 0; i < 100; ++i) {
            String messageStr = String.valueOf(System.currentTimeMillis());
            producer.send(new KeyedMessage<Integer, String>(topic, messageStr));
            System.out.println(messageStr);
            try {
                Thread.sleep(1000 );
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
        KafkaTimestampProducer producer = new KafkaTimestampProducer("test");
        producer.start();
    }
}
