package kafka.demo;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerWithCallback {

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(ProducerWithCallback.class);
//        Create Producer properties
        String bootstrapServers = "localhost:9092";
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//        Create the producer itself
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
//        Create the Producer Record
        ProducerRecord<String, String> record = new ProducerRecord<>("primeiro-topico", "Olá com Callback!");
//        Send data
        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                // executes every time a record is sucessfully sent or exception is thrown
                if (e == null){
                    logger.info("\n#####-Received new metadata-#####\n" +
                            "Topic: " + recordMetadata.topic() + "\n" +
                            "Partition: " +recordMetadata.partition() + "\n" +
                            "Offset: " + recordMetadata.offset() + "\n" +
                            "Timestamp: " + recordMetadata.timestamp() );
                } else {
                    logger.error("Error while producing", e);
                }
            }
        });
        producer.close();
    }
}
