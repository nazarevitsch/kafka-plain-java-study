package org.study;

import com.kafka.study.producer.client.dto.Message;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Properties;

public class Main {
    public static void main(String[] args) {
        KafkaConsumer<String, Message> consumer = createConsumer();
        consumer.subscribe(Arrays.asList("message-topic"));
        try {
            processRecords(consumer);
        } catch (Exception e) {}
    }

    private static KafkaConsumer<String, Message> createConsumer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9094");
        props.put("group.id", "message-group");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "4");
        props.put("max.partition.fetch.bytes", "100");
        props.put("auto.offset.reset", "earliest");
        props.put("heartbeat.interval.ms", "3000");
        props.put("session.timeout.ms", "6001");
        props.put("key.deserializer", "io.confluent.kafka.serializers.KafkaAvroDeserializer");
        props.put("value.deserializer", "io.confluent.kafka.serializers.KafkaAvroDeserializer");

        props.put("schema.registry.url", "http://0.0.0.0:8081");
        props.put("specific.avro.reader", "true");
        return new KafkaConsumer<>(props);
    }

    private static void processRecords(KafkaConsumer<String, Message> consumer) throws InterruptedException {
        System.out.println("Start !!!");
        while (true) {
            ConsumerRecords<String, Message> records = consumer.poll(Duration.of(100, ChronoUnit.MILLIS));
            if (!records.isEmpty()) {
                System.out.println("Begin of butch !!!");
                for (ConsumerRecord<String, Message> record : records) {
                    stopSever(2);
                    System.out.printf("Offset = %d, Key = %s, Value = %s\n", record.offset(), record.key(), record.value());
                    if (record.value().getMessage() == 666) {stopSever(100000);}
//                    consumer.commitSync();
                }
//                consumer.commitSync();
                System.out.println("End of butch !!!");
            }
        }
    }

    private static void stopSever(int ms) {
        try {
            Thread.sleep(ms);
        } catch (Exception e){}
    }
}