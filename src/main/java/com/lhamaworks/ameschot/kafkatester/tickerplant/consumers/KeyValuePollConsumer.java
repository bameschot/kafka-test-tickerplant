package com.lhamaworks.ameschot.kafkatester.tickerplant.consumers;

import com.lhamaworks.ameschot.kafkatester.tickerplant.kafkasettings.DefaultKafkaSettings;
import com.lhamaworks.ameschot.kafkatester.tickerplant.kafkasettings.TickerPlantTopics;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class KeyValuePollConsumer implements AutoCloseable {
    /*Constants*/

    /*Attributes*/
    protected String topic;
    protected Map<String, Double> keyValueMap = new HashMap<>();
    protected Properties consumerProperties;
    protected KafkaConsumer<String, Double> consumer;

    /*Constructor*/
    public KeyValuePollConsumer(String topic, String appID, String groupID) {
        this.topic = topic;

        consumerProperties = new DefaultKafkaSettings();
        consumerProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, appID);
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, groupID);
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.DoubleDeserializer");

        consumer = new KafkaConsumer<>(consumerProperties);
    }

    /*Methods*/

    protected KafkaStreams consume() {
        //setup consumer
        consumer.subscribe(Arrays.asList(topic));

        consumer.seekToBeginning(Arrays.asList());

        System.out.println("Started polling: " + topic);
        while (true) {
            //read topic records
            ConsumerRecords<String, Double> recs = consumer.poll(Duration.of(10000, ChronoUnit.MILLIS));

            //add results to map
            for (ConsumerRecord<String, Double> cr : recs) {
                //System.out.println("CR: "+cr);
                keyValueMap.put(cr.key(), cr.value());
            }

            //commit as already read
            consumer.commitSync();
        }
    }

    public void startConsumer() {

        // attach shutdown handler to catch control-c
        final CountDownLatch latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(consumerProperties.getProperty(StreamsConfig.APPLICATION_ID_CONFIG) + "-shutdown-hook") {
            @Override
            public void run() {
                consumer.close();
                latch.countDown();
            }
        });

        try {
            System.out.println("Started Consumer: " + consumerProperties.getProperty(StreamsConfig.APPLICATION_ID_CONFIG) + " on: " + topic);
            consume();

            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
    }


    public static void main(String[] args) throws Exception {
        List<KeyValuePollConsumer> consumers = Arrays.asList(
                new KeyValuePollConsumer(TickerPlantTopics.T_INDEXES, "app-indexes-poll-consumer", "group-indexes-poll-consumer")
        );

        for (KeyValuePollConsumer consumer : consumers) {
            new Thread(consumer::startConsumer).start();
            Thread.sleep(250);
        }

        //create and start a thread that prints the contents it a slower pace than each received item
        new Thread(() ->
        {
            while (true) {
                for (KeyValuePollConsumer consumer : consumers) {
                    //print
                    System.out.println("-------<<" + consumer.topic + ">>---------");
                    consumer.keyValueMap.forEach((key, value) -> System.out.println(key + " = " + String.format("%.2f", value)));


                }

                System.out.println("+++++++++++++++++++++++++++++++++++++++++++++++++++");

                //sleep
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }


        }).start();

    }


    @Override
    public void close() {
        consumer.close();
    }
}