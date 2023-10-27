package com.lhamaworks.ameschot.kafkatester.tickerplant.consumers;

import com.lhamaworks.ameschot.kafkatester.tickerplant.kafkasettings.DefaultKafkaSettings;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public abstract class AbstractStreamConsumer<K, V> {
    /*Constants*/

    /*Attributes*/
    protected String topic;
    protected Properties consumerProperties;

    /*Constructor*/
    public AbstractStreamConsumer(String topic, String applicationID, String groupID, Serde<K> keySerde, Serde<V> valueSerde) {
        this.topic = topic;

        //properties
        consumerProperties = new DefaultKafkaSettings();
        consumerProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationID);
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, groupID);
        consumerProperties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, keySerde.getClass());
        consumerProperties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, valueSerde.getClass());
    }

    /*Methods*/
    protected abstract KafkaStreams buildConsumer();

    protected void postStartAction(KafkaStreams streams) {

    }

    public void startConsumer() {
        KafkaStreams streams = buildConsumer();

        // attach shutdown handler to catch control-c
        final CountDownLatch latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(consumerProperties.getProperty(StreamsConfig.APPLICATION_ID_CONFIG) + "-shutdown-hook") {
            @Override
            public void run() {
                System.out.println("Closed consumer");
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            postStartAction(streams);
            System.out.println("Started Consumer: " + consumerProperties.getProperty(StreamsConfig.APPLICATION_ID_CONFIG) + " on: " + topic);

            latch.await();
        } catch (Throwable e) {
            System.out.println(e);
            System.exit(1);
        }
    }

}
