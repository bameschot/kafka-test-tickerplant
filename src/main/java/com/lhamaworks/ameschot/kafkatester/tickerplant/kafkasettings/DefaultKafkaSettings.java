package com.lhamaworks.ameschot.kafkatester.tickerplant.kafkasettings;

import org.apache.kafka.streams.StreamsConfig;

import java.util.Properties;

public class DefaultKafkaSettings extends Properties {
    public DefaultKafkaSettings() {
        //put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.2.5:9092");
        put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        put("enable.auto.commit", "true");
        //put("auto.offset.reset","earliest");
    }
}
