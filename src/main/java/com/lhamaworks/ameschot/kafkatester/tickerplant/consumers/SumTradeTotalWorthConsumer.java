package com.lhamaworks.ameschot.kafkatester.tickerplant.consumers;

import com.lhamaworks.ameschot.kafkatester.tickerplant.kafkasettings.TickerPlantTopics;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;

public class SumTradeTotalWorthConsumer extends AbstractStreamConsumer {
    /*Constants*/

    /*Attributes*/

    /*Constructor*/
    public SumTradeTotalWorthConsumer(String topic) {
        super(topic, "app-sum-total-trade-worth-consumer", "group-sum-total-trade-worth-consumer", Serdes.String(), Serdes.Double());
    }

    /*Methods*/

    @Override
    protected KafkaStreams buildConsumer() {
        final StreamsBuilder builder = new StreamsBuilder();

        //get the inbound source topic
        final KStream<String, Double> source = builder
                .stream(topic, Consumed.with(Serdes.String(), Serdes.Double())).peek((s, aDouble) -> System.out.println("W: " + s + " = " + String.format("%.2f", aDouble)));

        //KTable<String, Double> table = builder.table(topic, Consumed.with(Serdes.String(), Serdes.Double())).;


        //build the topology
        final Topology topology = builder.build();
        return new KafkaStreams(topology, consumerProperties);
    }


    public static void main(String[] args) throws Exception {
        new SumTradeTotalWorthConsumer(TickerPlantTopics.T_TOTAL_SUM_TRADE_WORTH).startConsumer();
    }


}