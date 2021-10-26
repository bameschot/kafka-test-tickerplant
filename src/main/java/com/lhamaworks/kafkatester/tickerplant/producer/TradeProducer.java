package com.lhamaworks.kafkatester.tickerplant.producer;

import com.lhamaworks.kafkatester.tickerplant.kafkasettings.TickerPlantTopics;
import com.lhamaworks.kafkatester.tickerplant.kafkasettings.DefaultKafkaSettings;
import com.lhamaworks.kafkatester.tickerplant.market.Trade;
import com.lhamaworks.kafkatester.tickerplant.market.TradeGenerator;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;

import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class TradeProducer extends AbstractProducer<String,Trade>
{
    /*Constants*/

    /*Attributes*/

    /*Constructor*/
    public TradeProducer(String topic, long timeoutMS, int maxPublishes)
    {
        super(topic,timeoutMS,maxPublishes, Serdes.String().serializer(),new Trade.TradeSerializer());
    }

    /*Main*/
    public static void main(String[] args)
    {
        TradeProducer tProd = new TradeProducer(TickerPlantTopics.T_RAW_TRADES,1000,-1);
        new Thread(tProd).start();
        Runtime.getRuntime().addShutdownHook(new Thread(tProd::close));
    }

    /*Methods*/
    @Override
    public List<ProducerRecord<String, Trade>> produce()
    {
        Trade trade = TradeGenerator.i().generateTrade();
        return Collections.singletonList(new ProducerRecord<>(topic,trade.symbol.symbol,trade));
    }


}
