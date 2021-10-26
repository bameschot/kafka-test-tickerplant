package com.lhamaworks.kafkatester.tickerplant.producer;

import com.lhamaworks.kafkatester.tickerplant.kafkasettings.TickerPlantTopics;
import com.lhamaworks.kafkatester.tickerplant.market.Symbols;
import com.lhamaworks.kafkatester.tickerplant.market.Trade;
import com.lhamaworks.kafkatester.tickerplant.market.TradeGenerator;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class OutstandingSharesProducer extends AbstractProducer<String, Integer>
{
    /*Constants*/

    /*Attributes*/
    protected Random random = new Random();

    /*Constructor*/
    public OutstandingSharesProducer(String topic, long timeoutMS, int maxPublishes)
    {
        super(topic, timeoutMS, maxPublishes, Serdes.String().serializer(), Serdes.Integer().serializer());
    }

    /*Main*/
    public static void main(String[] args)
    {
        OutstandingSharesProducer tProd = new OutstandingSharesProducer(TickerPlantTopics.T_OUTSTANDING_SHARES, 10000, -1);
        new Thread(tProd).start();
        Runtime.getRuntime().addShutdownHook(new Thread(tProd::close));
    }

    /*Methods*/
    @Override
    public List<ProducerRecord<String, Integer>> produce()
    {
        List<ProducerRecord<String,Integer>> messages = new ArrayList<>();

        for (Symbols s : Symbols.values())
        {
            int newShares = s.shares + ((random.nextBoolean() ? 1 : -1) * random.nextInt(s.shares / 4));

            //Use key if you want all the messages to go to a single partition
            messages.add(new ProducerRecord<String, Integer>(topic, s.symbol, newShares));
        }

        return messages;
    }


}
