package com.lhamaworks.ameschot.kafkatester.tickerplant.producer;

import com.lhamaworks.ameschot.kafkatester.tickerplant.kafkasettings.TickerPlantTopics;

public class ProducerStarter {
    public static void main(String[] args) {
        //outstanding shares producer
        OutstandingSharesProducer oProd = new OutstandingSharesProducer(TickerPlantTopics.T_OUTSTANDING_SHARES, 10000, -1);
        new Thread(oProd).start();
        Runtime.getRuntime().addShutdownHook(new Thread(oProd::close));


        //trade producer
        TradeProducer tProd = new TradeProducer(TickerPlantTopics.T_RAW_TRADES, 1000, -1);
        new Thread(tProd).start();
        Runtime.getRuntime().addShutdownHook(new Thread(tProd::close));
    }
}
