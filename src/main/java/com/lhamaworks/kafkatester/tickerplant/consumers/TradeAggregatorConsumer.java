package com.lhamaworks.kafkatester.tickerplant.consumers;

import com.lhamaworks.kafkatester.tickerplant.kafkasettings.TickerPlantTopics;
import com.lhamaworks.kafkatester.tickerplant.market.Trade;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

public class TradeAggregatorConsumer extends AbstractStreamConsumer
{
    /*Constants*/

    /*Attributes*/

    /*Constructor*/
    public TradeAggregatorConsumer(String topic)
    {
        super(topic, "app-raw-trade-consumer", "group-raw-trade-consumer",Serdes.String(),new Trade.TradeSerde());

        //posts updates as loose events to the output stream rather than emitting the entire table
        consumerProperties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

    }

    /*Methods*/

    @Override
    protected KafkaStreams buildConsumer()
    {
        final StreamsBuilder builder = new StreamsBuilder();

        //get the inbound tradeSource topic
        final KStream<String, Trade> tradeSource = builder
                .stream(topic, Consumed.with(Serdes.String(), new Trade.TradeSerde()));


       //final KTable<String, String> tradeWorthTable = stream.toTable(Materialized.as("stream-converted-to-table"));

        /*Sums On Totals*/
        //sum the total trade worth (price*volume) per symbol
        KStream<String, Double> totalPerSymbolStream = tradeSource.map((s, trade) -> KeyValue.pair(trade.getSymbol().symbol, trade.price * trade.volume))
                .peek((s, aDouble) -> System.out.println("R: " + s + " = " + String.format("%.2f", aDouble)))
                .groupByKey(Grouped.with(Serdes.String(), Serdes.Double()))
                .reduce((c, n) -> c + n)
                .toStream()
                //.peek((s, aDouble) -> System.out.println("A: "+s+" = "+aDouble))
                ;
        totalPerSymbolStream.to(TickerPlantTopics.T_SUM_TRADE_WORTH, Produced.with(Serdes.String(), Serdes.Double()));

        //sum the totals per symbol for the grand total
        totalPerSymbolStream
                .map((s, aDouble) -> KeyValue.pair("TOTAL", aDouble))
                .groupByKey(Grouped.with(Serdes.String(), Serdes.Double()))
                .reduce((c, n) -> c + n)
                .toStream()
                //.peek((s, aDouble) -> System.out.println("T: "+s+" = "+String.format("%.2f", aDouble)))
                .to(TickerPlantTopics.T_TOTAL_SUM_TRADE_WORTH, Produced.with(Serdes.String(), Serdes.Double()));

        /*Streams on Prices*/
        KGroupedStream<String, Double> groupedSymbolPriceStream = tradeSource.map((s, trade) -> KeyValue.pair(trade.symbol.symbol, trade.getPrice())).groupByKey(Grouped.with(Serdes.String(), Serdes.Double()));

        //use the grouped stream to calculate the maximum per symbol
        groupedSymbolPriceStream.reduce((c, n) -> n < c ? c : n)
                .toStream()
                .to(TickerPlantTopics.T_MAX_SYMBOL_PRICE, Produced.with(Serdes.String(), Serdes.Double()));

        //use the grouped stream to calculate the minimum per symbol
        groupedSymbolPriceStream.reduce((c, n) -> n > c ? c : n)
                .toStream()
                .to(TickerPlantTopics.T_MIN_SYMBOL_PRICE, Produced.with(Serdes.String(), Serdes.Double()));


        /*market cap*/
        //get the inbound shareSource topic
        final KStream<String, Integer> shareSource = builder
                .stream(TickerPlantTopics.T_OUTSTANDING_SHARES, Consumed.with(Serdes.String(), Serdes.Integer()));


        KTable<String, Integer> sharesOutstandingKTable = shareSource
                .peek((s, integer) -> System.out.println("S: "+s+" - "+integer))
                .toTable(Materialized.with(Serdes.String(),Serdes.Integer()));

        //get the trade price and join the current outstanding shares to get the market cap
        tradeSource
                .map((s, trade) -> KeyValue.pair(trade.symbol.symbol,trade.price))
                .toTable(Materialized.with(Serdes.String(),Serdes.Double()))
                .join(sharesOutstandingKTable,(price,shares) -> price*shares)
                .toStream()
                .to(TickerPlantTopics.T_SYMBOL_MARKET_CAP,Produced.with(Serdes.String(),Serdes.Double()));



//        sharesOutstandingKTable.join(tradeSource
//                .map((s, trade) -> KeyValue.pair(trade.symbol.symbol, trade.price))
//                .toTable(),(shares,price) -> price*shares)
//                .toStream()
//                .to(TickerPlantTopics.T_SYMBOL_MARKET_CAP,Produced.with(Serdes.String(),Serdes.Double()));


        //build the topology
        final Topology topology = builder.build();

        System.out.println("Topology:");
        System.out.println(topology.describe());

        return new KafkaStreams(topology, consumerProperties);
    }

    public static void main(String[] args) throws Exception
    {
        new TradeAggregatorConsumer(TickerPlantTopics.T_RAW_TRADES).startConsumer();
    }


}