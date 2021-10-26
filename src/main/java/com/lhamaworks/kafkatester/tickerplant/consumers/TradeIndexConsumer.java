package com.lhamaworks.kafkatester.tickerplant.consumers;

import com.lhamaworks.kafkatester.tickerplant.kafkasettings.DefaultKafkaSettings;
import com.lhamaworks.kafkatester.tickerplant.kafkasettings.TickerPlantTopics;
import com.lhamaworks.kafkatester.tickerplant.market.Indexes;
import com.lhamaworks.kafkatester.tickerplant.market.Symbols;
import com.lhamaworks.kafkatester.tickerplant.market.Trade;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Properties;


public class TradeIndexConsumer extends AbstractStreamConsumer
{
    /*Constants*/

    /*Attributes*/
    String storeName;
    Indexes index;

    ReadOnlyKeyValueStore<String,Double> indexComponentPriceTableStore;

    Properties producerProperties;
    Producer<String,Double> producer;

    /*Constructor*/
    public TradeIndexConsumer(String topic, Indexes index)
    {
        super(topic, "app-trade-index-consumer-" + index.name, "group-trade-index-consumer-" + index.name, Serdes.String(), Serdes.Double());

        this.index = index;
        this.storeName = "index-component-prices-" + index.name;

        //posts updates as loose events to the output stream rather than emitting the entire table
        consumerProperties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

        //setup the producer
        producerProperties = new DefaultKafkaSettings();
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Serdes.String().serializer().getClass());
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, Serdes.Double().serializer().getClass());

        producer = new KafkaProducer<>(producerProperties);

    }

    /*Methods*/

    @Override
    protected KafkaStreams buildConsumer()
    {
        final StreamsBuilder builder = new StreamsBuilder();

        //get the inbound tradeSource topic
        //https://www.ftserussell.com/education-center/calculating-index-values
        //final KStream<String, Double> tradeSource =
        KTable<String,Double> indexComponentPriceTable = builder
                .stream(topic, Consumed.with(Serdes.String(), new Trade.TradeSerde()))
                //filter on those trades that are part of the index
                .filter((s, trade) ->
                {
                    if (trade == null)
                        return false;
                    for (Symbols indexComponent : index.constituents)
                    {
                        if (trade.symbol == indexComponent)
                        {
                            return true;
                        }
                    }
                    return false;
                })
                .peek((s, trade) -> System.out.println("Add:" + index + " = " + trade))
                //map to key price multiplied by the index's constituent shares
                .map((s, trade) -> KeyValue.pair(s, trade.price * index.getSharesByConstituent(s)))
                //add to the index component price table, ensure the store name is set explicitly
                .toTable(Named.as(storeName), Materialized.as(storeName))
                //suppress results for 10 seconds before further processing to emit only one event for each interval
                .suppress(Suppressed.untilTimeLimit(Duration.ofSeconds(10), null).withName("index-consumer-"+index.name))
                //.suppress(Suppressed.untilTimeLimit(Duration.ZERO, null).withName("index-consumer-"+index.name))

                //.suppress(Suppressed.untilTimeLimit(Duration.of(1, ChronoUnit.SECONDS), null))//Suppressed.BufferConfig.maxBytes(1024*1024).emitEarlyWhenFull()))

        ;

        //use the updates written to the table to trigger the aggregation of all value columns of all rows in the table
        //using the store, then send the new index price using a producer to the indexes topic
        indexComponentPriceTable.toStream().foreach((s, aDouble) ->
        {
            double total = 0d;
            for (KeyValueIterator<String,Double> it = indexComponentPriceTableStore.all(); it.hasNext(); )
            {
                KeyValue<String,Double> kv = it.next();
                total+=kv.value;
                //System.out.println(kv.key+" = "+kv.value);
            }

            System.out.println("Send index update: "+index.name+": "+total + " -> " + total/index.baseDivisor);

            total/=index.baseDivisor;
            producer.send(new ProducerRecord<String,Double>(TickerPlantTopics.T_INDEXES,index.name,total));

        });

        //build the topology
        final Topology topology = builder.build();

        System.out.println("Topology:");
        System.out.println(topology.describe());

        //return
        return new KafkaStreams(topology, consumerProperties);
    }

    @Override
    protected void postStartAction(KafkaStreams streams)
    {
        //get the queryable index component price table used for aggregating all rows in the stream
        indexComponentPriceTableStore = streams.store(StoreQueryParameters.fromNameAndType(storeName, QueryableStoreTypes.keyValueStore()));
    }

    public static void main(String[] args) throws Exception
    {
        new Thread(() -> new TradeIndexConsumer(TickerPlantTopics.T_RAW_TRADES, Indexes.THREE_APPLES).startConsumer()).start();
        new Thread(() -> new TradeIndexConsumer(TickerPlantTopics.T_RAW_TRADES, Indexes.FOUR_BEES).startConsumer()).start();
        new Thread(() -> new TradeIndexConsumer(TickerPlantTopics.T_RAW_TRADES, Indexes.FIVE_CARROTS).startConsumer()).start();

    }


}