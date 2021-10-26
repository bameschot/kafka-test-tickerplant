package com.lhamaworks.kafkatester.tickerplant.synchronous;

import com.lhamaworks.kafkatester.tickerplant.kafkasettings.DefaultKafkaSettings;
import com.lhamaworks.kafkatester.tickerplant.kafkasettings.TickerPlantTopics;
import com.lhamaworks.kafkatester.tickerplant.market.Trade;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class SyncSubmitTradeReceiver implements AutoCloseable
{
    /*Constants*/

    /*Attributes*/
    protected String topic;

    protected Properties consumerProperties;
    protected Properties replyProducerProperties;
    protected Properties sumbitTradeProducerProperties;

    protected KafkaConsumer<String, Trade> consumer;
    protected KafkaProducer<String, String> producer;
    protected KafkaProducer<String, Trade> tradeProducer;


    /*Constructor*/
    public SyncSubmitTradeReceiver(String topic, String appID, String groupID)
    {
        this.topic = topic;

        consumerProperties = new DefaultKafkaSettings();
        consumerProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, appID);
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, groupID);
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Trade.TradeDeserializer.class);

        consumer = new KafkaConsumer<>(consumerProperties);


        //setup kafka properties
        replyProducerProperties = new DefaultKafkaSettings();
        replyProducerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Serdes.String().serializer().getClass());
        replyProducerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, Serdes.String().serializer().getClass());

        //setup the producer
        producer = new KafkaProducer<String, String>(replyProducerProperties);

        //setup kafka properties
        sumbitTradeProducerProperties = new DefaultKafkaSettings();
        sumbitTradeProducerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Serdes.String().serializer().getClass());
        sumbitTradeProducerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, Trade.TradeSerializer.class);

        tradeProducer = new KafkaProducer<String, Trade>(sumbitTradeProducerProperties);

    }

    /*Methods*/
    protected KafkaStreams consume()
    {
        //setup consumer
        consumer.subscribe(Arrays.asList(topic));

        consumer.seekToBeginning(Arrays.asList());

        System.out.println("Started polling: " + topic);
        while (true)
        {
            //read topic records
            ConsumerRecords<String, Trade> recs = consumer.poll(Duration.of(10000, ChronoUnit.MILLIS));

            //add results to map
            for (ConsumerRecord<String, Trade> cr : recs)
            {
                System.out.println("-------------");
                System.out.println("Receive: "+cr);

                //headers
                //retrieve the correlation id
                Iterable<Header> cIdH = cr.headers().headers(SyncProducer.CORRELATION_ID_HEADER_KEY);
                String correlationID = new String(cIdH.iterator().next().value(), StandardCharsets.UTF_8);

                //retrieve the reply topic
                Iterable<Header> rtH = cr.headers().headers(SyncProducer.REPLY_TOPIC_HEADER_KEY);
                String replyTopic = new String(rtH.iterator().next().value(), StandardCharsets.UTF_8);

                //value
                Trade trade = cr.value();
                ProducerRecord<String,Trade> submitTradeMessage = new ProducerRecord<>(trade.symbol.name,trade);
                System.out.println("Submit: "+submitTradeMessage);
                tradeProducer.send(submitTradeMessage);

                //create and send reply message
                ProducerRecord<String,String> replyMessage = new ProducerRecord<>(replyTopic,cr.key(),"Submitted: "+trade.toString());
                replyMessage.headers().add(SyncProducer.CORRELATION_ID_HEADER_KEY,correlationID.getBytes(StandardCharsets.UTF_8));

                System.out.println("Reply: "+replyMessage);
                producer.send(replyMessage);
            }

            //commit as already read
            consumer.commitSync();
        }
    }

    public void startConsumer()
    {

        // attach shutdown handler to catch control-c
        final CountDownLatch latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(consumerProperties.getProperty(StreamsConfig.APPLICATION_ID_CONFIG) + "-shutdown-hook")
        {
            @Override
            public void run()
            {
                consumer.close();
                latch.countDown();
            }
        });

        try
        {
            System.out.println("Started Consumer: " + consumerProperties.getProperty(StreamsConfig.APPLICATION_ID_CONFIG) + " on: " + topic);
            consume();

            latch.await();
        }
        catch (Throwable e)
        {
            System.exit(1);
        }
    }


    public static void main(String[] args) throws Exception
    {
        new Thread(new SyncSubmitTradeReceiver(TickerPlantTopics.T_A, "app-test-a-poll-consumer", "group-test-a-poll-consumer")::startConsumer).start();
    }


    @Override
    public void close()
    {
        consumer.close();
    }
}