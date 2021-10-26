package com.lhamaworks.kafkatester.tickerplant.producer;

import com.lhamaworks.kafkatester.tickerplant.kafkasettings.DefaultKafkaSettings;
import com.lhamaworks.kafkatester.tickerplant.market.Trade;
import com.lhamaworks.kafkatester.tickerplant.market.TradeGenerator;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serializer;

import java.util.List;
import java.util.Properties;

public abstract class AbstractProducer<K, V> implements AutoCloseable, Runnable
{
    /*Constants*/

    /*Attributes*/
    protected final long timeoutMS;
    protected final int maxPublishes;
    protected final String topic;
    protected final Properties props;

    protected KafkaProducer<K, V> producer;

    /*Constructor*/
    public AbstractProducer(String topic, long timeoutMS, int maxPublishes, Serializer<? extends K> keySerializer, Serializer<? extends V> valueSerializer)
    {
        props = new DefaultKafkaSettings();
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializer.getClass());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer.getClass());

        this.timeoutMS = timeoutMS;
        this.maxPublishes = maxPublishes;
        this.topic = topic;

        producer = new KafkaProducer<K, V>(props);
    }

    /*Methods*/
    @Override
    public void close()
    {
        producer.close();
    }

    public abstract List<ProducerRecord<K, V>> produce();

    @Override
    public void run()
    {
        System.out.println("Started Producer: " + this.topic);

        for (int i = 0; maxPublishes <= 0 || i < maxPublishes; i++)
        {
            //Use key if you want all the messages to go to a single partition
            for (ProducerRecord<K, V> message : produce())
            {
                producer.send(message);
                System.out.println("Send: " + message);
            }

            try
            {
                Thread.sleep(timeoutMS);
            }
            catch (InterruptedException e)
            {
                e.printStackTrace();
            }
        }
    }

    @Override
    public String toString()
    {
        return "TradeProducer{" +
                "timeoutMS=" + timeoutMS +
                ", maxPublishes=" + maxPublishes +
                ", topic='" + topic + '\'' +
                '}';
    }
}
