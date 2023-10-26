package com.lhamaworks.ameschot.kafkatester.tickerplant.synchronous;

import com.lhamaworks.ameschot.kafkatester.tickerplant.kafkasettings.DefaultKafkaSettings;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.StreamsConfig;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class SyncProducer<SK, SV, RK, RV> implements AutoCloseable {
    /*Constants*/
    public static final String CORRELATION_ID_HEADER_KEY = "CorrelationID";
    public static final String REPLY_TOPIC_HEADER_KEY = "ReplyTopic";

    public static final String REPLY_TOPIC_POSTFIX = "-reply";
    public static final int TIMEOUT_EXTENSION_MS = 10;

    /*Attributes*/
    protected ThreadPoolExecutor executor;
    protected Properties kafkaProducerProperties;
    protected Properties kafkaConsumerProperties;

    protected KafkaProducer<SK, SV> producer;
    protected KafkaConsumer<RK, RV> consumer;

    protected String topic;
    protected String replyTopic;

    protected int timeoutMS;
    protected boolean isRunning = true;

    protected Map<String, CountDownLatch> replyLatchMap = new ConcurrentHashMap<>();
    protected Map<String, RV> replyValueMap = new ConcurrentHashMap<>();


    /*Constructor*/
    public SyncProducer(String topic, String applicationID, String groupID, int timeoutMS, Serializer<? extends SK> sendKeySerializer, Serializer<? extends SV> sendValueSerializer, Deserializer<RK> receiveKeyDeserializer, Deserializer<RV> receiveValueDeserializer) {
        //topic and reply topic
        this.topic = topic;
        replyTopic = topic + REPLY_TOPIC_POSTFIX;

        //timeout
        this.timeoutMS = timeoutMS;

        //setup the thread pool
        executor = (ThreadPoolExecutor) new ThreadPoolExecutor(2, 4, timeoutMS + TIMEOUT_EXTENSION_MS - 1, TimeUnit.MILLISECONDS, new LinkedBlockingDeque<>());//Executors.newCachedThreadPool();

        //setup kafka properties
        kafkaProducerProperties = new DefaultKafkaSettings();
        kafkaProducerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, sendKeySerializer.getClass());
        kafkaProducerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, sendValueSerializer.getClass());

        //setup the producer
        producer = new KafkaProducer<SK, SV>(kafkaProducerProperties);

        //consumer properties
        kafkaConsumerProperties = new DefaultKafkaSettings();
        kafkaConsumerProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationID);
        kafkaConsumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, groupID);
        kafkaConsumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, receiveKeyDeserializer.getClass());
        kafkaConsumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, receiveValueDeserializer.getClass());


        //setup consumer
        consumer = new KafkaConsumer<RK, RV>(kafkaConsumerProperties);
        //consumer.subscribe(Arrays.asList(topic));

        //dynamically assign all partitions to this consumer, ignore explicitly the default app id assignment from
        //kafka
        List<PartitionInfo> partitionInfos = consumer.partitionsFor(replyTopic);
        if (partitionInfos != null) {
            List<TopicPartition> partitions = new ArrayList<>(partitionInfos.size());
            for (PartitionInfo partition : partitionInfos) {
                partitions.add(new TopicPartition(partition.topic(), partition.partition()));
            }
            consumer.assign(partitions);
        }

        //run the polling loop directly
        new Thread(new ReceiverRunnable()).start();

    }

    /*Methods*/
    @Override
    public void close() throws Exception {
        isRunning = false;
        producer.close();

        isRunning = false;
    }

    public RV send(SK key, SV value) throws InterruptedException, ExecutionException, TimeoutException {
        //get a future from the threadpool
        Future<RV> future = executor.submit(new senderCallable(key, value));

        //get the result or timeout
        return future.get(SyncProducer.this.timeoutMS + TIMEOUT_EXTENSION_MS, TimeUnit.MILLISECONDS);
    }


    /*Inner Classes*/
    private class ReceiverRunnable implements Runnable {
        /*Attributes*/

        /*Constructor*/

        /*Methods*/
        @Override
        public void run() {
            while (isRunning) {
                //read topic records
                ConsumerRecords<RK, RV> recs = consumer.poll(Duration.of(10000, ChronoUnit.MILLIS));

                //loop over the records received
                for (ConsumerRecord<RK, RV> cr : recs) {
                    //retrieve the correlation id
                    Iterable<Header> s = cr.headers().headers(CORRELATION_ID_HEADER_KEY);
                    String correlationID = new String(s.iterator().next().value(), StandardCharsets.UTF_8);

                    //set the response value for the correlation id
                    replyValueMap.put(correlationID, cr.value());

                    //release the correlation id's latch in order to return the reply value
                    CountDownLatch cl = replyLatchMap.get(correlationID);
                    if (cl != null)
                        cl.countDown();
                }

                //commit as already read
                consumer.commitSync();
            }

            //closing the consumer when the tread ends
            System.out.println("Consumer Closed");
            consumer.close();

        }

    }

    private class senderCallable implements Callable<RV> {

        /*Attributes*/
        private final SK key;
        private final SV value;

        /*Constructor*/

        public senderCallable(SK key, SV value) {
            this.key = key;
            this.value = value;
        }

        /*Methods*/
        @Override
        public RV call() throws Exception {
            //generate the correlation id for the message
            String correlationID = UUID.randomUUID().toString();

            //construct the CountDownLatch to wait on and store it under the correlation id
            CountDownLatch latch = new CountDownLatch(1);
            replyLatchMap.put(correlationID, latch);

            //create the message (with correlationID) and send it
            ProducerRecord<SK, SV> message = new ProducerRecord<>(topic, key, value);
            message.headers().add(CORRELATION_ID_HEADER_KEY, correlationID.getBytes(StandardCharsets.UTF_8));
            message.headers().add(REPLY_TOPIC_HEADER_KEY, replyTopic.getBytes(StandardCharsets.UTF_8));
            producer.send(message);

            //wait for the latch to return, remove the latch from the map if a timeout occurred
            if (!latch.await(timeoutMS, TimeUnit.MILLISECONDS)) {
                //remove the latch from the map to prevent leak
                replyLatchMap.remove(correlationID);

                //force a wait that is certain to exceed the max timeout of the callable created by send
                // forcing a timeout exception on that level
                Thread.sleep(TIMEOUT_EXTENSION_MS * 2);
            }

            //get (and remove) the response from the response map based on the correlation id
            return replyValueMap.remove(correlationID);
        }
    }


}
