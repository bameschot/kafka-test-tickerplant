package com.lhamaworks.ameschot.kafkatester.tickerplant.kafkasettings;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;

import java.util.Arrays;

public class TickerPlantTopics {
    public static final String T_RAW_TRADES = "t-raw-trades";
    public static final String T_SUM_TRADE_WORTH = "t-sum-trade-worth";
    public static final String T_TOTAL_SUM_TRADE_WORTH = "t-total-sum-trade-worth";
    public static final String T_MIN_SYMBOL_PRICE = "t-min-symbol-price";
    public static final String T_MAX_SYMBOL_PRICE = "t-max-symbol-price";
    public static final String T_SYMBOL_MARKET_CAP = "t-symbol-market-cap";

    public static final String T_OUTSTANDING_SHARES = "t-outstanding-shares";

    public static final String T_INDEXES = "t-indexes";


    public static final String T_A = "t-a";
    public static final String T_B = "t-b";

    public static final String T_A_REPLY = "t-a-reply";


    public static void main(String[] args) throws InterruptedException {

        //create client
        AdminClient kafkaAdminClient = KafkaAdminClient.create(new DefaultKafkaSettings());

        //delete topics
        DeleteTopicsResult dl = kafkaAdminClient.deleteTopics(Arrays.asList(
                        T_RAW_TRADES,
                        T_SUM_TRADE_WORTH,
                        T_TOTAL_SUM_TRADE_WORTH,
                        T_MIN_SYMBOL_PRICE,
                        T_MAX_SYMBOL_PRICE,
                        T_SYMBOL_MARKET_CAP,

                        T_OUTSTANDING_SHARES,

                        T_INDEXES,

                        T_A,
                        T_B,
                        T_A_REPLY
                )
        );

        Thread.sleep(10000);

        System.out.println("Delete:");
        System.out.println(dl.values());

        //create topics
        CreateTopicsResult rl = kafkaAdminClient.createTopics(Arrays.asList(
                        new NewTopic(T_RAW_TRADES, 1, (short) 1),
                        new NewTopic(T_SUM_TRADE_WORTH, 1, (short) 1),
                        new NewTopic(T_TOTAL_SUM_TRADE_WORTH, 1, (short) 1),
                        new NewTopic(T_MIN_SYMBOL_PRICE, 1, (short) 1),
                        new NewTopic(T_MAX_SYMBOL_PRICE, 1, (short) 1),
                        new NewTopic(T_SYMBOL_MARKET_CAP, 1, (short) 1),

                        new NewTopic(T_OUTSTANDING_SHARES, 1, (short) 1),
                        new NewTopic(T_INDEXES, 1, (short) 1),


                        new NewTopic(T_A, 1, (short) 1),
                        new NewTopic(T_B, 1, (short) 1),

                        new NewTopic(T_A_REPLY, 1, (short) 1)
                )
        );

        Thread.sleep(5000);

        System.out.println("Result:");
        System.out.println(rl.values());
    }
}
