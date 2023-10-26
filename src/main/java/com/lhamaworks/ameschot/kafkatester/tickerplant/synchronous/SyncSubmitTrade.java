package com.lhamaworks.ameschot.kafkatester.tickerplant.synchronous;

import com.lhamaworks.ameschot.kafkatester.tickerplant.kafkasettings.TickerPlantTopics;
import com.lhamaworks.ameschot.kafkatester.tickerplant.market.Trade;
import com.lhamaworks.ameschot.kafkatester.tickerplant.market.TradeGenerator;
import org.apache.kafka.common.serialization.Serdes;

import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

public class SyncSubmitTrade {
    public static void main(String[] args) throws InterruptedException, ExecutionException, TimeoutException {


        for (int i = 0; i < 6; i++) {
            new Thread(new TR(i)).start();
        }

    }

    static class TR implements Runnable {
        int id;

        public TR(int id) {
            this.id = id;
        }

        @Override
        public void run() {
            try {
                Random r = new Random();
                SyncProducer<String, Trade, String, String> syncProducer = new SyncProducer<>(
                        TickerPlantTopics.T_A,
                        "sync-test-app-id",
                        "sync-test-group-id",
                        60000,
                        Serdes.String().serializer(),
                        new Trade.TradeSerializer(),
                        Serdes.String().deserializer(),
                        Serdes.String().deserializer());


                for (int i = 0; i < 20; i++) {
                    Trade t = TradeGenerator.i().generateTrade();//new Trade(Symbols.AAPL, i * 100, 100.0d);
                    System.out.println("send: " + id + "/" + i + ": " + t.toString());

                    String reply = syncProducer.send(t.symbol.symbol, t);

                    System.out.println("Received: " + id + "/" + i + ": " + reply);

                    Thread.sleep(r.nextInt(500) + 100);

                }

                System.out.println("DONE: " + id);
                syncProducer.close();

            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            } catch (TimeoutException e) {
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
            }


        }
    }
}
