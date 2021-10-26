package com.lhamaworks.kafkatester.tickerplant.market;

import java.util.EnumMap;
import java.util.Random;

public class TradeGenerator
{
    /*Constants*/
    private static final TradeGenerator instance = new TradeGenerator();

    /*Attributes*/
    private final Random random = new Random();

    public EnumMap<Symbols, Double> lastTradePrice = new EnumMap<Symbols, Double>(Symbols.class);

    /*Constructor*/
    private TradeGenerator()
    {
        //setup the last trade prices with the base values
        for (Symbols s : Symbols.values())
        {
            lastTradePrice.put(s, s.baseVal);
        }

    }

    /*Methods*/
    public static TradeGenerator i()
    {
        return instance;
    }

    public synchronized Trade generateTrade()
    {
        //select random symbol
        Symbols s = Symbols.values()[random.nextInt(Symbols.values().length)%38];

        return generateTrade(s);
    }

    public synchronized Trade generateTrade(Symbols s)
    {

        //get a new price based on the previous price but only allow a maximum change of 12%
        //then calculate the new price and log it again
        double priceChange = 1d + (random.nextDouble() % 0.12 * (random.nextBoolean() ? 1d : -1d));

        double newPrice = Math.round(s.baseVal * priceChange * 100.0) / 100.0;

        //correct price negatives
        if (newPrice < 0)
            newPrice = newPrice * -1;

        lastTradePrice.put(s, newPrice);

        //create a new trade with a random amount of shares and the symbol and new price
        return new Trade(s, random.nextInt(1000), newPrice);
    }


}
