package com.lhamaworks.ameschot.kafkatester.tickerplant.market;

import java.util.Arrays;

public enum Indexes {
    /*Values*/
    THREE_APPLES("three-apples", new Symbols[]{Symbols.ABBV, Symbols.ABT, Symbols.AIG}, new int[]{50, 10, 30}, 1000.0),
    FOUR_BEES("four-bees", new Symbols[]{Symbols.BAC, Symbols.BMY, Symbols.BRK, Symbols.BK}, new int[]{10, 50, 40, 80}, 1000.0),
    FIVE_CARROTS("five-carrots", new Symbols[]{Symbols.CRM, Symbols.CMC, Symbols.CSC, Symbols.CVX, Symbols.COF}, new int[]{10, 50, 40, 80, 20}, 1000.0);


    /*Attributes*/
    public final String name;
    public final Symbols[] constituents;
    public final int[] shares;
    public final double baseValue;
    public final double baseDivisor;

    /*Constructor*/
    Indexes(String name, Symbols[] constituents, int[] shares, double baseValue) {
        this.name = name;
        this.constituents = constituents;
        this.shares = shares;
        this.baseValue = baseValue;


        Double t = 0.0;
        for (int i = 0; i < constituents.length; i++) {
            t += constituents[i].baseVal * shares[i];
        }
        this.baseDivisor = t / baseValue;
    }

    public int getSharesByConstituent(String symbol) {
        for (int i = 0; i < constituents.length; i++) {
            if (constituents[i].symbol.equals(symbol)) {
                return shares[i];
            }
        }
        return 0;
    }

    public static Indexes getIndex(String indexName) {
        for (int i = 0; i < values().length; i++) {
            if (values()[i].name.equals(indexName))
                return values()[i];
        }
        return null;
    }

    @Override
    public String toString() {
        return "Indexes{" +
                "name='" + name + '\'' +
                ", constituents=" + Arrays.toString(constituents) +
                ", shares=" + Arrays.toString(shares) +
                ", baseValue=" + baseValue +
                ", baseDivisor=" + baseDivisor +
                '}';
    }
}
