package com.lhamaworks.ameschot.kafkatester.tickerplant.market;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.charset.StandardCharsets;

public class Trade {
    /*Constants*/
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    /*Attributes*/
    public Symbols symbol;
    public int volume;
    public double price;

    /*Constructor*/
    public Trade() {

    }

    public Trade(Symbols symbol, int volume, double price) {
        this.symbol = symbol;
        this.volume = volume;
        this.price = price;
    }

    public Trade(String symbol, int volume, double price) {
        this(Symbols.getSymbol(symbol), volume, price);
    }

    /*Methods*/
    @Override
    public String toString() {
        return "Trade{" +
                "symbol=" + symbol +
                ", volume=" + volume +
                ", price=" + price +
                '}';
    }

    public String toJson() throws JsonProcessingException {
        return OBJECT_MAPPER.writeValueAsString(this);
    }

    public static Trade fromJson(String json) throws JsonProcessingException {
        return OBJECT_MAPPER.readValue(json, Trade.class);
    }

    public void setPrice(double price) {
        this.price = price;
    }

    public void setSymbol(Symbols symbol) {
        this.symbol = symbol;
    }

    public void setVolume(int volume) {
        this.volume = volume;
    }

    public double getPrice() {
        return price;
    }

    public int getVolume() {
        return volume;
    }

    public Symbols getSymbol() {
        return symbol;
    }

    /*Inner Classes*/
    public static final class TradeSerializer implements Serializer<Trade> {
        @Override
        public byte[] serialize(String s, Trade trade) {
            try {
                return trade.toJson().getBytes(StandardCharsets.UTF_8);
            } catch (JsonProcessingException e) {
                return null;
            }

        }
    }

    public static final class TradeDeserializer implements Deserializer<Trade> {
        @Override
        public Trade deserialize(String s, byte[] bytes) {
            try {
                return Trade.fromJson(new String(bytes, StandardCharsets.UTF_8));
            } catch (JsonProcessingException e) {
                return null;
            }
        }
    }

    public static final class TradeSerde extends Serdes.WrapperSerde<Trade> {
        public TradeSerde() {
            super(new TradeSerializer(), new TradeDeserializer());
        }
    }
}
