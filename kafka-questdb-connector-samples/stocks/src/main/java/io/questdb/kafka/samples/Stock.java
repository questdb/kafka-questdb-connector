package io.questdb.kafka.samples;

import org.springframework.data.annotation.Id;

import java.sql.Timestamp;

public class Stock {
    private @Id Long id;
    private String symbol;
    private double price;
    private Timestamp last_update;

    public String getSymbol() {
        return symbol;
    }

    public void setSymbol(String symbol) {
        this.symbol = symbol;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    public Long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public Timestamp getTimestamp() {
        return last_update;
    }

    public void setTimestamp(Timestamp timestamp) {
        this.last_update = timestamp;
    }

    @Override
    public String toString() {
        return "Stock{" +
                "id=" + id +
                ", symbol='" + symbol + '\'' +
                ", price=" + price +
                ", last_update=" + last_update +
                '}';
    }
}
