package io.questdb.kafka.samples;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Random;

@Service
public class StockService {
    private final StockRepository stockRepository;
    private String[] allSymbols;
    private final Random random = new Random();
    private int biasMaxValue = 0;
    private boolean biasPositive = true;
    private long biasChangeTime = 0;

    public StockService(StockRepository stockRepository) {
        this.stockRepository = stockRepository;
    }

    @PostConstruct
    public void init() {
        this.allSymbols = stockRepository.findAllSymbols().toArray(new String[0]);
    }

    @Scheduled(fixedDelay = 1)
    public void tick() {
        refreshBias();
        Timestamp now = timestampNow();
        for (String symbol : allSymbols) {
            double delta = random.nextGaussian() * 20;
            double biasNow = biasPositive ? random.nextDouble() * biasMaxValue : -random.nextDouble() * biasMaxValue;
            stockRepository.updateBySymbol(symbol, delta + biasNow, now);
        }
    }

    private void refreshBias() {
        long nowMillis = System.currentTimeMillis();
        if (nowMillis > biasChangeTime) {
            biasChangeTime = nowMillis + 1000;
            biasPositive = random.nextBoolean();
            biasMaxValue = random.nextInt(10);
        }
    }

    private static Timestamp timestampNow() {
        Instant instant = Instant.now();
        Timestamp now = new Timestamp(instant.toEpochMilli());
        now.setNanos(instant.getNano());
        return now;
    }
}
