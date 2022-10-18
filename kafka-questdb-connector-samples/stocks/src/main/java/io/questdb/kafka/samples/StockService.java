package io.questdb.kafka.samples;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.Random;

@Service
public class StockService {
    @Autowired
    private StockRepository stockRepository;
    private String[] allSymbols;
    private final Random random = new Random();

    @PostConstruct
    public void init() {
        this.allSymbols = stockRepository.findAllSymbols().toArray(new String[0]);
    }

    @Scheduled(fixedRate = 1)
    public void tick() {
        for (String symbol : allSymbols) {
            double factor = 1 + (random.nextGaussian() / 100);
            stockRepository.updateBySymbol(symbol, factor);
//            stockRepository.updateBySymbol(symbol, 1.0 + Math.random() * 0.1);
        }
    }
}
