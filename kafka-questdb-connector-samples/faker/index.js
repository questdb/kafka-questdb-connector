const { Kafka, logLevel, Partitioners } = require('kafkajs');

const TOPIC = 'trades';
const SYMBOLS = ['BTC-USDT', 'ETH-USDT', 'SOL-USDT', 'XRP-USDT', 'DOGE-USDT'];
const START_PRICES = [76853.5, 4102.2, 100.45, 2.71, 0.2134];
const DELAY_MS = 20;

const kafka = new Kafka({
    clientId: 'trades-producer',
    brokers: ['kafka:9092'],
    logLevel: logLevel.INFO,
});
const producer = kafka.producer({ createPartitioner: Partitioners.DefaultPartitioner });

// trade_id is unique even across producer restarts: it starts from the current time in
// microseconds and grows by one per trade. The readme uses it as a DEDUP key.
let tradeId = Date.now() * 1000;
const prices = [...START_PRICES];

function nextTrade() {
    const i = Math.floor(Math.random() * SYMBOLS.length);
    // random walk of at most 0.5% per trade, so a price chart looks plausible
    prices[i] = Math.max(0.0001, prices[i] * (1 + (Math.random() - 0.5) * 0.01));
    return {
        trade_id: tradeId++,
        symbol: SYMBOLS[i],
        side: Math.random() < 0.5 ? 'buy' : 'sell',
        price: Number(prices[i].toPrecision(8)),
        amount: Number((Math.random() * 2).toFixed(6)),
        timestamp: new Date().toISOString(),
    };
}

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function connectWithRetry() {
    for (;;) {
        try {
            await producer.connect();
            return;
        } catch (err) {
            console.warn(`Kafka is not ready yet (${err.message}), retrying in 2s`);
            await sleep(2000);
        }
    }
}

async function run() {
    await connectWithRetry();
    console.log(`Producing trades to topic '${TOPIC}'`);
    let sent = 0;
    for (;;) {
        const trade = nextTrade();
        try {
            // the message key is the symbol, so all trades of one symbol stay in order
            await producer.send({
                topic: TOPIC,
                messages: [{ key: trade.symbol, value: JSON.stringify(trade) }],
            });
            if (++sent % 500 === 0) {
                console.log(`Sent ${sent} trades, last one: ${JSON.stringify(trade)}`);
            }
        } catch (err) {
            // the topic is created on first use and Kafka may still be electing its leader
            console.warn(`Failed to send a trade, retrying: ${err.message}`);
            await sleep(1000);
        }
        await sleep(DELAY_MS);
    }
}

run().catch((err) => {
    console.error(err);
    process.exit(1);
});
