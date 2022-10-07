const {faker} = require("@faker-js/faker");
const { Kafka, logLevel} = require('kafkajs');

const kafka = new Kafka({
    clientId: 'producer',
    brokers: ['kafka:9092'],
    logLevel: logLevel.INFO
});

const producer = kafka.producer();

async function ingest() {
    try {
        await producer.connect();

        for (let year = 1970; year < 2021; year++) {
            for (let i = 0; i < 100; i++) {
                let batch = [];
                for (let j = 0; j < 100; j++) {
                    const data = {
                        firstname: faker.name.firstName(),
                        lastname: faker.name.lastName(),
                        birthday: faker.date.birthdate({min: year, max: year}),
                    };
                    batch.push({value: JSON.stringify(data)})
                }
                await producer.sendBatch({
                    topicMessages: [
                        {
                            topic: 'People',
                            messages: batch
                        }]
                });
            }
        }
        console.log("Done");
    } catch(err){
        console.error(err);
    }
}

ingest()
