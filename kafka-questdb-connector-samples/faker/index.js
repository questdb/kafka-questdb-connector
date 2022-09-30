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

        for (let i = 0; i < 100_000; i++) {
            const data = {
                firstname: faker.name.firstName(),
                lastname: faker.name.lastName(),
                birthday: faker.date.birthdate()
            };
            await producer.send({
                topic: "People",
                messages: [
                    {
                        value: JSON.stringify(data)
                    }
                ]
            });
        }
    } catch(err){
        console.error(err);
    }
}

ingest()
