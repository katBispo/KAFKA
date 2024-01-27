const { Kafka, logLevel } = require('kafkajs');
const fs = require('fs');
const mongoose = require('mongoose');
const KafkaConsumer = require('./KafkaConsumer');

mongoose.connect('mongodb://192.168.100.154:27017/bosta', {
  //useNewUrlParser: true,
  //useUnifiedTopology: true,
});

//  mongoose.models.Message para verificar se o modelo já foi compilado anteriormente
const MessageModel = mongoose.models.Message || mongoose.model('Message', { value: String });

const topicConfig = {
  topic: 'test-topic',
  configEntries: [
    {
      name: 'retention.ms',
      value: '10000',
    },
  ]
};

const kafkaConfig = {
  clientId: 'my-app',
  brokers: ['localhost:9092'],
  logLevel: logLevel.ERROR,
};

async function runProducer() {
  const kafka = new Kafka(kafkaConfig);
  const producer = kafka.producer({
    retry: {
      retries: Infinity,
      maxRetryTime: 30000,
    },
  });

  console.log('Conectando ao Kafka (produtor)...');
  await producer.connect();
  console.log('Conectado com sucesso ao Kafka (produtor)!');

  try {
    const messageValue = 'Hello KafkaJS user!';
    console.log(`Enviando mensagem para o tópico ${topicConfig.topic}: ${messageValue}`);

    await MessageModel.create({ value: messageValue });

    const result = await producer.send({
      topic: topicConfig.topic,
      messages: [{ value: messageValue }],
    });

    console.log('Mensagem enviada com sucesso:', result);

    await new Promise(resolve => setTimeout(resolve, 5000));
  } catch (error) {
    console.error('Erro ao enviar mensagem:', error);
  } finally {
    await producer.disconnect();
  }
}

async function run() {
  const consumer = new KafkaConsumer(topicConfig.topic, kafkaConfig);

  try {
    await runProducer();
    await consumer.consume();
  } catch (error) {
    console.error('Erro na execução:', error);
  }
}

run().catch(error => {
  console.error('Erro:', error);
});
