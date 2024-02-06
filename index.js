const { Kafka, logLevel } = require('kafkajs');
const fs = require('fs');
const mongoose = require('mongoose');
const { KafkaConsumer, MessageModel } = require('./KafkaConsumer'); // Importe o MessageModel

const kafkaConfig = {
  clientId: 'my-app',
  brokers: ['localhost:9092'],
  logLevel: logLevel.ERROR,
};
const topicConfig = {
  topic: 'test-topic',
  configEntries: [
    {
      name: 'retention.ms',
      value: '10000',
    },
  ]
};

async function connectToMongoDB() {
  const maxRetries = 3;
  let currentRetry = 0;

  while (currentRetry < maxRetries) {
    try {
      await mongoose.connect('mongodb://192.168.100.154:27017/bosta', {
        //useNewUrlParser: true,
        //useUnifiedTopology: true,
      });
      console.log('Conectado com sucesso ao MongoDB!');
      break; // Conexão bem-sucedida, sair do loop
    } catch (error) {
      console.error(`Erro ao conectar ao MongoDB: ${error.message}`);
      currentRetry++;
      if (currentRetry < maxRetries) {
        console.log(`Tentando reconectar ao MongoDB. Tentativa ${currentRetry}/${maxRetries}...`);
        await new Promise(resolve => setTimeout(resolve, 5000)); // Aguardar antes de tentar novamente
      } else {
        console.error('Número máximo de tentativas alcançado. Falha na conexão com o MongoDB.');
        throw error; // Lançar erro após esgotar as tentativas
      }
    }
  }
}

async function runProducer() {
  await connectToMongoDB(); // Conectar ao MongoDB antes de iniciar o produtor

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

    // Não tenho certeza de onde vem topicConfig, então você pode precisar defini-lo ou passá-lo como argumento para runProducer()

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
