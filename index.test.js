const { Kafka, logLevel } = require('kafkajs');
const mongoose = require('mongoose');
const { KafkaConsumer, MessageModel } = require('./KafkaConsumer'); 

jest.setTimeout(300000);

const consumerConfig = {
  clientId: 'my-app',
  brokers: ['localhost:9092'],
  //logLevel: 'logLevel.ERROR',
};

const kafkaConsumer = new KafkaConsumer('test-topic', consumerConfig);

describe('Teste de Consumidor Kafka com Banco de Dados', () => {
  beforeAll(async () => {
    await mongoose.connect('mongodb://192.168.100.154:27017/bosta');
  });

  afterAll(async () => {
    await mongoose.disconnect();
    await kafkaConsumer.disconnect(); // Desconecta o consumidor após a execução do teste
  });

  it('deve enviar e consumir uma mensagem do Kafka, salvando no banco de dados', async () => {
    try {
      const kafkaConfig = {
        clientId: 'my-app',
        brokers: ['localhost:9092'],
        logLevel: logLevel.ERROR,
      };

      const kafka = new Kafka(kafkaConfig);
      const producer = kafka.producer();

      await producer.connect();

      const messageValue = 'ENVIANDO MSG  DO TETS';
      await producer.send({
        topic: 'test-topic',
        messages: [{ value: messageValue }],
      });

      await producer.disconnect();

      await kafkaConsumer.consume();

      const messageInDatabase = await mongoose.model('Message').findOne({ value: 'ENVIANDO MSG AGR' });

      expect(messageInDatabase).toBeTruthy();
    } catch (error) {
      console.error('Erro ao enviar, consumir mensagem ou verificar o banco de dados:', error);
      throw error;
    }
  }, 60000);
});
