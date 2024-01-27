const { Kafka, logLevel, Partitioners } = require('kafkajs');
const fs = require('fs');
const KafkaConsumerAck = require('./KafkaConsumerAck'); 

const topicConfig = {
  topic: 'ack-topic',
  configEntries: [
    {
      name: 'retention.ms',
      value: '10000', // Tempo de retenção em milissegundos
    },
  ]
};

const kafkaConfig = {
  clientId: 'my-app',
  brokers: ['localhost:9092'],
  logLevel: logLevel.INFO,
};

// Adicionando acks ao criar o produtor
const producerConfig = {
  retry: {
    retries: Infinity,
    maxRetryTime: 30000,
  },
  acks: -1,
  createPartitioner: Partitioners.LegacyPartitioner, // Adicione esta linha
};

async function runProducer() {
  const kafka = new Kafka(kafkaConfig);
  const producer = kafka.producer(producerConfig);

  console.log('Conectando ao Kafka (produtor)...');
  await producer.connect();
  console.log('Conectado com sucesso ao Kafka (produtor)!');

  // Adicione o ouvinte para o evento 'delivery-report' ANTES de enviar a mensagem
  producer.on(producer.events.DELIVERY_REPORT, e => {
    const { payload, errorCode, ...rest } = e;
    if (errorCode) {
      console.error('Erro na confirmação do ack:', e);
    } else {
      console.log('Ack recebido:', rest);
    }
  });

  // Lógica do produtor
  try {
    const messageValue = 'Hello KafkaJS user!';
    console.log(`Enviando mensagem para o tópico ${topicConfig.topic}: ${messageValue}`);

    fs.appendFileSync('mensagens.log', `${new Date().toISOString()} - ${messageValue}\n`);

    await producer.send({
      topic: topicConfig.topic,
      messages: [{ value: messageValue }],
    });

    console.log('Mensagem enviada com sucesso.');

    await new Promise(resolve => setTimeout(resolve, 5000));
  } catch (error) {
    console.error('Erro ao enviar mensagem:', error);
  } finally {
    await producer.disconnect();
    console.log('Desconectado do Kafka (produtor)');
  }
}

async function run() {
  const consumer = new KafkaConsumerAck(topicConfig.topic, { ...kafkaConfig });

  try {
    // Executa o produtor e o consumidor sequencialmente
    await runProducer();
    await consumer.consume();
  } catch (error) {
    console.error('Erro na execução:', error);
  }
}

run().catch(error => {
  console.error('Erro:', error);
});
