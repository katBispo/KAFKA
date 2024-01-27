// KafkaConsumer.js
const { Kafka, logLevel } = require('kafkajs');
const MAX_RETRIES = 5; 

class KafkaConsumerAck {//CONSTRUTORES
  constructor(topic, consumerConfig) {//recebendo nome do topicp e configuração do consumidor
    this.topic = topic;
    this.consumer = new Kafka(consumerConfig).consumer({ groupId: 'test-group' });//instanciando objeto kafka do tipo consumidor e 
  }

  async consume() {
    await this.consumer.connect();
    await this.consumer.subscribe({ topic: this.topic });
    console.log(`${this.topic} Started to consume messages`);

    return await this.consumer.run({
      autoCommit: false,
      eachMessage: async ({ topic, partition, message }) => {
        const MAX_RETRIES = 3; //numero de tentativas da função

        const prefix = `${topic}[${partition} | ${message.offset}] / ${message.timestamp}`;
        console.log(`- ${prefix} ${message.key}`);

        // Chamada à função persistError
        try {
          await this.retryIfThrowsException(
            async () => {//funçãp cb
            const eventData = await this.transformMessage(message.value);
            await this.handleData(eventData);
          }, MAX_RETRIES);
        } catch (error) {
          this.persistError(error, { tags: { 'EventConsumerError': topic } });
        }

        await this.consumer.commitOffsets([{ topic, partition, offset: message.offset }]);//topico,particao, id msg
        console.log(`${this.constructor.name} Mensagem Consumida`);
      },
    });
  }

  async retryIfThrowsException(cb) {// tentando executar a função callback
    const SLEEP_DURATION = 6000;
    let tries = 0;
    let lastError = null;

    while (tries < MAX_RETRIES) {
      try {
        await new Promise(resolve => setTimeout(resolve, SLEEP_DURATION));
        await cb();
        break;
      } catch (e) {
        console.error(`retryIfThrowsException(): tried ${tries + 1} times. Error: ${e.message}`);
        lastError = e;
        tries++;
      }
    }

    if (tries >= MAX_RETRIES) {
      throw lastError;
    } else {
      return true;
    }
  }

  async transformMessage(value) {
    console.log(`Transformando mensagem: ${value}`);
    return value;
  }

  async handleData(eventData) {
    console.log(`Manipulando dados: ${eventData}`);
  }

  persistError(error, options) {
    console.error('Erro persistente:', error, options);
  }
}

module.exports = KafkaConsumerAck;
