const { Kafka, logLevel } = require('kafkajs');
const mongoose = require('mongoose');

const MAX_RETRIES = 3;


const MessageModel = mongoose.model('Message', { value: String });

class KafkaConsumer {
  constructor(topic, consumerConfig) {
    this.topic = topic;
    this.consumer = new Kafka(consumerConfig).consumer({ groupId: 'test-group' });
  }


  async consume() {
    await this.consumer.connect();
    await this.consumer.subscribe({ topic: this.topic });
    console.log(`${this.topic} Started to consume messages`);

    return await this.consumer.run({
      autoCommit: false,
      eachMessage: async ({ topic, partition, message }) => {
        // const MAX_RETRIES = 3; Remova esta linha, pois já está definida no início do arquivo

        const prefix = `${topic}[${partition} | ${message.offset}] / ${message.timestamp}`;
        console.log(`- ${prefix} ${message.key}`);

        try {
          await this.retryIfThrowsException(
            async () => {
              const eventData = await this.transformMessage(message.value);
              await this.handleData(eventData);
            }
          );
        } catch (error) {
          this.persistError(error, { tags: { 'EventConsumerError': topic } });
        }

        await this.consumer.commitOffsets([{ topic, partition, offset: message.offset }]);
        console.log(`${this.constructor.name} Mensagem Consumida`);
      },
    });
  }

  async retryIfThrowsException(cb) {
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

    // Salva no MongoDB
    await MessageModel.create({ value: eventData });
  }

  persistError(error, options) {
    console.error('Erro persistente:', error, options);
  }
}

module.exports = KafkaConsumer;
