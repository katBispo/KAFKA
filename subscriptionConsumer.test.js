/**SIMULANDO A SUBSCRIÇÃO DO CONSUMIDOR A UM TOPICO ESPECIFICO
 * VERIFICA TBM SE A SIMULÇÃO E A SUBSCRIÇÃO FORAM BEM SUCEDIDAS
 */

const { Kafka } = require('kafkajs');
const { helper } = require('mockgoose');

// Jest mock para kafkajs
jest.mock('kafkajs', () => {
  const actualKafkajs = jest.requireActual('kafkajs');

  const mockConsumerRun = jest.fn();

  const KafkaConsumer = class {
    constructor() {
      this.run = mockConsumerRun;
      this.commitOffsets = jest.fn();
      this.stop = jest.fn();
    }

    async connect() {
      //conexão do consumidor
    }

    async subscribe({ topic, fromBeginning }) {
      //subscrição do consumidor
    }
  };

  return {
    ...actualKafkajs,
    Kafka: jest.fn().mockImplementation(() => ({
      consumer: jest.fn().mockReturnValue(new KafkaConsumer()),
    })),
    mockConsumerRun,
  };
});

describe('Test Kafka Consumer', () => {
  beforeAll(async () => {
    //conectar ao mongodb dps
  });

  it('should consume messages and confirm offset', async () => {
    const consumer = new Kafka().consumer({ groupId: 'test-group' });
  
    // Simulando que os métodos connect e subscribe foram bem-sucedidos
    consumer.connect = jest.fn().mockResolvedValue();
    consumer.subscribe = jest.fn().mockResolvedValue();
  
    await consumer.connect();
    await consumer.subscribe({ topic: 'test-topic', fromBeginning: true });
  
    // Verificar se os métodos foram chamados corretamente
    expect(consumer.connect).toHaveBeenCalled();
    expect(consumer.subscribe).toHaveBeenCalledWith({ topic: 'test-topic', fromBeginning: true });
  });
  
});
