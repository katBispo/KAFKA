const { runProducer } = require('./index.js');

// producer.test.js
// producer.test.js
describe('Teste de Integração Kafka', () => {
  it('deve enviar uma mensagem para o Kafka', async () => {
    try {
      await runProducer();  // Corrigido para chamar runProducer
      console.log('Mensagem enviada com sucesso.');
    } catch (error) {
      console.error('Erro ao enviar mensagem:', error);
      throw error;
    }
  });
});

