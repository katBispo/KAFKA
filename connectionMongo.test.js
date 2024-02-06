// connectionMongo.test.js

const mongoose = require('mongoose');

describe('MongoDB Connection', () => {
  it('should connect to MongoDB', async () => {
    try {
      // Configurar a URL do MongoDB
      const mongoURI = 'mongodb://127.0.0.1:27017/bosta';

      // Tentar conectar ao MongoDB
      await mongoose.connect(mongoURI, {
        useNewUrlParser: true,
        useUnifiedTopology: true,
      });

      // Verificar se a conexão foi bem-sucedida
      const isConnected = mongoose.connection.readyState === 1;
      expect(isConnected).toBe(true);

    } catch (error) {
      console.error('MongoDB connection error:', error);
      throw error;
    }
  });
});
