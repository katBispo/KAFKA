const mongoose = require('mongoose');

const MessageModel = mongoose.model('Message', { value: String });

module.exports = MessageModel;

