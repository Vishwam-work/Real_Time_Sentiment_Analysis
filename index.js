// const Twit = require('twit');
const NewsAPI = require('newsapi')
const { Kafka } = require('kafkajs');
const express = require('express');
const http = require('http');
const socketIO = require('socket.io');
const { MongoClient } = require('mongodb');
const cron = require('node-cron')
const app = express();
const server = http.createServer(app);
const io = socketIO(server);

const newsApiKey = 'f5ab00f6047748a1b6d8247b9457bd3c';
const newsApi = new NewsAPI(newsApiKey);



const kafka = new Kafka({
  clientId: 'twitter-kafka',
  brokers: ['localhost:9092'],
});

const producer = kafka.producer();

async function connectKafka() {
  await producer.connect();
  console.log('Connected to Kafka');
}

async function disconnectKafka() {
  await producer.disconnect();
  console.log('Disconnected from Kafka');
}

async function sendToKafka(topic, message) {
  await producer.send({
    topic: topic,
    messages: [{ value: JSON.stringify(message) }],
  });
}

io.on('connection', (socket) => {
  console.log('Client connected');

  socket.on('disconnect', () => {
    console.log('Client disconnected');
  });
});

async function fetchNews() {
    try {
      const response = await newsApi.v2.topHeadlines({
        country: 'us',
      });
  
      const articles = response.articles;
      articles.forEach((article) => {
        io.emit('news', article);
        sendToKafka('news-topic', article);
        saveToMongoDB(article);
      });
    } catch (error) {
      console.error('Error fetching news:', error);
    }


}
async function saveToMongoDB(news) {
  const mongoURL = 'mongodb://localhost:27017';
  const dbName = 'News-Data';
  const collectionName = 'news_live';

  const client = await MongoClient.connect(mongoURL);
  const db = client.db(dbName);
  const collection = db.collection(collectionName);

  try {
    await collection.insertOne(news);
    console.log('News saved to MongoDB:', news.text);
  } catch (error) {
    console.error('Error saving news to MongoDB:', error);
  } finally {
    client.close();
  }
}

// Start the server
const port = 3000;
server.listen(port, () => {
  console.log(`Server listening on port ${port}`);
});

// Connect to Kafka and start streaming newss
connectKafka()
  .then(() => {
    fetchNews();
  })
  .catch((error) => {
    console.error('Error connecting to Kafka:', error);
  });

cron.schedule('*/30 * * * * *', () => {
    fetchNews();
});


