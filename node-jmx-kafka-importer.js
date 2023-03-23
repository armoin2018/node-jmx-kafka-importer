const yaml = require('js-yaml');
const fs = require('fs');
const { Kafka } = require('kafkajs');
const jmx = require('jmx');
const tls = require('tls');

const config = yaml.safeLoad(fs.readFileSync('config.yaml', 'utf8'));

// Prepare Kafka configuration options
const kafkaOptions = {
  brokers: [config.kafka.host],
};

// Add SSL options if configured
if (config.kafka.ssl) {
  kafkaOptions.ssl = {
    rejectUnauthorized: config.kafka.ssl.rejectUnauthorized,
    ca: [fs.readFileSync(config.kafka.ssl.caCertPath)],
    key: fs.readFileSync(config.kafka.ssl.clientKeyPath),
    cert: fs.readFileSync(config.kafka.ssl.clientCertPath),
  };
}

// Create Kafka client
const kafka = new Kafka(kafkaOptions);

// Create Kafka producer
const producer = kafka.producer();

// Connect to Kafka
async function connectToKafka() {
  await producer.connect();
  console.log('Kafka producer is ready.');
}
connectToKafka().catch((err) => {
  console.error('Error connecting to Kafka:', err);
  process.exit(1);
});

// Loop through each port defined in the config file
for (const portConfig of config.ports) {
  const port = portConfig.port;
  const topic = portConfig.topic;

  // Prepare JMX options
  const jmxOptions = {
    service: `service:jmx:rmi:///jndi/rmi://localhost:${port}/jmxrmi`,
  };

  // Add SSL options if configured
  if (portConfig.ssl) {
    jmxOptions.ssl = true;
    jmxOptions.sslOptions = {
      rejectUnauthorized: portConfig.ssl.rejectUnauthorized,
      key: fs.readFileSync(portConfig.ssl.clientKeyPath),
      cert: fs.readFileSync(portConfig.ssl.clientCertPath),
      ca: [fs.readFileSync(portConfig.ssl.caCertPath)],
    };
  }

  // Connect to JMX
  const jmxConnection = jmx.createClient(jmxOptions);
  jmxConnection.connect();

  // Listen for JMX data and produce it as Kafka messages
  jmxConnection.on('connect', () => {
    console.log(`Connected to JMX on port ${port}.`);
  });

  jmxConnection.on('error', (err) => {
    console.error(`Error from JMX on port ${port}:`, err);
  });

  jmxConnection.on('data', async (data) => {
    const message = {
      topic: topic,
      messages: JSON.stringify(data),
    };

    try {
      await producer.send({
        topic: topic,
        messages: [{ value: message.messages }],
      });
      console.log(`Sent Kafka message for port ${port} to topic ${topic}`);
    } catch (err) {
      console.error(`Error sending Kafka message for port ${port} to topic ${topic}:`, err);
    }
  });
}
